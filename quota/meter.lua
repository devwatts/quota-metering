-- KEYS: balance, receipt, limits, events, pending.
-- ARGV: action, org, feature, id, units, request, fingerprint, month, start, end,
--       final_state, response, receipt_ttl.
-- Scripts isolate commands but do not roll them back: validate before writing.
local action, org, feature, id = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local function kind(key, expected)
    local actual = redis.call('TYPE', key).ok
    return actual == 'none' or actual == expected
end
if not kind(KEYS[4], 'stream') or not kind(KEYS[5], 'zset') then
    return redis.error_reply('Unexpected quota key type')
end
-- HMGET also checks that these keys have the expected type.
local old = redis.call('HMGET', KEYS[2], 'org', 'feature', 'units', 'fingerprint',
    'state', 'period', 'response', 'balance')
local function outcome(state, period, response)
    return {'ok', state, period, response or ''}
end
-- Resolve retries before checking the current month so they keep their original period.
if action == 'reserve' and old[5] then
    if old[2] ~= feature or old[3] ~= ARGV[5] or old[4] ~= ARGV[7] then
        return {'conflict'}
    end
    -- Repeat a write so the retry waits for AOF fsync too; HSET keeps the original TTL.
    redis.call('HSET', KEYS[2], 'state', old[5])
    return outcome(old[5], old[6], old[7])
end
if action == 'settle' then
    if not old[5] then return {'missing_operation'} end
    if old[1] ~= org or old[2] ~= feature or old[8] ~= KEYS[1] then return {'conflict'} end
    if old[5] ~= 'pending' then
        -- Another attempt already settled this operation; its recorded outcome wins.
        redis.call('HSET', KEYS[2], 'state', old[5])
        return outcome(old[5], old[6], old[7])
    end
    if ARGV[11] ~= 'confirmed' and ARGV[11] ~= 'released' then return {'invalid_state'} end
end
local now
if action ~= 'settle' then
    now = tonumber(redis.call('TIME')[1])
    if now < tonumber(ARGV[9]) or now >= tonumber(ARGV[10]) then
        return {'clock', tostring(now)}
    end
end
local values = redis.call('HMGET', KEYS[1], 'limit', 'used', 'reserved', 'reset')
local limit, used, reserved, reset
if values[1] then
    limit, used, reserved, reset = tonumber(values[1]), tonumber(values[2]),
        tonumber(values[3]), tonumber(values[4])
else
    if action == 'settle' then return redis.error_reply('Missing reserved balance') end
    local configured = redis.call('HGET', KEYS[3], feature)
    if not configured then return {'unconfigured'} end
    limit, used, reserved, reset = tonumber(configured), 0, 0, tonumber(ARGV[10])
end
if not limit or not used or not reserved or not reset or limit < 0 or limit > 1000000000
    or used < 0 or reserved < 0 or used + reserved > limit then
    return redis.error_reply('Invalid quota balance')
end
if action == 'usage' then
    return {'ok', cjson.encode({limit=limit, used=used, reserved=reserved, reset=reset, period=ARGV[8]})}
end
if action == 'reserve' then
    local units = tonumber(ARGV[5])
    if not units or units < 1 or units > 10000 or units ~= math.floor(units) then
        return {'invalid_units'}
    end
    if units > limit - used - reserved then return {'exhausted'} end
    redis.call('HSET', KEYS[1], 'limit', limit, 'used', used, 'reserved', reserved + units, 'reset', reset)
    redis.call('HSET', KEYS[2], 'id', id, 'org', org, 'feature', feature, 'units', ARGV[5],
        'fingerprint', ARGV[7], 'period', ARGV[8], 'request', ARGV[6], 'state', 'pending', 'balance', KEYS[1])
    redis.call('ZADD', KEYS[5], now, KEYS[2])
    return outcome('pending', ARGV[8], '')
elseif action == 'settle' then
    local units = tonumber(old[3])
    if not units or units < 1 or reserved < units then return redis.error_reply('Missing reserved units') end
    local charged = ARGV[11] == 'confirmed' and units or 0
    -- Only final outcomes enter the SQL ledger. Pending receipts are durable in AOF.
    redis.call('XADD', KEYS[4], '*', 'org', org, 'feature', feature, 'operation', id,
        'period', old[6], 'state', ARGV[11], 'units', old[3], 'used_delta', tostring(charged))
    redis.call('HSET', KEYS[1], 'used', used + charged, 'reserved', reserved - units)
    redis.call('HSET', KEYS[2], 'state', ARGV[11], 'response', ARGV[12])
    redis.call('ZREM', KEYS[5], KEYS[2])
    -- Start retention only after a final outcome; pending receipts must not expire.
    redis.call('EXPIRE', KEYS[2], ARGV[13])
    return outcome(ARGV[11], old[6], ARGV[12])
end
return {'invalid_action'}
