# Monthly quota metering

A Python quota component with a small schedule-search API as its consumer.
Redis enforces the quota across application processes. Organizations are assigned
to one of two Redis shards. PostgreSQL receives an
asynchronous usage ledger.

## Run it

Requires Docker with Compose v2:

```sh
./run.sh
```

This builds the app, starts two Redis instances, PostgreSQL, eight API workers and the ledger
worker, then runs 2,000 schedule searches at 200 requests/second. The demo prints
latency and accounting results and stops the containers when it finishes.
Named volumes retain data between runs; each load run uses fresh organizations.
The demo rate is intended for a laptop. It is not the performance claim.
The launcher returns nonzero and prints diagnostics if startup or the demo fails,
or if the demo exits without completing its checks. Ctrl+C stops the stack and
returns a nonzero status. Containers are recreated on each run to avoid accepting
a stale completion marker; database volumes are retained. The latency gate is
enabled separately with `--require-slo` in the load-test commands below.

On the separate benchmark VMs, the implemented consumer passed 2k requests/sec
for 60 seconds and 10k/sec for five minutes, with check-and-deduction p99 of
2.79 ms and 6.89 ms. See [DESIGN.md](DESIGN.md) for hardware, complete timings
and limits.

To leave the API running:

```sh
docker compose up --build -d api ledger
```

Search two routes, consuming two units:

```sh
curl -i http://localhost:8080/orgs/acme/schedules/search \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: search-001' \
  -d '{"routes":["SGSIN-NLRTM","CNSHA-USLAX"]}'

curl http://localhost:8080/orgs/acme/usage/sailing-schedule
```

The usage response contains `limit`, `used`, `reserved`, `remaining`, `period`
and `resets_at`. `used` counts completed searches; pending work occupies
`reserved`. All month boundaries are UTC.

While its receipt is retained, repeat the same request and key to retrieve the
stored outcome without another charge. Changing its payload returns HTTP 409.
Insufficient quota returns 429; a batch containing an unknown route returns 422
and releases the entire batch.
The third supported route is `INNSA-AEJEA`.

Completed outcomes are retained for one hour from confirmation or release. Retry
within that window using the same organization, key and payload; retries do not
extend expiry. A key reused after its receipt expires is a new operation and can
consume quota again. Unresolved reservations do not expire; the recovery worker
resolves them using the stored request. Production retention must cover the
client's maximum supported retry window; see [DESIGN.md](DESIGN.md).

## Tests

```sh
docker compose run --build --rm tests
```

Tests use real Redis and PostgreSQL. Two tests start eight Python processes at a
barrier, then send competing five-unit batches against a 500-unit allowance.
They check both exactly sufficient quota and demand exceeding the allowance.
Other cases cover retries, racing success/failure, lost responses, recovery,
month boundaries, SQL replay, and the HTTP interface.

## Configuration

Edit [config/quotas.json](config/quotas.json). In Compose, rebuild and recreate the
API with `docker compose up --build -d api` because configuration is copied into
the image. When running Python directly, restart the API. A limit is snapshotted
when the first reservation opens an organization/feature/month.
Changing configuration affects periods that have not opened yet; it does not
rewrite an existing allowance.

The component accepts arbitrary feature names. This submission implements one
consumer, `sailing-schedule`, which charges one unit per route search. The fixed
schedule dataset is deliberately small and has no external side effects.

The API binds to localhost in Compose. Organization identity is supplied in the
path for the demo; a deployed service must obtain it from authenticated context.
The bundled database credentials are only for this local stack.

## Load test

With the stack running:

```sh
docker compose run --rm demo python scripts/load.py \
  --url http://api:8080 --rate 2000 --seconds 60 --orgs 5000 --require-slo

docker compose run --rm demo python scripts/load.py \
  --url http://api:8080 --rate 10000 --seconds 300 --orgs 5000 --require-slo
```

Use `--hot` for one organization and `--mixed` for batches of 1, 10 and 100 units.
The hot-batch measurement used `--rate 2000 --seconds 30 --orgs 1 --hot --mixed`.
Capacity runs used `--rate 11000 --seconds 30` and `--rate 15000 --seconds 20`.
`--require-slo` also makes the command fail if quota admission has p99 of 10 ms or more.
Without it, request errors, dropped arrivals or mismatched accounting still fail
the run.

The generator schedules arrivals independently of completion and reports dropped
arrivals. It verifies successful units against both Redis and PostgreSQL.
Latency histograms use 0.01 ms buckets. Admission, settlement, their combined
time, HTTP roundtrip, and scheduled-arrival latency are reported separately.

For separate hosts, install with `pip install '.[test]'`, set `REDIS_URLS` to the
two comma-separated Redis URLs and set `DATABASE_URL`. Run the API and worker
in separate terminals:

```sh
gunicorn 'quota.api:create_app()' --worker-class aiohttp.GunicornUVLoopWebWorker \
  --workers 8 --bind 0.0.0.0:8080 --timeout 60
python -m quota.worker
```

Run the generator in another terminal or host using the same storage variables:

```sh
python scripts/load.py --url "$API_URL" \
  --rate 2000 --seconds 60 --orgs 5000 --require-slo --output report.json
```

Set `API_URL` to the API host's HTTP address. Running the generator directly
with `--output` saves the full report on that host; the Compose commands above
print their summaries to the terminal.
Redis must use the
persistence policy in [config/redis.conf](config/redis.conf); startup checks it.
See [DESIGN.md](DESIGN.md) for the measurement boundary and tested deployment.

Shard order and count are fixed for an existing deployment. Startup rejects a
change that would move an organization's quota to another shard. Scaling the
storage needs migration of existing state, not just another URL in configuration.

```sh
docker compose down
```

This stops the local stack and retains its data volumes.
