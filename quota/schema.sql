CREATE TABLE IF NOT EXISTS quota_events (
    stream text NOT NULL,
    event_id text NOT NULL,
    org text NOT NULL,
    feature text NOT NULL,
    period text NOT NULL,
    operation text NOT NULL,
    state text NOT NULL,
    units bigint NOT NULL,
    used_delta bigint NOT NULL,
    PRIMARY KEY (stream, event_id)
);

CREATE TABLE IF NOT EXISTS quota_totals (
    stream text NOT NULL,
    org text NOT NULL,
    feature text NOT NULL,
    period text NOT NULL,
    used bigint NOT NULL,
    PRIMARY KEY (stream, org, feature, period)
);
