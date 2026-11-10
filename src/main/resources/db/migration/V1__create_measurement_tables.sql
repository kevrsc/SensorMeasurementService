CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE sensor_readings (
    time            timestamptz      NOT NULL,
    device          text             NOT NULL,
    sensor_index    int              NOT NULL,
    address         text             NOT NULL,
    celsius         double precision NOT NULL,
    fahrenheit      double precision NOT NULL,
    outlier_reasons text[]           NOT NULL DEFAULT '{}',
    PRIMARY KEY (device, sensor_index, time)
);
SELECT create_hypertable('sensor_readings', by_range('time'));

CREATE TABLE moisture_readings (
    time             timestamptz NOT NULL,
    device           text        NOT NULL,
    probe_index      int         NOT NULL,
    pin              int         NOT NULL,
    raw              int         NOT NULL,
    percent          int         NOT NULL,
    expected_percent int         NOT NULL,
    outlier_reasons  text[]      NOT NULL DEFAULT '{}',
    PRIMARY KEY (device, probe_index, time)
);
SELECT create_hypertable('moisture_readings', by_range('time'));

CREATE TABLE device_status (
    time             timestamptz NOT NULL,
    device           text        NOT NULL,
    seq              bigint,
    uptime_ms        bigint,
    reset_reason     text,
    boot_count       int,
    rssi_dbm         int,
    ip               text,
    wifi_reconnects  int,
    post_failures    int,
    free_heap        bigint,
    min_free_heap    bigint,
    sketch_md5       text,
    keepalive        text,
    keepalive_age_ms bigint,
    probe_count      int,
    PRIMARY KEY (device, time)
);
SELECT create_hypertable('device_status', by_range('time'));
