CREATE TABLE IF NOT EXISTS data_product_performance (
    ts                 TIMESTAMPTZ      NOT NULL,  -- "timestamp"
    plant              TEXT             NOT NULL,
    segment            TEXT             NOT NULL,
    production_line    TEXT             NOT NULL,  -- "productionLine"
    workcell           TEXT             NOT NULL,
    order_no           TEXT,                       -- "order"
    article            TEXT,
    cycle_time         DOUBLE PRECISION,
    target_cycle_time  DOUBLE PRECISION,
    injection_time     DOUBLE PRECISION,
    plasticizing_time  DOUBLE PRECISION
);

SELECT create_hypertable('data_product_performance', by_range('ts'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS data_product_performance_workcell_ts_idx
    ON data_product_performance (workcell, ts DESC);