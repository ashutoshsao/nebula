-- Fresh markets must expose new fills before the scheduled materialization runs.
-- Every level must be real-time so the hierarchical rollups reach the raw tail.
-- https://www.tigerdata.com/docs/learn/continuous-aggregates/real-time-aggregates
ALTER MATERIALIZED VIEW candles_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW candles_5m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW candles_15m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW candles_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW candles_4h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW candles_1d SET (timescaledb.materialized_only = false);
