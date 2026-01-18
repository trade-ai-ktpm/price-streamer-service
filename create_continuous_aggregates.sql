-- Continuous Aggregates for TimescaleDB
-- Create materialized views for different timeframes

-- 5m candles
CREATE MATERIALIZED VIEW IF NOT EXISTS candle_data_5m
WITH (timescaledb.continuous) AS
SELECT 
    coin_id,
    time_bucket('5 minutes', timestamp) AS timestamp,
    first(open, timestamp) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, timestamp) AS close,
    sum(volume) AS volume
FROM candle_data_1m
GROUP BY coin_id, time_bucket('5 minutes', timestamp);

-- 15m candles
CREATE MATERIALIZED VIEW IF NOT EXISTS candle_data_15m
WITH (timescaledb.continuous) AS
SELECT 
    coin_id,
    time_bucket('15 minutes', timestamp) AS timestamp,
    first(open, timestamp) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, timestamp) AS close,
    sum(volume) AS volume
FROM candle_data_1m
GROUP BY coin_id, time_bucket('15 minutes', timestamp);

-- 1h candles
CREATE MATERIALIZED VIEW IF NOT EXISTS candle_data_1h
WITH (timescaledb.continuous) AS
SELECT 
    coin_id,
    time_bucket('1 hour', timestamp) AS timestamp,
    first(open, timestamp) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, timestamp) AS close,
    sum(volume) AS volume
FROM candle_data_1m
GROUP BY coin_id, time_bucket('1 hour', timestamp);

-- 4h candles
CREATE MATERIALIZED VIEW IF NOT EXISTS candle_data_4h
WITH (timescaledb.continuous) AS
SELECT 
    coin_id,
    time_bucket('4 hours', timestamp) AS timestamp,
    first(open, timestamp) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, timestamp) AS close,
    sum(volume) AS volume
FROM candle_data_1m
GROUP BY coin_id, time_bucket('4 hours', timestamp);

-- 1d candles
CREATE MATERIALIZED VIEW IF NOT EXISTS candle_data_1d
WITH (timescaledb.continuous) AS
SELECT 
    coin_id,
    time_bucket('1 day', timestamp) AS timestamp,
    first(open, timestamp) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, timestamp) AS close,
    sum(volume) AS volume
FROM candle_data_1m
GROUP BY coin_id, time_bucket('1 day', timestamp);

-- Add refresh policies (auto-refresh)
SELECT add_continuous_aggregate_policy('candle_data_5m', 
    start_offset => INTERVAL '10 minutes', 
    end_offset => INTERVAL '1 minute', 
    schedule_interval => INTERVAL '1 minute');

SELECT add_continuous_aggregate_policy('candle_data_15m', 
    start_offset => INTERVAL '30 minutes', 
    end_offset => INTERVAL '1 minute', 
    schedule_interval => INTERVAL '1 minute');

SELECT add_continuous_aggregate_policy('candle_data_1h', 
    start_offset => INTERVAL '2 hours', 
    end_offset => INTERVAL '1 minute', 
    schedule_interval => INTERVAL '1 minute');

SELECT add_continuous_aggregate_policy('candle_data_4h', 
    start_offset => INTERVAL '8 hours', 
    end_offset => INTERVAL '1 minute', 
    schedule_interval => INTERVAL '5 minutes');

SELECT add_continuous_aggregate_policy('candle_data_1d', 
    start_offset => INTERVAL '2 days', 
    end_offset => INTERVAL '1 minute', 
    schedule_interval => INTERVAL '5 minutes');
