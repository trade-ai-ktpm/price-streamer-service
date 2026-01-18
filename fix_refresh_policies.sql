-- Fix refresh policies with correct intervals
-- Drop existing policies if any
SELECT remove_continuous_aggregate_policy('candle_data_5m', if_exists => true);
SELECT remove_continuous_aggregate_policy('candle_data_15m', if_exists => true);
SELECT remove_continuous_aggregate_policy('candle_data_1h', if_exists => true);
SELECT remove_continuous_aggregate_policy('candle_data_4h', if_exists => true);
SELECT remove_continuous_aggregate_policy('candle_data_1d', if_exists => true);

-- Add refresh policies with correct windows
SELECT add_continuous_aggregate_policy('candle_data_5m', 
    start_offset => INTERVAL '1 hour', 
    end_offset => INTERVAL '5 minutes', 
    schedule_interval => INTERVAL '5 minutes');

SELECT add_continuous_aggregate_policy('candle_data_15m', 
    start_offset => INTERVAL '2 hours', 
    end_offset => INTERVAL '15 minutes', 
    schedule_interval => INTERVAL '15 minutes');

SELECT add_continuous_aggregate_policy('candle_data_1h', 
    start_offset => INTERVAL '4 hours', 
    end_offset => INTERVAL '1 hour', 
    schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('candle_data_4h', 
    start_offset => INTERVAL '12 hours', 
    end_offset => INTERVAL '4 hours', 
    schedule_interval => INTERVAL '4 hours');

SELECT add_continuous_aggregate_policy('candle_data_1d', 
    start_offset => INTERVAL '3 days', 
    end_offset => INTERVAL '1 day', 
    schedule_interval => INTERVAL '1 day');
