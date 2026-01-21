"""
Real-time candle aggregator: Compute 5m/15m/1h/4h/1d/1w from 1m candles
Simple logic: Query closed 1m candles from DB + current 1m candle = current aggregated candle
"""
from datetime import datetime, timezone
from typing import Dict
import asyncio
import json
from redis_publisher import RedisPublisher
from database import async_session
from sqlalchemy import text

# Timeframe configurations (in minutes)
TIMEFRAME_CONFIGS = {
    "5m": {"minutes": 5},
    "15m": {"minutes": 15},
    "1h": {"minutes": 60},
    "4h": {"minutes": 240},
    "1d": {"minutes": 1440},
    "1w": {"minutes": 10080}
}

AGGREGATOR_TIMEFRAMES = ["5m", "15m", "1h", "4h", "1d", "1w"]

publisher = RedisPublisher()


def get_candle_start_time(timestamp_ms: int, interval_minutes: int) -> int:
    """
    Get the start time of a candle for a given interval
    
    Args:
        timestamp_ms: Current timestamp in milliseconds
        interval_minutes: Interval in minutes (5, 15, 60, etc.)
    
    Returns:
        Start time of the candle in milliseconds
    """
    timestamp_seconds = timestamp_ms // 1000
    interval_seconds = interval_minutes * 60
    
    # Round down to the start of the interval
    candle_start = (timestamp_seconds // interval_seconds) * interval_seconds
    
    return candle_start * 1000


async def aggregate_candle(symbol: str, candle_1m: dict):
    """
    Aggregate 1m candle into higher timeframes using simple logic:
    1. Get current 1m candle
    2. Query closed 1m candles from DB (from timeframe start to now)
    3. Calculate current aggregated candle = closed 1m candles + current 1m candle
    4. Save to Redis + publish to WebSocket
    5. Client replaces old current candle with new one
    
    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        candle_1m: 1m candle data with keys: timestamp, open, high, low, close, volume, is_closed
    """
    timestamp_ms = candle_1m['timestamp']
    is_closed = candle_1m.get('is_closed', False)
    
    # Get 1m candle start time (exclude this from DB query if not closed)
    candle_1m_start_ms = get_candle_start_time(timestamp_ms, 1)
    candle_1m_start_seconds = candle_1m_start_ms // 1000
    
    # Process each timeframe
    for tf in AGGREGATOR_TIMEFRAMES:
        try:
            interval_minutes = TIMEFRAME_CONFIGS[tf]['minutes']
            
            # Get timeframe start (e.g., for 5m at 10:04, start is 10:00)
            candle_start_ms = get_candle_start_time(timestamp_ms, interval_minutes)
            candle_start_seconds = candle_start_ms // 1000
            
            # Query CLOSED 1m candles from DB in this timeframe window
            # IMPORTANT: Exclude current 1m candle timestamp to avoid duplication
            # We will add it separately below
            query = text("""
                SELECT 
                    (array_agg(open ORDER BY timestamp))[1] as first_open,
                    MAX(high) as max_high,
                    MIN(low) as min_low,
                    (array_agg(close ORDER BY timestamp DESC))[1] as last_close,
                    SUM(volume) as total_volume,
                    COUNT(*) as candle_count
                FROM candle_data_1m
                WHERE coin_id = (SELECT id FROM coins WHERE symbol = :symbol LIMIT 1)
                    AND timestamp >= to_timestamp(:start_time)
                    AND timestamp < to_timestamp(:exclude_current)
            """)
            
            async with async_session() as session:
                result = await session.execute(query, {
                    "symbol": symbol,
                    "start_time": candle_start_seconds,
                    "exclude_current": candle_1m_start_seconds
                })
                row = result.fetchone()
            
            # Calculate aggregated candle
            if row and row.candle_count > 0:
                # Have closed 1m candles - combine with current 1m candle
                open_price = float(row.first_open)
                high = max(float(row.max_high), candle_1m['high'])
                low = min(float(row.min_low), candle_1m['low'])
                close = candle_1m['close']  # Always use current close
                volume = float(row.total_volume) + candle_1m['volume']
                candle_count = int(row.candle_count)
            else:
                # No closed candles yet - use current 1m candle only
                open_price = candle_1m['open']
                high = candle_1m['high']
                low = candle_1m['low']
                close = candle_1m['close']
                volume = candle_1m['volume']
                candle_count = 0
            
            # Determine if this aggregated candle should close
            # It closes when: current 1m is closed AND we have all 1m candles
            should_close = is_closed and (candle_count + 1 == interval_minutes)
            
            # Create candle update
            candle_update = {
                "type": "candle",
                "symbol": symbol,
                "timeframe": tf,
                "timestamp": candle_start_ms,
                "open": float(open_price),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "volume": float(volume),
                "is_closed": should_close
            }
            
            # Save to Redis for API access
            redis_key = f"current_candle:{symbol}:{tf}"
            await publisher.redis.setex(
                redis_key,
                interval_minutes * 60 + 60,  # TTL = candle duration + 1 min buffer
                json.dumps({
                    "time": candle_start_seconds,
                    "open": float(open_price),
                    "high": float(high),
                    "low": float(low),
                    "close": float(close),
                    "volume": float(volume),
                })
            )
            
            # Publish to WebSocket
            channel = f"candle:{symbol}:{tf}"
            await publisher.publish_price(channel, candle_update)
            
            # Log for debugging
            if symbol == "BTCUSDT" and tf in ["5m", "15m", "1h"]:
                print(f"📊 {symbol} {tf}: {candle_count+1}/{interval_minutes} candles, "
                      f"O={open_price:.2f}, H={high:.2f}, L={low:.2f}, C={close:.2f}, "
                      f"V={volume:.2f}, closed={should_close}", flush=True)
                if tf == "1h":
                    print(f"   🔍 1H Debug: candle_start={datetime.utcfromtimestamp(candle_start_seconds)}, "
                          f"1m_start={datetime.utcfromtimestamp(candle_1m_start_seconds)}, "
                          f"is_closed={is_closed}, db_count={candle_count}", flush=True)
            
            # If candle closes, publish close event and clean up Redis
            if should_close:
                closed_event = {
                    "type": "candle_closed",
                    "symbol": symbol,
                    "timeframe": tf,
                    "candle": candle_update,
                }
                await publisher.publish_price(channel, closed_event)
                await publisher.redis.delete(redis_key)
                print(f"✅ Candle closed: {symbol} {tf} at {datetime.utcfromtimestamp(candle_start_seconds)}", flush=True)
        
        except Exception as e:
            print(f"❌ Error aggregating {symbol} {tf}: {e}", flush=True)


async def save_aggregated_candle(symbol: str, timeframe: str, candle: dict, table_name: str):
    """
    Save completed aggregated candle to database
    
    Args:
        symbol: Trading pair
        timeframe: Timeframe (5m, 15m, etc.)
        candle: Candle data
        table_name: Table name
    """
    # NOTE: This function is not used anymore.
    # We rely on TimescaleDB continuous aggregates to compute higher timeframes.
    # This function is kept for reference only.
    pass
