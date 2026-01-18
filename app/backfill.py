"""
Backfill missing candle data from Binance REST API
"""
import asyncio
import httpx
from datetime import datetime, timedelta
from sqlalchemy import text
from database import async_session
from config import SYMBOLS

# Binance REST API endpoint
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

async def get_latest_timestamp(coin_id: int) -> datetime | None:
    """Get the latest candle timestamp from database for a coin"""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT MAX(timestamp) as latest_time
                FROM candle_data_1m
                WHERE coin_id = :coin_id
            """),
            {"coin_id": coin_id}
        )
        row = result.fetchone()
        return row.latest_time if row else None

async def fetch_binance_klines(symbol: str, interval: str, start_time: int, end_time: int, limit: int = 1000):
    """
    Fetch klines from Binance REST API
    
    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        interval: Candle interval (1m, 5m, 1h, etc.)
        start_time: Start timestamp in milliseconds
        end_time: End timestamp in milliseconds
        limit: Max number of candles per request (max 1000)
    
    Returns:
        List of kline data
    """
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(BINANCE_API_URL, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Error fetching {symbol} klines: {e}", flush=True)
            return []

async def insert_candles_batch(coin_id: int, candles: list):
    """Bulk insert candles into database"""
    if not candles:
        return 0
    
    async with async_session() as session:
        try:
            # Prepare batch insert
            values = []
            for candle in candles:
                timestamp = datetime.utcfromtimestamp(candle[0] / 1000)
                values.append({
                    "coin_id": coin_id,
                    "timestamp": timestamp,
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5])
                })
            
            # Bulk insert with ON CONFLICT
            await session.execute(
                text("""
                    INSERT INTO candle_data_1m (coin_id, timestamp, open, high, low, close, volume)
                    VALUES (:coin_id, :timestamp, :open, :high, :low, :close, :volume)
                    ON CONFLICT (coin_id, timestamp) DO UPDATE
                    SET open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                """),
                values
            )
            await session.commit()
            return len(candles)
        except Exception as e:
            print(f"❌ Error inserting batch: {e}", flush=True)
            await session.rollback()
            return 0

async def backfill_symbol(symbol: str, coin_id: int, max_gap_hours: int = 24 * 30):
    """
    Backfill missing data for a symbol
    
    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        coin_id: Database coin ID
        max_gap_hours: Maximum hours to backfill (default 30 days)
    """
    print(f"🔍 Checking {symbol} for missing data...", flush=True)
    
    # Get latest timestamp from DB
    latest_time = await get_latest_timestamp(coin_id)
    
    if latest_time is None:
        # No data exists, backfill from max_gap_hours ago
        start_time = datetime.utcnow() - timedelta(hours=max_gap_hours)
        print(f"  📭 No data found, backfilling from {start_time}", flush=True)
    else:
        start_time = latest_time + timedelta(minutes=1)  # Start from next minute
        gap_minutes = int((datetime.utcnow() - start_time).total_seconds() / 60)
        
        if gap_minutes <= 1:
            print(f"  ✅ {symbol} is up to date (latest: {latest_time})", flush=True)
            return 0
        
        print(f"  ⚠️  Gap detected: {gap_minutes} minutes ({gap_minutes/60:.1f} hours)", flush=True)
        print(f"  📥 Backfilling from {start_time} to now...", flush=True)
    
    # Binance limit: 1000 candles per request
    # For 1m interval: 1000 minutes = ~16.6 hours per request
    end_time = datetime.utcnow()
    current_start = start_time
    total_inserted = 0
    
    while current_start < end_time:
        # Calculate end of current batch (max 1000 minutes)
        current_end = min(
            current_start + timedelta(minutes=1000),
            end_time
        )
        
        # Convert to milliseconds
        start_ms = int(current_start.timestamp() * 1000)
        end_ms = int(current_end.timestamp() * 1000)
        
        print(f"    📊 Fetching {current_start} to {current_end}...", flush=True)
        
        # Fetch from Binance
        klines = await fetch_binance_klines(symbol, "1m", start_ms, end_ms)
        
        if not klines:
            print(f"    ⚠️  No data returned from Binance", flush=True)
            break
        
        # Insert batch
        inserted = await insert_candles_batch(coin_id, klines)
        total_inserted += inserted
        print(f"    ✅ Inserted {inserted} candles", flush=True)
        
        # Move to next batch
        current_start = current_end + timedelta(minutes=1)
        
        # Rate limit: wait 100ms between requests
        await asyncio.sleep(0.1)
    
    print(f"  🎉 {symbol} backfill complete: {total_inserted} candles inserted", flush=True)
    return total_inserted

async def backfill_all_symbols(coin_ids: dict, max_gap_hours: int = 24 * 30):
    """
    Backfill all symbols in parallel
    
    Args:
        coin_ids: Dict mapping symbol to coin_id (e.g., {"BTCUSDT": 1})
        max_gap_hours: Maximum hours to backfill per symbol
    """
    print("=" * 70, flush=True)
    print("🔄 Starting backfill process...", flush=True)
    print(f"📊 Symbols to check: {len(coin_ids)}", flush=True)
    print(f"⏰ Max gap to fill: {max_gap_hours} hours ({max_gap_hours/24:.1f} days)", flush=True)
    print("=" * 70, flush=True)
    
    # Run backfill for all symbols concurrently (with rate limiting)
    tasks = []
    for symbol, coin_id in coin_ids.items():
        task = backfill_symbol(symbol, coin_id, max_gap_hours)
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    total = sum(r for r in results if isinstance(r, int))
    errors = sum(1 for r in results if isinstance(r, Exception))
    
    print("=" * 70, flush=True)
    print(f"✅ Backfill complete!", flush=True)
    print(f"   Total candles inserted: {total:,}", flush=True)
    if errors > 0:
        print(f"   ⚠️  Errors: {errors}", flush=True)
    print("=" * 70, flush=True)
    
    return total
