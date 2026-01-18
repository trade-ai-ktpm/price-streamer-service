import json
import asyncio
import websockets
import sys
from config import (
    BINANCE_WS_BASE, SYMBOLS, TIMEFRAMES, MAX_BACKFILL_HOURS,
    CLEANUP_ENABLED, RETENTION_DAYS_1M, CLEANUP_INTERVAL_HOURS
)
from redis_publisher import RedisPublisher
from models.price_event import PriceEvent
from database import async_session
from sqlalchemy import text
from datetime import datetime
import time
from backfill import backfill_all_symbols
from cleanup import cleanup_scheduler

publisher = RedisPublisher()

# Load coin IDs from database
coin_ids = {}

async def load_coin_ids():
    global coin_ids
    async with async_session() as session:
        result = await session.execute(text("SELECT id, symbol FROM coins"))
        for row in result:
            symbol = row.symbol
            coin_ids[f"{symbol}USDT"] = row.id
    print(f"Loaded {len(coin_ids)} coin IDs", flush=True)

async def save_candle(symbol: str, timeframe: str, candle_data: dict):
    """Save closed candle to database (only for 1m timeframe, others are aggregated)"""
    if symbol not in coin_ids:
        print(f"Coin ID not found for {symbol}", flush=True)
        return
    
    # Only save 1m candles to DB, other timeframes are aggregated by TimescaleDB
    if timeframe != "1m":
        return
    
    coin_id = coin_ids[symbol]
    kline = candle_data['k']
    
    # Convert timestamp to datetime
    timestamp = datetime.utcfromtimestamp(kline['t'] / 1000)
    
    async with async_session() as session:
        try:
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
                {
                    "coin_id": coin_id,
                    "timestamp": timestamp,
                    "open": float(kline['o']),
                    "high": float(kline['h']),
                    "low": float(kline['l']),
                    "close": float(kline['c']),
                    "volume": float(kline['v'])
                }
            )
            await session.commit()
        except Exception as e:
            print(f"Error saving candle: {e}", flush=True)
            await session.rollback()

async def publish_candle_update(symbol: str, timeframe: str, kline: dict, is_closed: bool):
    """Publish candle update to Redis for realtime chart"""
    candle_update = {
        "type": "candle",
        "symbol": symbol,
        "timeframe": timeframe,
        "timestamp": kline['t'],
        "open": float(kline['o']),
        "high": float(kline['h']),
        "low": float(kline['l']),
        "close": float(kline['c']),
        "volume": float(kline['v']),
        "is_closed": is_closed
    }
    
    # Publish to candle channel: candle:{SYMBOL}:{timeframe}
    channel = f"candle:{symbol}:{timeframe}"
    await publisher.publish_price(channel, candle_update)

async def publish_price_update(symbol: str, price: float, timestamp: int):
    """Publish current price to Redis for price ticker"""
    event = PriceEvent(
        symbol=symbol,
        price=price,
        timestamp=timestamp
    )
    channel = f"price:{symbol}"
    await publisher.publish_price(channel, event.dict())

async def handle_message(message: str):
    """Handle incoming Binance WebSocket message"""
    data = json.loads(message)
    
    # Combined stream payload has 'data' wrapper
    if 'data' in data:
        data = data['data']
    
    kline = data.get('k')
    if not kline:
        return
    
    symbol = kline['s']  # e.g., BTCUSDT
    timeframe = kline['i']  # e.g., 1m, 5m, 1h, etc.
    price = float(kline['c'])
    timestamp = int(time.time() * 1000)
    is_closed = kline['x']  # True if candle is closed
    
    # Always publish candle update for realtime chart
    await publish_candle_update(symbol, timeframe, kline, is_closed)
    
    # Publish price update (only for 1m to avoid duplicates)
    if timeframe == "1m":
        await publish_price_update(symbol, price, timestamp)
    
    # Save to database when 1m candle closes
    if is_closed and timeframe == "1m":
        await save_candle(symbol, timeframe, data)

async def stream_all_symbols():
    """
    Subscribe to all symbols x timeframes via single combined stream connection.
    Binance allows up to 1024 streams per connection.
    We have 5 symbols x 7 timeframes = 35 streams.
    """
    # Build stream list: {symbol}@kline_{timeframe}
    streams = []
    for symbol in SYMBOLS:
        for tf in TIMEFRAMES:
            stream_name = f"{symbol}@kline_{tf}"
            streams.append(stream_name)
    
    total_streams = len(streams)
    print(f"Subscribing to {total_streams} streams (5 coins x 7 timeframes)", flush=True)
    print(f"Symbols: {[s.upper() for s in SYMBOLS]}", flush=True)
    print(f"Timeframes: {TIMEFRAMES}", flush=True)
    
    # Use combined stream endpoint (single connection for all streams)
    url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"
    
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                print(f"✅ Connected to Binance with {total_streams} streams via single connection", flush=True)
                sys.stdout.flush()
                
                async for msg in ws:
                    await handle_message(msg)
                    
        except Exception as e:
            print(f"❌ Connection error: {e}, reconnecting in 3s...", flush=True)
            await asyncio.sleep(3)

async def start():
    """Main entry point"""
    print("=" * 60, flush=True)
    print("🚀 Starting Price Streamer Service", flush=True)
    print(f"📊 Coins: {len(SYMBOLS)} | Timeframes: {len(TIMEFRAMES)} | Total Streams: {len(SYMBOLS) * len(TIMEFRAMES)}", flush=True)
    print("=" * 60, flush=True)
    
    # Load coin IDs from database
    await load_coin_ids()
    
    # Backfill missing data before starting realtime stream
    print("\n🔄 Checking for missing data gaps...", flush=True)
    await backfill_all_symbols(coin_ids, max_gap_hours=MAX_BACKFILL_HOURS)
    print("✅ Backfill check complete\n", flush=True)
    
    # Start cleanup scheduler in background if enabled
    if CLEANUP_ENABLED:
        asyncio.create_task(
            cleanup_scheduler(
                retention_days=RETENTION_DAYS_1M,
                interval_hours=CLEANUP_INTERVAL_HOURS
            )
        )
    else:
        print("⚠️  Data cleanup is DISABLED", flush=True)
    
    # Start streaming (single connection for all)
    await stream_all_symbols()
