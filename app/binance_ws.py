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
from aggregator import aggregate_candle
from aggregate_refresher import aggregate_refresh_scheduler

publisher = RedisPublisher()

# Load coin IDs from database
coin_ids = {}

async def load_coin_ids():
    global coin_ids
    async with async_session() as session:
        result = await session.execute(text("SELECT id, symbol FROM coins"))
        for row in result:
            symbol = row.symbol
            coin_ids[symbol] = row.id
    print(f"Loaded {len(coin_ids)} coin IDs", flush=True)

async def save_candle(symbol: str, timeframe: str, candle_data: dict):
    """Save closed 1m candle to database"""
    if symbol not in coin_ids:
        print(f"Coin ID not found for {symbol}", flush=True)
        return
    
    # Only save 1m candles to DB
    # Other timeframes will be computed by aggregator and saved when complete
    if timeframe != "1m":
        return
    
    coin_id = coin_ids[symbol]
    kline = candle_data['k']
    
    # Convert timestamp to datetime
    timestamp = datetime.utcfromtimestamp(kline['t'] / 1000)
    
    async with async_session() as session:
        try:
            query = text("""
                INSERT INTO candle_data_1m (coin_id, timestamp, open, high, low, close, volume)
                VALUES (:coin_id, :timestamp, :open, :high, :low, :close, :volume)
                ON CONFLICT (coin_id, timestamp) DO UPDATE
                SET open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """)
            
            await session.execute(
                query,
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
    
    # DEBUG: Log message receipt
    print(f"📨 Received message from Binance", flush=True)
    
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
    
    # DEBUG
    print(f"[Binance WS] {symbol} {timeframe} is_closed={is_closed}", flush=True)
    
    # Publish candle update for this timeframe (Aggregator handles higher timeframes)
    # Frontend needs these to detect candle close events
    await publish_candle_update(symbol, timeframe, kline, is_closed)
    print(f"[Binance WS] Published candle:{symbol}:{timeframe}", flush=True)
    
    # Publish price update (only for 1m to avoid duplicates)
    if timeframe == "1m":
        await publish_price_update(symbol, price, timestamp)
        print(f"[Binance WS] Published price:{symbol}", flush=True)
    
    # For 1m candles: save to DB and aggregate to higher timeframes
    if timeframe == "1m":
        if is_closed:
            await save_candle(symbol, timeframe, data)
        
        # Aggregate to 5m/15m/1h/4h/1d/1w
        # DISABLED: Using TimescaleDB continuous aggregates instead
        # Real-time aggregation was causing incorrect data
        candle_1m = {
            "timestamp": kline['t'],
            "open": float(kline['o']),
            "high": float(kline['h']),
            "low": float(kline['l']),
            "close": float(kline['c']),
            "volume": float(kline['v']),
            "is_closed": is_closed
        }
        await aggregate_candle(symbol, candle_1m)

async def stream_all_symbols():
    """
    Subscribe to all symbols x timeframes via single combined stream connection.
    Binance allows up to 1024 streams per connection.
    We have 5 symbols x 7 timeframes = 35 streams.
    """
    # Build stream list: ONLY subscribe 1m candles
    # Aggregator will create 5m, 15m, 1h, 4h, 1d, 1w from 1m data
    streams = []
    for symbol in SYMBOLS:
        stream_name = f"{symbol}@kline_1m"
        streams.append(stream_name)
    
    total_streams = len(streams)
    print(f"Subscribing to {total_streams} streams (5 coins x 1m only)", flush=True)
    print(f"Symbols: {[s.upper() for s in SYMBOLS]}", flush=True)
    print(f"Aggregator will create: 5m, 15m, 1h, 4h, 1d, 1w", flush=True)
    
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
    
    # Start aggregate refresh scheduler in background (every 5 minutes)
    asyncio.create_task(aggregate_refresh_scheduler(interval_minutes=5))
    
    # Start streaming (single connection for all)
    await stream_all_symbols()
