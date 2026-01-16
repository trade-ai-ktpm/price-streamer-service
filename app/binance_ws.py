import json
import asyncio
import websockets
import sys
from config import BINANCE_WS_BASE, SYMBOLS
from redis_publisher import RedisPublisher
from models.price_event import PriceEvent
import time

publisher = RedisPublisher()

async def handle_message(message: str):
    data = json.loads(message)

    # miniTicker payload
    symbol = data["s"]
    price = float(data["c"])   # close price
    timestamp = int(time.time() * 1000)

    event = PriceEvent(
        symbol=symbol,
        price=price,
        timestamp=timestamp
    )

    channel = f"price:{symbol}"
    await publisher.publish_price(channel, event.dict())
    print(f"Published {symbol}: {price}", flush=True)

async def stream_symbol(symbol: str):
    url = f"{BINANCE_WS_BASE}/{symbol}@miniTicker"

    while True:
        try:
            async with websockets.connect(url) as ws:
                print(f"Connected to {symbol}", flush=True)
                sys.stdout.flush()
                async for msg in ws:
                    await handle_message(msg)

        except Exception as e:
            print(f"Error {symbol}: {e}, reconnecting...", flush=True)
            await asyncio.sleep(3)

async def start():
    print(f"Starting price streamer for symbols: {SYMBOLS}", flush=True)
    tasks = [
        asyncio.create_task(stream_symbol(symbol))
        for symbol in SYMBOLS
    ]
    await asyncio.gather(*tasks)

