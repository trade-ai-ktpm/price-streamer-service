import asyncio
from app.binance_ws import start

if __name__ == "__main__":
    asyncio.run(start())
