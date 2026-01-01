import os

# Binance WS
BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"

# Danh sách cặp tiền realtime
SYMBOLS = os.getenv("SYMBOLS", "btcusdt,ethusdt").split(",")

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = 0