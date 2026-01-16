import os

# Binance WS
BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"

# Danh sách cặp tiền realtime
SYMBOLS = os.getenv("SYMBOLS", "btcusdt,ethusdt").split(",")

# Redis - Parse from REDIS_URL
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
if REDIS_URL.startswith("redis://"):
    redis_host_port = REDIS_URL.replace("redis://", "").split("/")[0]
    if ":" in redis_host_port:
        REDIS_HOST, port_str = redis_host_port.split(":")
        REDIS_PORT = int(port_str)
    else:
        REDIS_HOST = redis_host_port
        REDIS_PORT = 6379
else:
    REDIS_HOST = "localhost"
    REDIS_PORT = 6379

REDIS_DB = 0