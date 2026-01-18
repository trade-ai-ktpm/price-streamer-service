import os

# Binance WS
BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"

# Danh sách cặp tiền realtime (5 coins)
SYMBOLS = os.getenv("SYMBOLS", "btcusdt,ethusdt,bnbusdt,solusdt,adausdt").split(",")

# 7 timeframes để subscribe
TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]

# Backfill configuration (in hours)
MAX_BACKFILL_HOURS = int(os.getenv("MAX_BACKFILL_HOURS", 24 * 7))  # Default: 7 days

# Data cleanup configuration
CLEANUP_ENABLED = os.getenv("CLEANUP_ENABLED", "true").lower() == "true"
RETENTION_DAYS_1M = int(os.getenv("RETENTION_DAYS_1M", 30))  # Keep 1m candles for 30 days
CLEANUP_INTERVAL_HOURS = int(os.getenv("CLEANUP_INTERVAL_HOURS", 24))  # Run cleanup every 24 hours

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://aiuser:ngocphat@timescaledb-ai:5432/aidb")

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