# Lấy giá realtime từ Binance WebSocket → Redis

## Chức năng

- Kết nối Binance WebSocket (35 streams: 5 coins × 7 timeframes)
- Nhận giá realtime (kline/candlestick data)
- Lưu candle đã đóng vào TimescaleDB
- Publish realtime vào Redis Pub/Sub
- **Backfill tự động** khi có gap trong data
- **Cleanup tự động** data cũ để tránh DB phình to

## Environment Variables

### Backfill Configuration

```bash
MAX_BACKFILL_HOURS=168        # Backfill up to 7 days (default)
```

### Data Cleanup Configuration

```bash
CLEANUP_ENABLED=true          # Enable/disable auto cleanup (default: true)
RETENTION_DAYS_1M=30          # Keep 1m candles for 30 days (default)
CLEANUP_INTERVAL_HOURS=24     # Run cleanup every 24 hours (default)
```

### Symbols & Timeframes

```bash
SYMBOLS=btcusdt,ethusdt,bnbusdt,solusdt,xrpusdt
TIMEFRAMES=1m,5m,15m,1h,4h,1d,1w  # Hardcoded in config.py
```

## Features

### 🔄 Auto Backfill

Khi service restart sau downtime:

- Tự động detect gap trong database
- Fetch missing data từ Binance REST API
- Insert batch 1000 candles per request
- Support backfill up to `MAX_BACKFILL_HOURS` hours

**Example:**

```
Server down: 10:00 AM → 5:00 PM (7 hours = 420 minutes)
On restart:
  🔍 Checking BTCUSDT for missing data...
  ⚠️  Gap detected: 420 minutes (7.0 hours)
  📥 Backfilling from 10:01 AM to 5:00 PM...
  ✅ Inserted 420 candles
  🎉 BTCUSDT backfill complete
```

### 🗑️ Auto Cleanup

Định kỳ xóa data cũ:

- Chạy mỗi `CLEANUP_INTERVAL_HOURS` hours
- Xóa candles older than `RETENTION_DAYS_1M` days
- Hiển thị stats trước/sau cleanup
- Giữ DB size hợp lý

**Example:**

```
🧹 Starting cleanup job (retention: 30 days)...
📊 Database Statistics:
   Total candles: 2,160,000
   Days of data: 45
   Table size: 450 MB
🗑️  Cleaned up 720,000 candles older than 30 days
📊 After cleanup:
   Total candles: 1,440,000
   Days of data: 30
   Table size: 300 MB
```

## Retention Strategy

| Timeframe   | Retention                                                  | Reason                                     |
| ----------- | ---------------------------------------------------------- | ------------------------------------------ |
| 1m          | 30 days                                                    | High volume, only for short-term analysis  |
| 5m, 15m, 1h | Aggregated from 1m (via TimescaleDB continuous aggregates) |                                            |
| 4h, 1d, 1w  | Keep indefinitely                                          | Low volume, important for long-term trends |
