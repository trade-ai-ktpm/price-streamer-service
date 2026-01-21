# Aggregator Logic - Simplified Version

## Tổng quan

Logic tính toán current candle cho các timeframe cao hơn (5m, 15m, 1h, 4h, 1d, 1w) từ 1m candles được đơn giản hóa theo nguyên tắc:

**Query closed 1m candles từ DB + Current 1m candle = Current aggregated candle**

## Flow hoạt động

### 1. Nhận 1m candle update từ Binance WebSocket

```
Binance WebSocket → 1m candle update (mỗi giây)
                   ↓
            aggregate_candle()
```

### 2. Tính toán cho mỗi timeframe

Với mỗi timeframe (5m, 15m, 1h, 4h, 1d, 1w):

#### Bước 1: Xác định mốc bắt đầu của timeframe

```python
# VD: Hiện tại là 10:04:30
# Với 5m timeframe → start = 10:00:00
candle_start_ms = get_candle_start_time(timestamp_ms, interval_minutes)
```

#### Bước 2: Query closed 1m candles từ DB

```sql
SELECT
    (array_agg(open ORDER BY timestamp))[1] as first_open,
    MAX(high) as max_high,
    MIN(low) as min_low,
    (array_agg(close ORDER BY timestamp DESC))[1] as last_close,
    SUM(volume) as total_volume,
    COUNT(*) as candle_count
FROM candle_data_1m
WHERE coin_id = ...
    AND timestamp >= start_time      -- 10:00:00
    AND timestamp < current_minute   -- 10:04:00 (loại bỏ phút hiện tại chưa đóng)
```

**Ví dụ:** Hiện tại là 10:04:30

- Query lấy 1m candles đã đóng: 10:00, 10:01, 10:02, 10:03 (4 candles)
- Current 1m candle (chưa đóng): 10:04

#### Bước 3: Tính toán aggregated candle

```python
if có closed 1m candles từ DB:
    open = first_open từ DB           # Open của candle đầu tiên
    high = max(DB max_high, current_1m_high)  # High cao nhất
    low = min(DB min_low, current_1m_low)     # Low thấp nhất
    close = current_1m_close          # Close hiện tại
    volume = DB total_volume + current_1m_volume
    candle_count = DB candle_count
else:
    # Chưa có closed candles, dùng current 1m candle
    open = current_1m_open
    high = current_1m_high
    low = current_1m_low
    close = current_1m_close
    volume = current_1m_volume
    candle_count = 0
```

#### Bước 4: Xác định candle có đóng không

```python
should_close = is_closed and (candle_count + 1 == interval_minutes)
```

Ví dụ với 5m:

- Cần 5 candles 1m (10:00, 10:01, 10:02, 10:03, 10:04)
- Khi 10:04 đóng (`is_closed=True`) và có đủ 5 candles → 5m candle đóng

#### Bước 5: Publish và lưu Redis

```python
# 1. Lưu vào Redis cho API access
redis_key = f"current_candle:{symbol}:{timeframe}"
await redis.setex(redis_key, ttl, json.dumps(candle_data))

# 2. Publish WebSocket cho realtime updates
channel = f"candle:{symbol}:{timeframe}"
await publisher.publish_price(channel, candle_update)

# 3. Nếu candle đóng, publish close event và xóa Redis
if should_close:
    await publisher.publish_price(channel, candle_closed_event)
    await redis.delete(redis_key)
```

## So sánh logic cũ vs mới

### Logic cũ (phức tạp)

- ❌ In-memory state management với `incomplete_candles` dict
- ❌ Track `seen_1m_candles` set để deduplicate
- ❌ Track `volume_by_1m` dict để update volume
- ❌ Phải restore state từ DB khi restart
- ❌ Logic phức tạp để handle duplicate/missing candles
- ❌ Dễ bị sai khi có race condition hoặc out-of-order updates

### Logic mới (đơn giản)

- ✅ **Stateless** - không cần in-memory state
- ✅ **Source of truth từ DB** - mỗi lần update query lại DB
- ✅ **Không cần restore** - không có state để restore
- ✅ **Đơn giản hơn nhiều** - 1 query + tính toán OHLCV
- ✅ **Dễ debug** - mỗi update độc lập
- ✅ **Không bị race condition** - DB đã handle deduplicate

## Ví dụ cụ thể

### Case 1: 5m candle đang hình thành

**Thời điểm 10:04:30**

1. Nhận 1m candle update:

```json
{
  "timestamp": 1642152270000, // 10:04:30
  "open": 43500,
  "high": 43600,
  "low": 43480,
  "close": 43550,
  "volume": 10.5,
  "is_closed": false
}
```

2. Query DB lấy closed 1m candles (10:00, 10:01, 10:02, 10:03):

```json
{
  "first_open": 43200,
  "max_high": 43700,
  "min_low": 43100,
  "last_close": 43450,
  "total_volume": 42.3,
  "candle_count": 4
}
```

3. Tính toán 5m current candle:

```json
{
  "open": 43200, // first_open từ DB
  "high": 43700, // max(43700, 43600)
  "low": 43100, // min(43100, 43480)
  "close": 43550, // current_1m_close
  "volume": 52.8, // 42.3 + 10.5
  "is_closed": false // Chưa đủ 5 candles hoặc 1m chưa đóng
}
```

### Case 2: 5m candle đóng

**Thời điểm 10:05:00 (1m candle 10:04 vừa đóng)**

1. Nhận 1m candle update với `is_closed: true`
2. Query DB → 4 closed candles
3. `candle_count + 1 = 5` và `is_closed = true`
4. → `should_close = true`
5. Publish `candle_closed` event
6. Xóa Redis key (sẽ tạo mới khi 10:05 bắt đầu)

## Client side handling

Client nhận WebSocket message:

```javascript
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);

  if (data.type === "candle") {
    // Thay thế TOÀN BỘ current candle cũ bằng current candle mới
    chart.updateCandle(data.symbol, data.timeframe, {
      time: data.timestamp,
      open: data.open,
      high: data.high,
      low: data.low,
      close: data.close,
      volume: data.volume,
    });
  }

  if (data.type === "candle_closed") {
    // Candle đóng → convert current candle thành historical candle
    chart.finalizeCandle(data.symbol, data.timeframe, data.candle);
    // Bắt đầu candle mới
    chart.startNewCandle(data.symbol, data.timeframe);
  }
};
```

## Performance considerations

### DB Query overhead

- **Frequency:** Mỗi giây có ~1 query cho mỗi timeframe (6 timeframes × số symbols)
- **Query cost:** Simple aggregation với index trên `(coin_id, timestamp)` → Fast
- **Cache:** TimescaleDB tự cache recent chunks trong memory

### Optimization nếu cần

Nếu DB load quá cao, có thể cache query result trong Redis:

```python
# Cache key: f"closed_candles:{symbol}:{timeframe}:{candle_start}"
# TTL: 60 seconds
# Invalidate khi có 1m candle mới đóng
```

Nhưng với workload hiện tại (~10 symbols × 6 timeframes = 60 queries/second), PostgreSQL + TimescaleDB handle dễ dàng.

## Advantages

1. **Simplicity** - Dễ hiểu, dễ maintain
2. **Correctness** - DB là source of truth, không bị mất data
3. **Stateless** - Restart service không ảnh hưởng
4. **Debuggable** - Mỗi update độc lập, dễ trace
5. **Scalable** - Có thể scale horizontal (multiple instances)

## Database requirements

Đảm bảo index tối ưu:

```sql
-- Index cho query nhanh
CREATE INDEX idx_candle_data_1m_coin_timestamp
ON candle_data_1m (coin_id, timestamp DESC);

-- Partition theo thời gian (TimescaleDB)
SELECT create_hypertable('candle_data_1m', 'timestamp');
```

## Kết luận

Logic mới **đơn giản hơn 10x** so với logic cũ, đồng thời **chính xác hơn** và **dễ maintain hơn**.

Trade-off duy nhất là thêm DB queries, nhưng với TimescaleDB và proper indexing, đây không phải vấn đề.
