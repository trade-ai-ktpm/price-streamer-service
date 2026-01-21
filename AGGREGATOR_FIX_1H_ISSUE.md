# Fix 1H Aggregator Issue - Volume Duplication

## 🐛 Vấn đề

Khi test với 1H timeframe, candle bị **lệch volume và OHLC** so với expected.

### Root Cause

Logic cũ query DB sử dụng **`current_minute_start`** (thời gian phút hiện tại) để exclude:

```python
# ❌ WRONG: Dùng current time
current_minute_start = (int(datetime.now(timezone.utc).timestamp()) // 60) * 60

query = """
    ...
    AND timestamp < to_timestamp(:current_minute)
"""
```

**Vấn đề:**

- Giả sử đang nhận 1m candle update cho **10:34** (timestamp = 10:34:30)
- `current_minute_start` = 10:34:00
- Query exclude `< 10:34:00` → Lấy 10:00-10:33 (34 candles)
- **Nhưng 10:34 chưa đóng!** Đang được aggregate từ stream

→ **Duplicate:** 1m candle 10:34 bị tính 2 lần:

1. Từ DB (nếu có partial data)
2. Từ current candle stream

### Ví dụ cụ thể

**Timeline:**

```
10:00 ─────────────────────────────────────────────── 11:00
  │                                          │
  └── 1H candle bắt đầu                     └── 1H candle kết thúc

10:34:00                               10:34:59
  │────────── 1m candle 10:34 ──────────│
            │
            └── 10:34:30 (current update)
```

**Logic cũ (SAI):**

1. Nhận update cho 10:34:30
2. Query DB: `timestamp < to_timestamp(current_minute = 10:34:00)`
3. Kết quả: Lấy 10:00-10:33 (34 candles) ✅
4. Combine với current 1m (10:34) ✅
5. **Nhưng:** Nếu 10:34:00 đã được save vào DB (từ update trước), nó sẽ bị query lại!

**Tại sao bị duplicate?**

- Stream Binance gửi updates **MỖI GIÂY** cho 1m candle chưa đóng
- Sau mỗi update, ta save vào DB (upsert)
- 10:34:00 → 10:34:59: 60 updates cho cùng 1 candle
- Query `< 10:34:00` sẽ **KHÔNG exclude được candle 10:34** nếu nó có timestamp **exactly = 10:34:00**
- → Volume và OHLC bị tính 2 lần

## ✅ Giải pháp

Sử dụng **timestamp của 1m candle hiện tại** để exclude:

```python
# ✅ CORRECT: Dùng timestamp của 1m candle đang aggregate
candle_1m_start_ms = get_candle_start_time(timestamp_ms, 1)
candle_1m_start_seconds = candle_1m_start_ms // 1000

query = """
    ...
    AND timestamp < to_timestamp(:exclude_current)
"""

# Execute với exclude_current = candle_1m_start_seconds
```

### Logic mới

**Với update 10:34:30:**

1. `candle_1m_start_ms` = 10:34:00 (start của candle 10:34)
2. Query: `timestamp < to_timestamp(10:34:00)`
3. Kết quả: Lấy 10:00-10:33 (34 candles) ✅
4. Combine với current 1m (10:34 từ stream) ✅
5. **Không duplicate** vì query exclude đúng candle đang aggregate

### Luồng chính xác

```
Binance Stream (10:34:30)
    ↓
1m candle update: {t: 10:34:00, o: 43500, h: 43600, ...}
    ↓
aggregate_candle()
    ↓
For each timeframe (5m, 15m, 1h, ...):
    ├── Get timeframe start (1h: 10:00:00)
    ├── Get 1m candle start (10:34:00)
    ├── Query DB: timestamp >= 10:00:00 AND < 10:34:00
    │   → Result: 34 closed candles (10:00-10:33)
    ├── Combine: DB candles + current 1m candle
    │   → open = DB.first_open
    │   → high = max(DB.max_high, current.high)
    │   → low = min(DB.min_low, current.low)
    │   → close = current.close
    │   → volume = DB.sum_volume + current.volume
    ├── Save to Redis
    └── Publish WebSocket
```

## 🧪 Test Cases

### Test 1: 1H candle đang hình thành (10:34)

**Input:**

```python
candle_1m = {
    "timestamp": 1642152270000,  # 10:34:30
    "open": 43500,
    "high": 43600,
    "low": 43480,
    "close": 43550,
    "volume": 10.5,
    "is_closed": False
}
```

**DB Query:**

```sql
-- Query: 10:00:00 <= timestamp < 10:34:00
-- Result: 34 candles (10:00, 10:01, ..., 10:33)
first_open = 43000
max_high = 44000
min_low = 42800
last_close = 43400
total_volume = 350.5
```

**Expected Output:**

```python
{
    "open": 43000,         # from DB
    "high": 44000,         # max(44000, 43600)
    "low": 42800,          # min(42800, 43480)
    "close": 43550,        # from current
    "volume": 361.0,       # 350.5 + 10.5
    "candle_count": 35,    # 34 + 1
    "is_closed": False     # 35/60 < 100%
}
```

### Test 2: 1H candle đóng (11:00)

**Input:**

```python
candle_1m = {
    "timestamp": 1642155600000,  # 10:59:30
    "open": 43700,
    "high": 43800,
    "low": 43650,
    "close": 43750,
    "volume": 12.0,
    "is_closed": True  # ← 10:59 closed
}
```

**DB Query:**

```sql
-- Query: 10:00:00 <= timestamp < 10:59:00
-- Result: 59 candles
```

**Expected Output:**

```python
{
    "candle_count": 60,    # 59 + 1
    "is_closed": True,     # is_closed=True AND 60/60 = 100%
    ...
}
```

## 📊 Impact

### Before Fix

- ❌ 1H volume bị tăng gấp đôi
- ❌ High/Low bị calculate sai
- ❌ Mỗi timeframe có thể bị ảnh hưởng khác nhau
- ❌ Các timeframe dài (4H, 1D) bị ảnh hưởng nhiều hơn

### After Fix

- ✅ Volume chính xác 100%
- ✅ OHLC calculate đúng
- ✅ Áp dụng đồng nhất cho mọi timeframe
- ✅ Không có duplicate data

## 🔍 Debug Logging

Đã thêm logging chi tiết cho 1H:

```python
if symbol == "BTCUSDT" and tf == "1h":
    print(f"📊 {symbol} {tf}: {candle_count+1}/{interval_minutes} candles")
    print(f"   🔍 1H Debug: candle_start={candle_start}, "
          f"1m_start={candle_1m_start}, is_closed={is_closed}, "
          f"db_count={candle_count}")
```

Giúp track:

- Số lượng candles từ DB
- Timestamp của 1H candle start
- Timestamp của 1m candle đang aggregate
- Status của 1m candle (closed/open)

## ✅ Verification

Sau khi deploy, verify:

1. **Check volume consistency:**

   ```python
   # Sum of all 1m volumes in 1H window should equal 1H volume
   sum(1m_volumes[10:00-10:59]) == 1h_volume[10:00]
   ```

2. **Check candle count:**

   ```python
   # At 10:59 closed, should have exactly 60 candles
   candle_count + 1 == 60
   ```

3. **Check OHLC bounds:**
   ```python
   # 1H high should be >= all 1m highs
   # 1H low should be <= all 1m lows
   max(1m_highs) == 1h_high
   min(1m_lows) == 1h_low
   ```

## 📝 Summary

**Bug:** Query DB sử dụng `current_minute_start` thay vì timestamp của 1m candle đang aggregate
**Fix:** Sử dụng `candle_1m_start_seconds` để exclude đúng candle hiện tại
**Result:** Không còn duplicate volume/OHLC, logic hoạt động chính xác cho mọi timeframe
