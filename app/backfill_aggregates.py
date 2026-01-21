"""
Refresh continuous aggregate materialized views for higher timeframes
"""
import asyncio
from datetime import datetime, timezone
from sqlalchemy import text
from database import async_session

# Timeframe materialized views
TIMEFRAMES = ["candle_data_5m", "candle_data_15m", "candle_data_1h", 
              "candle_data_4h", "candle_data_1d", "candle_data_1w"]


async def refresh_view(view_name: str):
    """Refresh a continuous aggregate materialized view"""
    print(f"\n🔄 Refreshing {view_name}...")
    
    async with async_session() as session:
        # Execute outside transaction - use AUTOCOMMIT
        await session.connection(execution_options={"isolation_level": "AUTOCOMMIT"})
        await session.execute(text(f"CALL refresh_continuous_aggregate('{view_name}', NULL, NULL)"))
        
        # Get count
        count_result = await session.execute(
            text(f"SELECT COUNT(*) FROM {view_name}")
        )
        count = count_result.scalar()
        
        print(f"  ✅ {view_name}: {count:,} candles")


async def main():
    """Refresh all continuous aggregate views"""
    print("=" * 70)
    print("🚀 Refreshing Continuous Aggregates")
    print(f"📊 Views to refresh: {len(TIMEFRAMES)}")
    print("=" * 70)
    
    # Check 1m data
    async with async_session() as session:
        result = await session.execute(
            text("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM candle_data_1m")
        )
        count, min_time, max_time = result.fetchone()
        print(f"\n📈 1m candles available: {count:,}")
        print(f"   Range: {min_time} → {max_time}")
    
    if count == 0:
        print("\n⚠️  No 1m data found. Run backfill first!")
        return
    
    # Refresh each view
    for view_name in TIMEFRAMES:
        try:
            await refresh_view(view_name)
        except Exception as e:
            print(f"  ❌ Error refreshing {view_name}: {e}")
    
    print("\n" + "=" * 70)
    print("✅ All views refreshed!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
