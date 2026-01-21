"""
Auto-refresh TimescaleDB continuous aggregates for all timeframes
"""
import asyncio
from datetime import datetime
from sqlalchemy import text
from database import async_session

# Timeframes to refresh (in order of frequency)
TIMEFRAMES = [
    {"view": "candle_data_5m", "interval_minutes": 5},
    {"view": "candle_data_15m", "interval_minutes": 15},
    {"view": "candle_data_1h", "interval_minutes": 60},
    {"view": "candle_data_4h", "interval_minutes": 240},
    {"view": "candle_data_1d", "interval_minutes": 1440},
    {"view": "candle_data_1w", "interval_minutes": 10080},
]


async def refresh_continuous_aggregate(view_name: str):
    """Refresh a single continuous aggregate view"""
    async with async_session() as session:
        try:
            # Use AUTOCOMMIT isolation level for CALL statement
            await session.connection(execution_options={"isolation_level": "AUTOCOMMIT"})
            await session.execute(
                text(f"CALL refresh_continuous_aggregate('{view_name}', NULL, NULL)")
            )
            print(f"✅ Refreshed {view_name}", flush=True)
        except Exception as e:
            print(f"❌ Error refreshing {view_name}: {e}", flush=True)


async def refresh_all_aggregates():
    """Refresh all continuous aggregates"""
    print("🔄 Refreshing all continuous aggregates...", flush=True)
    
    for tf in TIMEFRAMES:
        await refresh_continuous_aggregate(tf["view"])
    
    print("✅ All aggregates refreshed", flush=True)


async def aggregate_refresh_scheduler(interval_minutes: int = 5):
    """
    Background scheduler to refresh continuous aggregates periodically
    
    Args:
        interval_minutes: How often to refresh (default: 5 minutes)
    """
    print(f"🚀 Starting aggregate refresh scheduler (every {interval_minutes} minutes)", flush=True)
    
    while True:
        try:
            await refresh_all_aggregates()
        except Exception as e:
            print(f"❌ Error in aggregate refresh scheduler: {e}", flush=True)
        
        # Wait for next refresh
        await asyncio.sleep(interval_minutes * 60)
