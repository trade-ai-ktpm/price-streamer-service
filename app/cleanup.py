"""
Data cleanup module - Remove old candle data to prevent database bloat
"""
import asyncio
from datetime import datetime, timedelta
from sqlalchemy import text
from database import async_session
from config import CLEANUP_ENABLED, RETENTION_DAYS_1M

async def cleanup_old_candles(retention_days: int = 7):
    """
    Delete candles older than retention_days from candle_data_1m table
    
    Args:
        retention_days: Number of days to keep (default: 7 days)
    
    Returns:
        Number of rows deleted
    """
    cutoff_time = datetime.utcnow() - timedelta(days=retention_days)
    
    async with async_session() as session:
        try:
            result = await session.execute(
                text("""
                    DELETE FROM candle_data_1m
                    WHERE timestamp < :cutoff_time
                """),
                {"cutoff_time": cutoff_time}
            )
            await session.commit()
            deleted_count = result.rowcount
            
            if deleted_count > 0:
                print(f"🗑️  Cleaned up {deleted_count:,} candles older than {retention_days} days (before {cutoff_time})", flush=True)
            else:
                print(f"✅ No old candles to clean (retention: {retention_days} days)", flush=True)
            
            return deleted_count
        except Exception as e:
            print(f"❌ Error during cleanup: {e}", flush=True)
            await session.rollback()
            return 0

async def get_table_stats():
    """Get statistics about candle_data_1m table"""
    async with async_session() as session:
        try:
            result = await session.execute(
                text("""
                    SELECT 
                        COUNT(*) as total_rows,
                        MIN(timestamp) as oldest_candle,
                        MAX(timestamp) as newest_candle,
                        COUNT(DISTINCT coin_id) as total_coins,
                        pg_size_pretty(pg_total_relation_size('candle_data_1m')) as table_size
                    FROM candle_data_1m
                """)
            )
            row = result.fetchone()
            
            if row and row.total_rows > 0:
                stats = {
                    "total_rows": row.total_rows,
                    "oldest_candle": row.oldest_candle,
                    "newest_candle": row.newest_candle,
                    "total_coins": row.total_coins,
                    "table_size": row.table_size,
                    "days_of_data": (row.newest_candle - row.oldest_candle).days if row.newest_candle and row.oldest_candle else 0
                }
                return stats
            return None
        except Exception as e:
            print(f"❌ Error getting table stats: {e}", flush=True)
            return None

async def print_cleanup_summary():
    """Print database statistics before/after cleanup"""
    stats = await get_table_stats()
    
    if stats:
        print("=" * 70, flush=True)
        print("📊 Database Statistics:", flush=True)
        print(f"   Total candles: {stats['total_rows']:,}", flush=True)
        print(f"   Coins tracked: {stats['total_coins']}", flush=True)
        print(f"   Oldest candle: {stats['oldest_candle']}", flush=True)
        print(f"   Newest candle: {stats['newest_candle']}", flush=True)
        print(f"   Days of data: {stats['days_of_data']}", flush=True)
        print(f"   Table size: {stats['table_size']}", flush=True)
        print("=" * 70, flush=True)
    else:
        print("📊 No data in database yet", flush=True)

async def run_cleanup_job(retention_days: int = 7):
    """
    Run a single cleanup job
    
    Args:
        retention_days: Number of days to keep
    """
    print(f"\n🧹 Starting cleanup job (retention: {retention_days} days)...", flush=True)
    
    # Print stats before cleanup
    await print_cleanup_summary()
    
    # Perform cleanup
    deleted = await cleanup_old_candles(retention_days)
    
    # Print stats after cleanup
    if deleted > 0:
        print(f"\n📊 After cleanup:", flush=True)
        await print_cleanup_summary()
    
    print(f"✅ Cleanup job completed\n", flush=True)

async def cleanup_scheduler(retention_days: int = 7, interval_hours: int = 24):
    """
    Periodic cleanup scheduler
    
    Args:
        retention_days: Number of days to keep
        interval_hours: How often to run cleanup (default: 24 hours)
    """
    print("=" * 70, flush=True)
    print("🧹 Data Cleanup Scheduler Started", flush=True)
    print(f"   Retention policy: Keep last {retention_days} days", flush=True)
    print(f"   Cleanup interval: Every {interval_hours} hours", flush=True)
    print("=" * 70, flush=True)
    
    while True:
        try:
            await run_cleanup_job(retention_days)
            
            # Wait until next cleanup
            next_run = datetime.utcnow() + timedelta(hours=interval_hours)
            print(f"⏰ Next cleanup scheduled at: {next_run} (in {interval_hours} hours)", flush=True)
            await asyncio.sleep(interval_hours * 3600)
            
        except Exception as e:
            print(f"❌ Cleanup scheduler error: {e}", flush=True)
            print(f"⏰ Retrying in 1 hour...", flush=True)
            await asyncio.sleep(3600)
