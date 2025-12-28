import asyncio
import logging
from datetime import datetime
import pytz
from src.features.core.config import AMSTERDAM_TZ

logger = logging.getLogger(__name__)

async def dm_scheduler_task(bot_instance):
    """Background task to process pending DMs (from telegram_bot.py:5800)"""
    client = bot_instance
    db_pool = getattr(bot_instance, 'db_pool', None)
    if not db_pool:
        logger.error("DM Scheduler: No database pool found")
        return
        
    while getattr(bot_instance, 'running', True):
        try:
            async with db_pool.acquire() as conn:
                # Safety migrations
                await conn.execute("ALTER TABLE dm_schedule ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending'")
                await conn.execute("ALTER TABLE dm_schedule ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMPTZ")
                
                pending = await conn.fetch(
                    "SELECT * FROM dm_schedule WHERE status = 'pending' AND scheduled_at <= $1",
                    datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                )
            
            for dm in pending:
                user_id = dm['member_id']
                # In the real bot, message_type determines content from templates
                # For now keeping it simple as per previous session stubs
                content = "Scheduled message" 
                
                try:
                    await client.send_message(user_id, content)
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE dm_schedule SET status = 'sent', sent_at = $1 WHERE member_id = $2 AND scheduled_at = $3",
                            datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ), user_id, dm['scheduled_at']
                        )
                    logger.info(f"Successfully sent scheduled DM to {user_id}")
                except Exception as e:
                    logger.error(f"Failed to send DM to {user_id}: {e}")
                
                await asyncio.sleep(2) # Anti-flood
            
            await asyncio.sleep(60) # Check every minute
        except Exception as e:
            logger.error(f"DM Scheduler error: {e}")
            await asyncio.sleep(60)
