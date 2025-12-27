from pyrogram.client import Client
from pyrogram.errors import PeerIdInvalid, UserIsBlocked, FloodWait
import asyncio
import logging
from datetime import datetime, timedelta
import pytz
from src.features.core.config import AMSTERDAM_TZ, MESSAGE_TEMPLATES, DEBUG_GROUP_ID

logger = logging.getLogger(__name__)

class DMManager:
    def __init__(self, app: Client, db_pool, bot_instance):
        self.app = app
        self.db = db_pool
        self.bot = bot_instance

    async def send_dm(self, user_id: int, message: str, retry_count: int = 0) -> bool:
        """Send a DM with error handling and automatic retry queueing"""
        try:
            await self.app.send_message(user_id, message, disable_web_page_preview=True)
            return True
        except (PeerIdInvalid, UserIsBlocked) as e:
            logger.warning(f"Failed to send DM to {user_id}: {type(e).__name__}")
            # Mark as invalid for peer ID escalation system
            if self.db:
                async with self.db.acquire() as conn:
                    await conn.execute(
                        "UPDATE peer_id_checks SET peer_id_established = FALSE WHERE user_id = $1",
                        int(user_id)
                    )
            return False
        except FloodWait as e:
            await asyncio.sleep(float(e.value))
            return await self.send_dm(user_id, message, retry_count)
        except Exception as e:
            logger.error(f"Unexpected error sending DM to {user_id}: {e}")
            return False

    async def schedule_follow_up(self, user_id: int, days: int):
        """Schedule a follow-up DM in the database"""
        if not self.db: return
        
        current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
        send_at = current_time + timedelta(days=days)
        
        async with self.db.acquire() as conn:
            await conn.execute(
                '''INSERT INTO dm_schedule (member_id, scheduled_at, dm_type, status)
                   VALUES ($1, $2, $3, 'pending')''',
                int(user_id), send_at, f"follow_up_{days}d"
            )
