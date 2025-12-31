import asyncio
import logging
from datetime import datetime
from src.features.core.config import AMSTERDAM_TZ, MESSAGE_TEMPLATES

logger = logging.getLogger(__name__)

class WelcomeDMRetryLoop:
    """Feature 5: Welcome DM Retry System"""
    def __init__(self, bot):
        self.bot = bot
        self.running = True

    async def run(self):
        """Run the retry loop every 2 minutes"""
        logger.info("🚀 Welcome DM Retry Loop started.")
        while self.running:
            try:
                await self.retry_failed_welcome_dms()
            except Exception as e:
                logger.error(f"Error in Welcome DM Retry Loop: {e}")
            await asyncio.sleep(120) # 2 minutes

    async def retry_failed_welcome_dms(self):
        """Process pending welcome DMs from the database"""
        if not self.bot.db_pool:
            return

        async with self.bot.db_pool.acquire() as conn:
            pending = await conn.fetch(
                "SELECT * FROM pending_welcome_dms WHERE failed_attempts < 5 ORDER BY last_attempt ASC NULLS FIRST"
            )

            if not pending:
                return

            for record in pending:
                user_id = record['user_id']
                attempts = record['failed_attempts']
                msg_content = record['message_content'] or MESSAGE_TEMPLATES["Engagement & Offers"]["Daily VIP Trial Offer"]["message"]
                
                try:
                    # Try to establish peer connection
                    await self.bot.get_users([user_id])
                    await asyncio.sleep(1)
                    await self.bot.send_message(user_id, msg_content)
                    
                    # Success! Remove from pending
                    await conn.execute("DELETE FROM pending_welcome_dms WHERE user_id = $1", user_id)
                    
                    # Update peer_id_checks
                    await conn.execute("UPDATE peer_id_checks SET welcome_dm_sent = TRUE WHERE user_id = $1", user_id)
                    
                    await self.bot.log_to_debug(f"✅ **Retry Success**: Welcome DM delivered to {user_id} (Attempt {attempts + 1})")
                    
                except Exception as e:
                    logger.warning(f"Retry failed for user {user_id}: {e}")
                    await conn.execute(
                        "UPDATE pending_welcome_dms SET failed_attempts = failed_attempts + 1, last_attempt = NOW() WHERE user_id = $1",
                        user_id
                    )
                    if attempts + 1 >= 5:
                        await self.bot.log_to_debug(f"❌ **Retry Failed**: Welcome DM failed for {user_id} after 5 attempts.", is_error=True)
