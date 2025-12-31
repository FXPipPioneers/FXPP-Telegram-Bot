import asyncio
import logging
from datetime import datetime, timedelta, timezone
import pytz
from src.features.core.config import AMSTERDAM_TZ, FREE_GROUP_ID, MESSAGE_TEMPLATES

logger = logging.getLogger(__name__)

class EngagementTrackingLoop:
    """Background loop: rewards engaged free group members with 50% discount"""
    
    def __init__(self, app, db_pool, bot_instance):
        self.app = app
        self.db_pool = db_pool
        self.bot = bot_instance
    
    async def send_engagement_discount_dm(self, user_id: int):
        """Send 50% discount DM to engaged free group members"""
        try:
            message = MESSAGE_TEMPLATES["Engagement & Offers"]["Engagement Discount (50% Off)"]["message"]
            await self.app.send_message(user_id, message)
            logger.info(f"✅ Sent engagement discount DM to user {user_id}")
        except Exception as e:
            error_msg = f"❌ Could not send engagement discount DM to {user_id}: {e}"
            logger.error(error_msg)
            if hasattr(self.bot, 'log_to_debug'):
                message = MESSAGE_TEMPLATES["Engagement & Offers"]["Engagement Discount (50% Off)"]["message"]
                await self.bot.log_to_debug(error_msg, is_error=True, user_id=user_id)
    
    async def recover_offline_engagement(self):
        """Check for users who hit engagement threshold while offline (Feature 3)"""
        try:
            if not self.db_pool or not FREE_GROUP_ID: return
            
            if hasattr(self.bot, 'log_to_debug'):
                await self.bot.log_to_debug("🔍 Checking for offline engagement rewards...")
            
            current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
            
            async with self.db_pool.acquire() as conn:
                eligible_users = await conn.fetch(
                    'SELECT user_id, joined_at FROM free_group_joins WHERE discount_sent = FALSE'
                )
                
                recovered_count = 0
                for row in eligible_users:
                    user_id = row['user_id']
                    joined_at = row['joined_at']
                    
                    two_weeks_ago = joined_at + timedelta(days=14)
                    if current_time < two_weeks_ago: continue
                    
                    reaction_count = await conn.fetchval(
                        '''SELECT COUNT(DISTINCT message_id) FROM emoji_reactions
                           WHERE user_id = $1 AND reaction_time > $2''',
                        user_id, two_weeks_ago
                    )
                    
                    if reaction_count >= 5:
                        await self.send_engagement_discount_dm(user_id)
                        await conn.execute(
                            'UPDATE free_group_joins SET discount_sent = TRUE WHERE user_id = $1',
                            user_id
                        )
                        recovered_count += 1
                
                if recovered_count > 0 and hasattr(self.bot, 'log_to_debug'):
                    await self.bot.log_to_debug(f"✅ Recovered {recovered_count} offline engagement discounts.")
        except Exception as e:
            logger.error(f"Error recovering offline engagement: {e}")

    async def run(self):
        """Background loop: tracks emoji reactions and rewards engagement (runs every 2 hours)"""
        await asyncio.sleep(120)  # Wait 2 minutes before starting
        
        while getattr(self.bot, 'running', True):
            try:
                if not self.db_pool or not FREE_GROUP_ID:
                    await asyncio.sleep(7200)
                    continue
                
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                
                # Get all free group joins
                async with self.db_pool.acquire() as conn:
                    joins = await conn.fetch(
                        'SELECT user_id, joined_at, discount_sent FROM free_group_joins WHERE discount_sent = FALSE'
                    )
                
                for join_row in joins:
                    user_id = join_row['user_id']
                    joined_at = join_row['joined_at']
                    
                    # Check if user has been in group for more than 2 weeks
                    two_weeks_ago = joined_at + timedelta(days=14)
                    if current_time < two_weeks_ago:
                        continue
                    
                    # Count reactions AFTER 2-week mark
                    async with self.db_pool.acquire() as conn:
                        reaction_count = await conn.fetchval(
                            '''SELECT COUNT(DISTINCT message_id) FROM emoji_reactions
                               WHERE user_id = $1 AND reaction_time > $2''',
                            user_id, two_weeks_ago
                        )
                    
                    # If user has 5+ reactions after 2 weeks, send discount DM
                    if reaction_count >= 5:
                        await self.send_engagement_discount_dm(user_id)
                        
                        # Mark as sent
                        async with self.db_pool.acquire() as conn:
                            await conn.execute(
                                'UPDATE free_group_joins SET discount_sent = TRUE WHERE user_id = $1',
                                user_id
                            )
                        
                        logger.info(f"Sent engagement discount DM to user {user_id} (5+ reactions after 2 weeks)")
                
                await asyncio.sleep(7200)  # Check every 2 hours
            except Exception as e:
                if hasattr(self.bot, 'log_to_debug'):
                    await self.bot.log_to_debug(f"Error in engagement tracking loop: {e}", is_error=True)
                await asyncio.sleep(7200)


def create_engagement_tracking_loop(app, db_pool, bot_instance):
    """Factory function to create and return the engagement tracking loop"""
    loop = EngagementTrackingLoop(app, db_pool, bot_instance)
    return loop.run()
