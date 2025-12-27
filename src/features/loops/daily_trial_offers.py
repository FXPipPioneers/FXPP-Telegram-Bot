import asyncio
import logging
from datetime import datetime
import pytz
from pyrogram.enums import ChatMemberStatus
from src.features.core.config import AMSTERDAM_TZ, VIP_GROUP_ID, MESSAGE_TEMPLATES

logger = logging.getLogger(__name__)

class DailyVIPTrialOfferLoop:
    """Background loop: auto-offers VIP trials to eligible free-only members at 09:00 Amsterdam time"""
    
    def __init__(self, app, db_pool, bot_instance):
        self.app = app
        self.db_pool = db_pool
        self.bot = bot_instance
    
    async def run(self):
        """Daily check at 09:00 Amsterdam time to offer VIP trial to free-only members"""
        await asyncio.sleep(60)
        
        while getattr(self.bot, 'running', True):
            try:
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                
                if current_time.hour == 9 and current_time.minute < 2:  # 09:00 - 09:01
                    if not self.db_pool:
                        await asyncio.sleep(60)
                        continue
                    
                    try:
                        async with self.db_pool.acquire() as conn:
                            free_members = await conn.fetch(
                                '''SELECT DISTINCT fgj.user_id FROM free_group_joins fgj
                                   LEFT JOIN trial_offer_history toh ON fgj.user_id = toh.user_id
                                   LEFT JOIN vip_trial_activations vta ON fgj.user_id = vta.user_id
                                   WHERE vta.user_id IS NULL AND 
                                   (toh.user_id IS NULL OR toh.offered_at < NOW() - INTERVAL '24 hours')
                                '''
                            )
                        
                        for member_row in free_members:
                            user_id = member_row['user_id']
                            try:
                                # First check if user is in VIP group
                                try:
                                    chat_member = await self.app.get_chat_member(VIP_GROUP_ID, user_id)
                                    is_in_vip = chat_member.status in [
                                        ChatMemberStatus.MEMBER,
                                        ChatMemberStatus.ADMINISTRATOR,
                                        ChatMemberStatus.OWNER
                                    ]
                                except Exception:
                                    is_in_vip = False
                                
                                # Check if they have an active or expired trial in the database
                                # This is a safety check alongside the SQL exclusion
                                async with self.db_pool.acquire() as conn:
                                    has_trial = await conn.fetchval(
                                        "SELECT 1 FROM vip_trial_activations WHERE user_id = $1", 
                                        user_id
                                    )
                                
                                if not is_in_vip and not has_trial:
                                    # CHECK COOLDOWN: 48 hour pattern (Day 1, 3, 5, 7...)
                                    async with self.db_pool.acquire() as conn:
                                        last_offer = await conn.fetchval(
                                            "SELECT offered_at FROM trial_offer_history WHERE user_id = $1", 
                                            user_id
                                        )
                                        
                                        should_send = False
                                        if last_offer is None:
                                            should_send = True # First time
                                        else:
                                            # Ensure last offer was at least 40 hours ago to skip every other day at 09:00
                                            if last_offer.tzinfo is None:
                                                last_offer = AMSTERDAM_TZ.localize(last_offer)
                                            hours_since = (current_time - last_offer).total_seconds() / 3600
                                            if hours_since >= 40: # 40h covers the 48h gap for 09:00 checks
                                                should_send = True

                                        if should_send:
                                            offer_message = MESSAGE_TEMPLATES["Engagement & Offers"]["Daily VIP Trial Offer"]["message"]
                                            try:
                                                await self.app.send_message(user_id, offer_message)
                                                await conn.execute(
                                                    '''INSERT INTO trial_offer_history (user_id, offered_at)
                                                       VALUES ($1, $2)
                                                       ON CONFLICT (user_id) DO UPDATE SET offered_at = $2''',
                                                    user_id, current_time
                                                )
                                                logger.info(f"✅ Sent daily VIP trial offer DM to user {user_id}")
                                                await self.bot.log_to_debug(f"✅ Sent daily VIP trial offer DM to user {user_id}", user_id=user_id)
                                            except Exception as e:
                                                logger.error(f"❌ Could not send daily trial offer DM to {user_id}: {e}")
                                else:
                                    # If they are in VIP or have had a trial, make sure they are recorded so they are skipped next time
                                    if has_trial:
                                        logger.debug(f"Skipping user {user_id}: already had a trial.")
                                    if is_in_vip:
                                        logger.info(f"Skipping user {user_id}: already in VIP group. Permanently excluding.")
                                        async with self.db_pool.acquire() as conn:
                                            # Record them in vip_trial_activations to permanently exclude them
                                            # This ensures has_trial will be TRUE forever for this user.
                                            await conn.execute(
                                                '''INSERT INTO vip_trial_activations (user_id, activation_date, expiry_date)
                                                   VALUES ($1, $2, $2)
                                                   ON CONFLICT (user_id) DO NOTHING''',
                                                user_id, current_time
                                            )
                            except Exception as e:
                                logger.error(f"Error processing member {user_id} in daily offer: {e}")
                        
                        await asyncio.sleep(120)
                    except Exception as e:
                        logger.error(f"Error in daily VIP trial offer check: {e}")
                        await asyncio.sleep(120)
                else:
                    await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in daily_vip_trial_offer_loop: {e}")
                await asyncio.sleep(60)


def create_daily_trial_offer_loop(app, db_pool, bot_instance):
    """Factory function to create and return the daily trial offer loop"""
    loop = DailyVIPTrialOfferLoop(app, db_pool, bot_instance)
    return loop.run()
