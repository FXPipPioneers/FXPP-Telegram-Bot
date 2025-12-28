import asyncio
import logging
from datetime import datetime, timedelta, timezone, timedelta
import pytz
from src.features.core.config import AMSTERDAM_TZ, AUTO_ROLE_CONFIG, MESSAGE_TEMPLATES, VIP_GROUP_ID

logger = logging.getLogger(__name__)

class TrialExpiryLoop:
    """Background loop: monitors VIP trial expiry times and removes expired users"""
    
    def __init__(self, app, db_pool, bot_instance):
        self.app = app
        self.db_pool = db_pool
        self.bot = bot_instance
    
    async def expire_trial(self, member_id: str):
        """Handle trial expiration for a member"""
        try:
            data = AUTO_ROLE_CONFIG['active_members'].get(member_id)
            if not data:
                return
            
            try:
                await self.app.ban_chat_member(VIP_GROUP_ID, int(member_id))
                await asyncio.sleep(1)
                await self.app.unban_chat_member(VIP_GROUP_ID, int(member_id))
            except Exception as e:
                logger.error(f"Error kicking member {member_id}: {e}")
            
            current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
            
            if member_id in AUTO_ROLE_CONFIG['role_history']:
                AUTO_ROLE_CONFIG['role_history'][member_id]['last_expired'] = current_time.isoformat()
            
            AUTO_ROLE_CONFIG['dm_schedule'][member_id] = {
                'role_expired': current_time.isoformat(),
                'dm_3_sent': False,
                'dm_7_sent': False,
                'dm_14_sent': False
            }
            
            del AUTO_ROLE_CONFIG['active_members'][member_id]
            
            try:
                expiry_msg = MESSAGE_TEMPLATES["Trial Status & Expiry"]["Trial Expired"]["message"]
                await self.app.send_message(int(member_id), expiry_msg)
            except Exception as e:
                logger.error(f"Could not send expiry DM to {member_id}: {e}")
            
            await self.bot.save_auto_role_config()
            logger.info(f"Trial expired for user {member_id}")
            
        except Exception as e:
            logger.error(f"Error expiring trial for {member_id}: {e}")
    
    async def run(self):
        """Background loop: monitors trial expiry times (runs every 1 minute)"""
        await asyncio.sleep(60)
        
        while getattr(self.bot, 'running', True):
            try:
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                expired_members = []
                
                for member_id, data in list(AUTO_ROLE_CONFIG['active_members'].items()):
                    expiry_time = datetime.fromisoformat(data['expiry_time'])
                    if expiry_time.tzinfo is None:
                        expiry_time = AMSTERDAM_TZ.localize(expiry_time)
                    
                    if current_time >= expiry_time:
                        expired_members.append(member_id)
                
                for member_id in expired_members:
                    await self.expire_trial(member_id)
                
            except Exception as e:
                await self.bot.log_to_debug(f"Error in trial expiry loop: {e}", is_error=True)
            
            await asyncio.sleep(60)


def create_trial_expiry_loop(app, db_pool, bot_instance):
    """Factory function to create and return the trial expiry loop"""
    loop = TrialExpiryLoop(app, db_pool, bot_instance)
    return loop.run()
