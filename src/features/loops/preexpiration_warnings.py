import asyncio
import logging
from datetime import datetime, timedelta, timezone
import pytz
from src.features.core.config import AMSTERDAM_TZ, AUTO_ROLE_CONFIG, MESSAGE_TEMPLATES

logger = logging.getLogger(__name__)

class PreexpirationWarningLoop:
    """Background loop: sends 24-hour and 3-hour warnings before trial expires"""
    
    def __init__(self, app, bot_instance):
        self.app = app
        self.bot = bot_instance
    
    async def send_24hr_warning(self, member_id: str):
        """Send 24-hour pre-expiration warning"""
        try:
            message = MESSAGE_TEMPLATES["Free Trial Heads Up"]["24-Hour Warning"]["message"]
            await self.app.send_message(int(member_id), message)
            logger.info(f"✅ Sent 24-hour trial warning DM to user {member_id}")
            await self.bot.log_to_debug(f"✅ Sent 24-hour trial warning DM to user {member_id}", user_id=int(member_id))
        except Exception as e:
            error_msg = f"❌ Could not send 24-hour warning DM to {member_id}: {e}"
            logger.error(error_msg)
            message = MESSAGE_TEMPLATES["Free Trial Heads Up"]["24-Hour Warning"]["message"]
            await self.bot.log_to_debug(error_msg, is_error=True, user_id=int(member_id), failed_message=message)
            if str(member_id) in AUTO_ROLE_CONFIG['active_members']:
                AUTO_ROLE_CONFIG['active_members'][str(member_id)]['warning_24h_sent'] = True
                await self.bot.save_auto_role_config()
    
    async def send_3hr_warning(self, member_id: str):
        """Send 3-hour pre-expiration warning"""
        try:
            message = MESSAGE_TEMPLATES["Free Trial Heads Up"]["3-Hour Warning"]["message"]
            await self.app.send_message(int(member_id), message)
            logger.info(f"✅ Sent 3-hour trial warning DM to user {member_id}")
            await self.bot.log_to_debug(f"✅ Sent 3-hour trial warning DM to user {member_id}", user_id=int(member_id))
        except Exception as e:
            error_msg = f"❌ Could not send 3-hour warning DM to {member_id}: {e}"
            logger.error(error_msg)
            message = MESSAGE_TEMPLATES["Free Trial Heads Up"]["3-Hour Warning"]["message"]
            await self.bot.log_to_debug(error_msg, is_error=True, user_id=int(member_id), failed_message=message)
            if str(member_id) in AUTO_ROLE_CONFIG['active_members']:
                AUTO_ROLE_CONFIG['active_members'][str(member_id)]['warning_3h_sent'] = True
                await self.bot.save_auto_role_config()
    
    async def run(self):
        """Background loop: sends 24-hour and 3-hour warnings (runs every 5 minutes)"""
        await asyncio.sleep(60)
        
        while getattr(self.bot, 'running', True):
            try:
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                
                for member_id, data in list(AUTO_ROLE_CONFIG['active_members'].items()):
                    expiry_time = datetime.fromisoformat(data['expiry_time'])
                    if expiry_time.tzinfo is None:
                        expiry_time = AMSTERDAM_TZ.localize(expiry_time)
                    
                    time_until_expiry = expiry_time - current_time
                    hours_left = time_until_expiry.total_seconds() / 3600
                    
                    # Initialize warning tracking if not present
                    if 'warning_24h_sent' not in data:
                        data['warning_24h_sent'] = False
                    if 'warning_3h_sent' not in data:
                        data['warning_3h_sent'] = False
                    
                    # Send 24-hour warning
                    if not data['warning_24h_sent'] and 23 < hours_left <= 24:
                        await self.send_24hr_warning(member_id)
                        data['warning_24h_sent'] = True
                        await self.bot.save_auto_role_config()
                    
                    # Send 3-hour warning
                    if not data['warning_3h_sent'] and 2.9 < hours_left <= 3:
                        await self.send_3hr_warning(member_id)
                        data['warning_3h_sent'] = True
                        await self.bot.save_auto_role_config()
                
            except Exception as e:
                await self.bot.log_to_debug(f"Error in preexpiration warning loop: {e}", is_error=True)
            
            await asyncio.sleep(600)  # Check every 5 minutes


def create_preexpiration_warning_loop(app, bot_instance):
    """Factory function to create and return the preexpiration warning loop"""
    loop = PreexpirationWarningLoop(app, bot_instance)
    return loop.run()
