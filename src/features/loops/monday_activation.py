import asyncio
import logging
from datetime import datetime, timedelta, timezone
import pytz
from src.features.core.config import AMSTERDAM_TZ, AUTO_ROLE_CONFIG, MESSAGE_TEMPLATES

logger = logging.getLogger(__name__)

class MondayActivationLoop:
    """Background loop: activates weekend-delayed trials at Monday 00:01"""
    
    def __init__(self, app, bot_instance):
        self.app = app
        self.bot = bot_instance
    
    async def run(self):
        """Background loop: activates weekend-delayed trials (runs every 1 minute, triggers at Monday 00:01)"""
        await asyncio.sleep(60)
        
        while getattr(self.bot, 'running', True):
            try:
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                weekday = current_time.weekday()
                hour = current_time.hour
                
                if weekday == 0 and hour <= 1:  # Monday (0) and hour <= 1 (00:00 - 01:59)
                    for member_id, data in list(AUTO_ROLE_CONFIG['active_members'].items()):
                        try:
                            if data.get('weekend_delayed', False) and not data.get('monday_notification_sent', False):
                                try:
                                    activation_message = MESSAGE_TEMPLATES["Welcome & Onboarding"]["Monday Activation (Weekend Delay)"]["message"]
                                    await self.app.send_message(int(member_id), activation_message)
                                    logger.info(f"✅ Sent Monday activation DM to {member_id}")
                                    await self.bot.log_to_debug(f"✅ Sent Monday activation DM to {member_id}")
                                    
                                    AUTO_ROLE_CONFIG['active_members'][member_id]['monday_notification_sent'] = True
                                    await self.bot.save_auto_role_config()
                                except Exception as e:
                                    error_msg = f"❌ Could not send Monday activation DM to {member_id}: {e}"
                                    logger.error(error_msg)
                                    await self.bot.log_to_debug(error_msg, is_error=True)
                        except Exception as e:
                            logger.error(f"Error processing Monday activation for {member_id}: {e}")
            except Exception as e:
                logger.error(f"Error in monday activation loop: {e}")
            
            await asyncio.sleep(3600)  # Check every hour


def create_monday_activation_loop(app, bot_instance):
    """Factory function to create and return the monday activation loop"""
    loop = MondayActivationLoop(app, bot_instance)
    return loop.run()
