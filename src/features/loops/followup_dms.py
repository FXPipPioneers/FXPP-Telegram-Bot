import asyncio
import logging
from datetime import datetime, timedelta
import pytz
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import UserNotParticipant
from src.features.core.config import AMSTERDAM_TZ, AUTO_ROLE_CONFIG, MESSAGE_TEMPLATES, VIP_GROUP_ID

logger = logging.getLogger(__name__)

class FollowupDMLoop:
    """Background loop: sends 3/7/14-day follow-up DMs after trial expires"""
    
    def __init__(self, app, bot_instance):
        self.app = app
        self.bot = bot_instance
    
    async def check_vip_membership(self, user_id: int) -> bool:
        """Check if user is currently in VIP group"""
        try:
            member = await self.app.get_chat_member(VIP_GROUP_ID, user_id)
            return member.status in [
                ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.OWNER
            ]
        except UserNotParticipant:
            return False
        except Exception:
            return False
    
    async def send_followup_dm(self, member_id: str, days: int):
        """Send follow-up DM after trial expiry"""
        try:
            if days == 3:
                message = MESSAGE_TEMPLATES["3/7/14 Day Follow-ups"]["3 Days After Trial Ends"]["message"]
            elif days == 7:
                message = MESSAGE_TEMPLATES["3/7/14 Day Follow-ups"]["7 Days After Trial Ends"]["message"]
            elif days == 14:
                message = MESSAGE_TEMPLATES["3/7/14 Day Follow-ups"]["14 Days After Trial Ends"]["message"]
            else:
                return
            
            await self.app.send_message(int(member_id), message)
            logger.info(f"✅ Sent {days}-day follow-up DM to user {member_id}")
            await self.bot.log_to_debug(f"✅ Sent {days}-day follow-up DM to user {member_id}", user_id=int(member_id))
        except Exception as e:
            error_msg = f"❌ Could not send {days}-day follow-up DM to {member_id}: {e}"
            logger.error(error_msg)
            if days == 3:
                msg = MESSAGE_TEMPLATES["3/7/14 Day Follow-ups"]["3 Days After Trial Ends"]["message"]
            elif days == 7:
                msg = MESSAGE_TEMPLATES["3/7/14 Day Follow-ups"]["7 Days After Trial Ends"]["message"]
            elif days == 14:
                msg = MESSAGE_TEMPLATES["3/7/14 Day Follow-ups"]["14 Days After Trial Ends"]["message"]
            else:
                msg = ""
            await self.bot.log_to_debug(error_msg, is_error=True, user_id=int(member_id), failed_message=msg)
    
    async def run(self):
        """Background loop: sends 3/7/14-day follow-up DMs (runs every 1 minute)"""
        await asyncio.sleep(300)  # Wait 5 minutes before starting
        
        while getattr(self.bot, 'running', True):
            try:
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                
                for member_id, data in list(AUTO_ROLE_CONFIG['dm_schedule'].items()):
                    role_expired = datetime.fromisoformat(data['role_expired'])
                    if role_expired.tzinfo is None:
                        role_expired = AMSTERDAM_TZ.localize(role_expired)
                    
                    try:
                        is_vip = await self.check_vip_membership(int(member_id))
                        if is_vip:
                            continue
                    except Exception:
                        pass
                    
                    dm_3_time = role_expired + timedelta(days=3)
                    dm_7_time = role_expired + timedelta(days=7)
                    dm_14_time = role_expired + timedelta(days=14)
                    
                    if not data['dm_3_sent'] and current_time >= dm_3_time:
                        await self.send_followup_dm(member_id, 3)
                        AUTO_ROLE_CONFIG['dm_schedule'][member_id]['dm_3_sent'] = True
                    
                    if not data['dm_7_sent'] and current_time >= dm_7_time:
                        await self.send_followup_dm(member_id, 7)
                        AUTO_ROLE_CONFIG['dm_schedule'][member_id]['dm_7_sent'] = True
                    
                    if not data['dm_14_sent'] and current_time >= dm_14_time:
                        await self.send_followup_dm(member_id, 14)
                        AUTO_ROLE_CONFIG['dm_schedule'][member_id]['dm_14_sent'] = True
                
            except Exception as e:
                await self.bot.log_to_debug(f"Error in followup DM loop: {e}", is_error=True)
            
            await asyncio.sleep(3600)  # Check every hour


def create_followup_dm_loop(app, bot_instance):
    """Factory function to create and return the followup DM loop"""
    loop = FollowupDMLoop(app, bot_instance)
    return loop.run()
