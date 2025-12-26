from pyrogram import filters, Client as PyClient
from pyrogram.types import ChatJoinRequest, ChatMemberUpdated
from pyrogram.enums import ChatMemberStatus
from src.core.config import VIP_GROUP_ID, FREE_GROUP_ID, AMSTERDAM_TZ, AUTO_ROLE_CONFIG, MESSAGE_TEMPLATES
from datetime import datetime, timedelta
import pytz
import logging

logger = logging.getLogger(__name__)

def register_community_handlers(app, db_manager):
    @app.on_chat_join_request(filters.chat(VIP_GROUP_ID))
    async def handle_join_request(client: Client, join_request: ChatJoinRequest):
        user_id = join_request.from_user.id
        user_name = join_request.from_user.first_name or str(user_id)
        
        try:
            history = await db_manager.get_role_history(user_id)
            if history:
                await join_request.decline()
                rejection_msg = MESSAGE_TEMPLATES["Trial Status & Expiry"]["Trial Rejected (Used Before)"]["message"].replace("{user_name}", user_name)
                try:
                    await client.send_message(user_id, rejection_msg)
                except Exception as e:
                    logger.error(f"Failed to send rejection DM to {user_id}: {e}")
                return

            await join_request.approve()
            logger.info(f"Approved trial for {user_name} ({user_id})")
        except Exception as e:
            logger.error(f"Join request error: {e}")

    @app.on_chat_member_updated()
    async def on_member_update(client: Client, member_update: ChatMemberUpdated):
        if not member_update.new_chat_member: return
        
        new_status = member_update.new_chat_member.status
        old_status = member_update.old_chat_member.status if member_update.old_chat_member else None
        
        if old_status in [None, ChatMemberStatus.LEFT] and new_status == ChatMemberStatus.MEMBER:
            user = member_update.new_chat_member.user
            if member_update.chat.id == FREE_GROUP_ID:
                # Trigger peer ID verification logic
                await db_manager.add_peer_id_check(user.id)
                logger.info(f"Member joined Free Group: {user.id}")
            elif member_update.chat.id == VIP_GROUP_ID:
                # Register trial expiry
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                expiry_time = current_time + timedelta(hours=72)
                # This would typically save to active_members table
                logger.info(f"Member joined VIP Group: {user.id}, expiry: {expiry_time}")
