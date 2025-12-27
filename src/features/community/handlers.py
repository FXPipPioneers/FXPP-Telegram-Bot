from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import ChatJoinRequest, ChatMemberUpdated, Message, CallbackQuery
from pyrogram.enums import ChatMemberStatus
from src.features.core.config import (
    VIP_GROUP_ID, FREE_GROUP_ID, DEBUG_GROUP_ID, BOT_OWNER_USER_ID,
    MESSAGE_TEMPLATES, PAIR_CONFIG, EXCLUDED_FROM_TRACKING, AUTO_ROLE_CONFIG,
    PRICE_TRACKING_CONFIG, AMSTERDAM_TZ
)
import logging
from datetime import datetime, timedelta
import pytz
import re
import asyncio
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class CommunityHandlers:
    """Event handlers for group join requests, member updates, group messages, and reactions"""
    
    def __init__(self, app: Client, db_pool, bot_instance):
        self.app = app
        self.db_pool = db_pool
        self.bot = bot_instance
        self.trial_pending_approvals = set()
    
    async def is_owner(self, user_id: int) -> bool:
        """Check if user is the bot owner"""
        if BOT_OWNER_USER_ID == 0:
            return False
        return user_id == BOT_OWNER_USER_ID
    
    async def log_to_debug(self, message: str, is_error: bool = False, user_id: int = None):
        """Log message to debug group"""
        if hasattr(self.bot, 'log_to_debug'):
            await self.bot.log_to_debug(message, is_error, user_id)
        elif DEBUG_GROUP_ID:
            try:
                if is_error:
                    msg_text = f"🚨 **SYSTEM ERROR**\n\n**Issue:** {message}\n\n@fx_pippioneers"
                else:
                    msg_text = f"📊 **SYSTEM LOG**\n\n**Event:** {message}"
                
                if user_id:
                    msg_text += f"\n\n👤 **User ID:** `{user_id}`"
                
                await self.app.send_message(DEBUG_GROUP_ID, msg_text)
            except Exception as e:
                logger.error(f"Failed to send debug log: {e}")
    
    # ========== JOIN REQUEST HANDLER ==========
    async def process_join_request(self, client: Client, join_request: ChatJoinRequest):
        """Handle join requests to VIP group - auto-approve trial users"""
        if VIP_GROUP_ID == 0 or join_request.chat.id != VIP_GROUP_ID:
            return
        
        try:
            if not join_request.from_user:
                return
            user_id = join_request.from_user.id
            user_id_str = str(user_id)
            user_name = join_request.from_user.first_name or user_id_str
            
            has_used_trial = user_id_str in AUTO_ROLE_CONFIG['role_history']
            
            if not has_used_trial and self.db_pool:
                try:
                    async with self.db_pool.acquire() as conn:
                        db_history = await conn.fetchrow("SELECT * FROM role_history WHERE member_id = $1", user_id)
                        if db_history: has_used_trial = True
                except Exception as e:
                    logger.error(f"Error checking role_history: {e}")
            
            if has_used_trial:
                await self.log_to_debug(f"❌ Rejected join request from {user_name} (ID: {user_id}) - trial already used before")
                try:
                    await join_request.decline()
                except Exception as e:
                    logger.error(f"Error declining join request: {e}")
                try:
                    rejection_dm = MESSAGE_TEMPLATES["Trial Status & Expiry"]["Trial Rejected (Used Before)"]["message"].replace("{user_name}", user_name)
                    await client.send_message(user_id, rejection_dm, disable_web_page_preview=True)
                except Exception as e:
                    logger.error(f"Could not send rejection DM: {e}")
                return
            
            self.trial_pending_approvals.add(user_id)
            try:
                await join_request.approve()
            except Exception as e:
                # If approve fails (e.g. user already joined or link invalid), still proceed to log
                logger.warning(f"Join request approval failed for {user_id}: {e}")
            await self.log_to_debug(f"🎯 Auto-approved trial join request from {user_name} (ID: {user_id})")
        except Exception as e:
            logger.error(f"❌ Error processing join request: {e}")
    
    # ========== MEMBER UPDATE HANDLERS ==========
    async def process_member_update(self, client: Client, member_update: ChatMemberUpdated):
        """Handle new member joins to groups"""
        if not member_update.new_chat_member: return
        old_status = member_update.old_chat_member.status if member_update.old_chat_member else None
        new_status = member_update.new_chat_member.status
        is_new_join = (old_status in [None, ChatMemberStatus.LEFT, ChatMemberStatus.BANNED] and new_status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER])
        if not is_new_join: return
        user = member_update.new_chat_member.user
        if member_update.chat.id == FREE_GROUP_ID: await self.handle_free_group_join(client, user)
        elif member_update.chat.id == VIP_GROUP_ID: await self.handle_vip_group_join(client, user, member_update.invite_link)
    
    async def handle_free_group_join(self, client: Client, user):
        """Track free group join for engagement tracking and peer ID verification"""
        if self.db_pool:
            try:
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                async with self.db_pool.acquire() as conn:
                    await conn.execute('''INSERT INTO free_group_joins (user_id, joined_at, discount_sent) VALUES ($1, $2, FALSE) ON CONFLICT (user_id) DO NOTHING''', user.id, current_time)
                    next_check = current_time + timedelta(minutes=3)
                    await conn.execute('''INSERT INTO peer_id_checks (user_id, joined_at, next_check_at) VALUES ($1, $2, $3) ON CONFLICT (user_id) DO NOTHING''', user.id, current_time, next_check)
                await self.log_to_debug(f"👤 New member joined FREE group: {user.first_name} (ID: {user.id})")
            except Exception as e:
                logger.error(f"Error tracking free group join for {user.id}: {e}")
    
    async def handle_vip_group_join(self, client: Client, user, invite_link):
        """Handle VIP group joins - register trial users"""
        user_id_str = str(user.id)
        current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
        
        # Check if user is in pending approvals or if we should auto-start trial
        is_trial_user = user.id in self.trial_pending_approvals
        self.trial_pending_approvals.discard(user.id)
        
        if not is_trial_user:
            # If not in pending, check if they've used trial before
            has_used_trial = user_id_str in AUTO_ROLE_CONFIG['role_history']
            if not has_used_trial and self.db_pool:
                try:
                    async with self.db_pool.acquire() as conn:
                        db_history = await conn.fetchrow("SELECT * FROM role_history WHERE member_id = $1", user.id)
                        if db_history: has_used_trial = True
                except Exception as e:
                    logger.error(f"Error checking role_history in VIP join: {e}")
            
            if has_used_trial:
                return # Don't grant trial if already used
            is_trial_user = True # Grant trial if never used before
            
        if not is_trial_user: return
        
        from src.features.community.trial_manager import TrialManager
        trial_manager = TrialManager(self.db_pool, self.bot)
        expiry_time, is_weekend = await trial_manager.register_trial(user, current_time)
        await self.bot.save_auto_role_config()
        
        try:
            from src.features.community.dm_manager import DMManager
            dm_manager = DMManager(self.app, self.db_pool, self.bot)
            welcome_msg = MESSAGE_TEMPLATES["Welcome & Onboarding"]["Welcome DM (Trial Activated)"]["message"]
            await dm_manager.send_dm(user.id, welcome_msg)
        except Exception as e:
            logger.error(f"Could not send welcome DM: {e}")
        await self.log_to_debug(f"✅ Trial activated for {user.first_name} (expires {expiry_time.strftime('%Y-%m-%d %H:%M')})")
    
    # ========== GROUP MESSAGE HANDLERS ==========
    async def handle_text_input(self, client: Client, message: Message):
        if not message.from_user:
            return
        if not await self.is_owner(message.from_user.id):
            return
        await self.bot.handle_text_input(client, message)
    
    async def delete_service_messages(self, client: Client, message: Message):
        if message.chat.id in [VIP_GROUP_ID, FREE_GROUP_ID]:
            try: await message.delete()
            except: pass
    
    async def handle_group_reaction_update(self, client: Client, message: Message):
        if message.chat.id == FREE_GROUP_ID and message.reactions and self.db_pool:
            asyncio.create_task(self._fetch_and_store_reactions(message))
    
    async def _fetch_and_store_reactions(self, message: Message):
        try:
            current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
            async with self.db_pool.acquire() as conn:
                for reaction in message.reactions:
                    emoji_str = str(reaction.emoji) if hasattr(reaction, 'emoji') else str(reaction)
                    pass
        except Exception as e:
            logger.debug(f"Error storing reactions: {e}")

def register_community_handlers(client: Client, db_pool, bot_instance=None):
    """Factory function to register community handlers"""
    handlers = CommunityHandlers(client, db_pool, bot_instance or client)
    
    @client.on_chat_join_request(filters.group)
    async def _join_req(c, jr):
        await handlers.process_join_request(c, jr)
    
    @client.on_chat_member_updated(filters.group)
    async def _member_upd(c, mu):
        await handlers.process_member_update(c, mu)
    
    @client.on_message(filters.group & ~filters.service)
    async def _group_msg(c, m):
        # Manual signal handling moved to trading/handlers.py
        pass
    
    @client.on_message(filters.service)
    async def _service_msg(c, m):
        await handlers.delete_service_messages(c, m)
    
    return handlers
