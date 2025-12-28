from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from datetime import datetime
import pytz
import logging
from src.features.core.config import AMSTERDAM_TZ

logger = logging.getLogger(__name__)

async def handle_peer_id_status(client: Client, message: Message, is_owner_func, bot_instance):
    """Check if a user's peer ID has been established - widget style"""
    if not await is_owner_func(message.from_user.id):
        return
    
    args = message.text.split()
    
    # If user ID provided as argument, show status directly
    if len(args) >= 2:
        try:
            user_id = int(args[1])
            await _show_peer_id_status(message, user_id, bot_instance.db_pool)
        except ValueError:
            await message.reply("❌ Invalid user ID. Please provide a valid number.")
        return
    
    # Otherwise show widget for user to input ID
    text = "**🔍 Peer ID Status Checker**\n\nSend me a user ID to check their peer establishment status."
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancel", callback_data="pid_cancel")]
    ])
    
    await message.reply(text, reply_markup=keyboard)
    
    # Set up context for text input
    if not hasattr(bot_instance, '_waiting_for_peer_id'):
        bot_instance._waiting_for_peer_id = {}
    bot_instance._waiting_for_peer_id[message.from_user.id] = True

async def _show_peer_id_status(message, user_id: int, db_pool):
    """Display peer ID status for a user"""
    if not db_pool:
        await message.reply("❌ Database not available")
        return
    
    try:
        async with db_pool.acquire() as conn:
            peer_check = await conn.fetchrow(
                'SELECT * FROM peer_id_checks WHERE user_id = $1',
                user_id
            )
        
        if not peer_check:
            await message.reply(f"❌ No peer ID check found for user {user_id}")
            return
        
        is_established = peer_check['peer_id_established']
        established_at = peer_check['established_at']
        joined_at = peer_check['joined_at']
        current_delay = peer_check['current_delay_minutes']
        welcome_sent = peer_check['welcome_dm_sent']
        
        status_text = f"**Peer ID Status for User {user_id}**\n\n"
        
        if is_established:
            status_text += f"✅ **Peer ID: ESTABLISHED**\n"
            if established_at:
                status_text += f"📅 Established at: {established_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
            status_text += f"💬 Welcome DM sent: {'Yes ✓' if welcome_sent else 'No ✗'}\n"
        else:
            status_text += f"⏳ **Peer ID: NOT YET ESTABLISHED**\n"
            status_text += f"📅 Joined at: {joined_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
            status_text += f"⏱️ Current retry delay: {current_delay} minutes\n"
            status_text += f"💬 Welcome DM sent: {'Yes ✓' if welcome_sent else 'No ✗'}\n"
            
            # Calculate time elapsed
            current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
            if joined_at.tzinfo is None:
                joined_at = AMSTERDAM_TZ.localize(joined_at)
            elapsed = (current_time - joined_at).total_seconds() / 3600
            status_text += f"⏳ Time elapsed: {elapsed:.1f} hours\n"
        
        await message.reply(status_text)
        
    except Exception as e:
        await message.reply(f"❌ Error checking peer ID status: {str(e)}")

async def handle_peerid_callback(client: Client, callback_query: CallbackQuery, is_owner_func, bot_instance):
    """Handle peer ID callback (cancel button)"""
    if not await is_owner_func(callback_query.from_user.id):
        await callback_query.answer("Restricted to bot owner.", show_alert=True)
        return
    
    if callback_query.data == "pid_cancel":
        if not hasattr(bot_instance, '_waiting_for_peer_id'):
            bot_instance._waiting_for_peer_id = {}
        
        user_id = callback_query.from_user.id
        bot_instance._waiting_for_peer_id.pop(user_id, None)
        
        await callback_query.message.edit_text("❌ Peer ID check cancelled.")
        await callback_query.answer()
