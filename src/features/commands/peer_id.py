from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from datetime import datetime
import pytz
import logging
from src.features.core.config import AMSTERDAM_TZ

logger = logging.getLogger(__name__)

async def handle_peer_id_status(client: Client, message: Message, is_owner_func, bot_instance):
    if not await is_owner_func(message.from_user.id):
        return
    
    args = message.text.split()
    if len(args) >= 2:
        try:
            user_id = int(args[1])
            await _show_peer_id_status(message, user_id, bot_instance.db_pool)
        except ValueError:
            await message.reply("❌ Invalid user ID.")
        return
    
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="pid_cancel")]])
    await message.reply("**🔍 Peer ID Status Checker**\n\nSend me a user ID to check.", reply_markup=keyboard)
    if not hasattr(bot_instance, '_waiting_for_peer_id'):
        bot_instance._waiting_for_peer_id = {}
    bot_instance._waiting_for_peer_id[message.from_user.id] = True

async def _show_peer_id_status(message, user_id, db_pool):
    if not db_pool:
        await message.reply("❌ Database not available")
        return
    
    try:
        async with db_pool.acquire() as conn:
            peer_check = await conn.fetchrow('SELECT * FROM peer_id_checks WHERE user_id = $1', user_id)
        
        if not peer_check:
            await message.reply(f"❌ No peer ID check found for user {user_id}")
            return
        
        is_est = peer_check['peer_id_established']
        status = f"**Peer ID Status for User {user_id}**\n\n"
        status += f"{'✅' if is_est else '⏳'} **Status: {'ESTABLISHED' if is_est else 'NOT ESTABLISHED'}**\n"
        status += f"💬 Welcome DM sent: {'Yes ✓' if peer_check['welcome_dm_sent'] else 'No ✗'}\n"
        await message.reply(status)
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

async def handle_peerid_callback(client: Client, callback_query: CallbackQuery, is_owner_func, bot_instance):
    if not await is_owner_func(callback_query.from_user.id):
        await callback_query.answer("Restricted.", show_alert=True)
        return
    
    if callback_query.data == "pid_cancel":
        if hasattr(bot_instance, '_waiting_for_peer_id'):
            bot_instance._waiting_for_peer_id.pop(callback_query.from_user.id, None)
        await callback_query.message.edit_text("❌ Cancelled.")
        await callback_query.answer()
