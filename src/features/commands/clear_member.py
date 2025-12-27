from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import time
import logging
from src.features.core.config import AUTO_ROLE_CONFIG, AMSTERDAM_TZ
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

async def handle_clear_member(client: Client, message: Message, is_owner_func, bot_instance):
    """Command to clear a member's trial history"""
    if not await is_owner_func(message.from_user.id):
        return

    if not hasattr(bot_instance, 'cleartrial_mappings'):
        bot_instance.cleartrial_mappings = {}

    menu_id = f"ct_{int(time.time() * 1000)}"
    user_mapping = {}
    buttons = []

    if AUTO_ROLE_CONFIG['active_members']:
        current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
        members_with_time = []
        for user_id_str, member_data in AUTO_ROLE_CONFIG['active_members'].items():
            expiry = datetime.fromisoformat(member_data.get('expiry_time', ''))
            if expiry.tzinfo is None:
                expiry = AMSTERDAM_TZ.localize(expiry)
            total_seconds = max(0, (expiry - current_time).total_seconds())
            members_with_time.append((user_id_str, member_data, total_seconds))
        
        members_with_time.sort(key=lambda x: x[2])
        for idx, (user_id_str, member_data, total_seconds) in enumerate(members_with_time[:20]):
            user_mapping[str(idx)] = user_id_str
            buttons.append([InlineKeyboardButton(f"User {user_id_str}", callback_data=f"clrtrl_{menu_id}_select_{idx}")])

    buttons.append([InlineKeyboardButton("📝 Enter Custom Member ID", callback_data=f"clrtrl_{menu_id}_custom")])
    buttons.append([InlineKeyboardButton("Cancel", callback_data=f"clrtrl_{menu_id}_cancel")])

    bot_instance.cleartrial_mappings[menu_id] = user_mapping
    status_text = "**Clear Trial History**\n\nSelect a user or enter a custom ID:"
    await message.reply(status_text, reply_markup=InlineKeyboardMarkup(buttons))

async def handle_cleartrial_callback(client: Client, callback_query: CallbackQuery, is_owner_func, bot_instance):
    user_id = callback_query.from_user.id
    if not await is_owner_func(user_id):
        await callback_query.answer("Restricted.", show_alert=True)
        return

    data = str(callback_query.data)
    parts = data.split("_")
    menu_id = parts[1]
    action_type = parts[2]

    if action_type == "cancel":
        bot_instance.cleartrial_mappings.pop(menu_id, None)
        await callback_query.message.edit_text("Cancelled.")
        await callback_query.answer()
        return

    if action_type == "custom":
        bot_instance.cleartrial_mappings[menu_id] = {'waiting_for_id': True}
        if not hasattr(bot_instance, 'awaiting_cleartrial_input'):
            bot_instance.awaiting_cleartrial_input = {}
        bot_instance.awaiting_cleartrial_input[callback_query.from_user.id] = menu_id
        await callback_query.message.edit_text("**Enter Member ID**\n\nPlease reply with the member ID.")
        await callback_query.answer()
        return

    if action_type == "select":
        idx = parts[3]
        user_mapping = bot_instance.cleartrial_mappings.get(menu_id, {})
        selected_user_id_str = user_mapping.get(idx)
        if selected_user_id_str:
            AUTO_ROLE_CONFIG['active_members'].pop(selected_user_id_str, None)
            AUTO_ROLE_CONFIG['role_history'].pop(selected_user_id_str, None)
            if bot_instance.db_pool:
                try:
                    async with bot_instance.db_pool.acquire() as conn:
                        await conn.execute("DELETE FROM active_members WHERE member_id = $1", int(selected_user_id_str))
                        await conn.execute("DELETE FROM role_history WHERE member_id = $1", int(selected_user_id_str))
                except Exception as e:
                    logger.error(f"Database error in clear member: {e}")
            await callback_query.message.edit_text(f"✅ User {selected_user_id_str} cleared.")
        await callback_query.answer()
        return
