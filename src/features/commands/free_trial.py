from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from datetime import datetime, timedelta
import pytz
import logging
from src.features.core.config import AMSTERDAM_TZ, AUTO_ROLE_CONFIG, MESSAGE_TEMPLATES

logger = logging.getLogger(__name__)

async def handle_timed_auto_role(client: Client, message: Message, is_owner_func):
    if not await is_owner_func(message.from_user.id):
        return

    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📊 View Stats", callback_data="tar_status")],
         [InlineKeyboardButton("📋 List Active Users", callback_data="tar_list")],
         [InlineKeyboardButton("⏱️ Edit Time Duration", callback_data="tar_retract")],
         [InlineKeyboardButton("🗑️ Clear Trial History", callback_data="tar_clear")],
         [InlineKeyboardButton("Cancel", callback_data="tar_cancel")]])

    await message.reply("**Free Trial Management**\n\n"
                        "Select an option:",
                        reply_markup=keyboard)

async def handle_timedautorole_callback(client: Client, callback_query: CallbackQuery, is_owner_func):
    user_id = callback_query.from_user.id

    if not await is_owner_func(user_id):
        await callback_query.answer("This is restricted to the bot owner.", show_alert=True)
        return

    data = callback_query.data

    if data == "tar_cancel":
        await callback_query.message.edit_text("Menu closed.")
        await callback_query.answer()
        return

    if data == "tar_status":
        active_count = len(AUTO_ROLE_CONFIG['active_members'])
        history_count = len(AUTO_ROLE_CONFIG['role_history'])

        status = (
            f"**Trial System Status**\n\n"
            f"**Status:** {'Enabled' if AUTO_ROLE_CONFIG['enabled'] else 'Disabled'}\n"
            f"**Duration:** Exactly 3 trading days (Mon-Fri only)\n"
            f"**Expiry Time:** 22:59 on the 3rd trading day\n\n"
            f"**Active Trials:** {active_count}\n"
            f"**Anti-abuse Records:** {history_count}\n")
        await callback_query.message.edit_text(status)

    elif data == "tar_list":
        if not AUTO_ROLE_CONFIG['active_members']:
            await callback_query.message.edit_text("No active trial members.")
            await callback_query.answer()
            return

        response = "**Active Trial Members**\n\n"
        current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)

        members_with_time = []
        for member_id, data_item in AUTO_ROLE_CONFIG['active_members'].items():
            expiry = datetime.fromisoformat(data_item.get('expiry_time', current_time.isoformat()))
            if expiry.tzinfo is None:
                expiry = AMSTERDAM_TZ.localize(expiry)

            time_left = expiry - current_time
            total_seconds = max(0, time_left.total_seconds())
            members_with_time.append((member_id, data_item, total_seconds))

        members_with_time.sort(key=lambda x: x[2])

        for member_id, data_item, total_seconds in members_with_time[:20]:
            hours = int(total_seconds // 3600)
            minutes = int((total_seconds % 3600) // 60)
            weekend = " (weekend)" if data_item.get('weekend_delayed') else ""
            
            # Fetch user info to show name
            name_display = ""
            try:
                user = await client.get_users(member_id)
                first_name = user.first_name or ""
                last_name = user.last_name or ""
                name_display = f" ({first_name} {last_name})".strip()
            except Exception:
                pass
                
            response += f"User {member_id}{name_display}: {hours}h {minutes}m left{weekend}\n"

        if len(AUTO_ROLE_CONFIG['active_members']) > 20:
            response += f"\n_...and {len(AUTO_ROLE_CONFIG['active_members']) - 20} more_"

        await callback_query.message.edit_text(response)
    
    await callback_query.answer()
