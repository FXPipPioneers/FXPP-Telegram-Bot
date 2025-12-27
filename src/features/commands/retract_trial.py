from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import time
import logging
from src.features.core.config import AUTO_ROLE_CONFIG, AMSTERDAM_TZ
from datetime import datetime, timedelta
import pytz

logger = logging.getLogger(__name__)

async def handle_retract_trial(client: Client, message: Message, is_owner_func, bot_instance):
    if not await is_owner_func(message.from_user.id):
        await message.reply("❌ This command can only be used by the bot owner.")
        return

    if not AUTO_ROLE_CONFIG['active_members']:
        await message.reply("No active trial members found.")
        return

    if not hasattr(bot_instance, 'retracttrial_mappings'):
        bot_instance.retracttrial_mappings = {}

    menu_id = str(message.id)[-8:]
    user_mapping = {}
    buttons = []

    for idx, (user_id_str, member_data) in enumerate(list(AUTO_ROLE_CONFIG['active_members'].items())[:20]):
        user_mapping[str(idx)] = user_id_str
        expiry = datetime.fromisoformat(member_data.get('expiry_time', ''))
        if expiry.tzinfo is None:
            expiry = AMSTERDAM_TZ.localize(expiry)

        total_seconds = max(0, (expiry - datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)).total_seconds())
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)

        buttons.append([
            InlineKeyboardButton(
                f"User {user_id_str} ({hours}h {minutes}m left)",
                callback_data=f"retrt_{menu_id}_select_{idx}")
        ])

    bot_instance.retracttrial_mappings[menu_id] = user_mapping
    buttons.append([InlineKeyboardButton("Cancel", callback_data=f"retrt_{menu_id}_cancel")])

    keyboard = InlineKeyboardMarkup(buttons)
    await message.reply("**Retract Trial Time**\n\nSelect a user to adjust their trial expiry time:", reply_markup=keyboard)

async def handle_retracttrial_callback(client: Client, callback_query: CallbackQuery, is_owner_func, bot_instance):
    user_id = callback_query.from_user.id
    if not await is_owner_func(user_id):
        await callback_query.answer("This is restricted to the bot owner.", show_alert=True)
        return

    data = str(callback_query.data)
    if not data.startswith("retrt_"):
        return

    parts = data.split("_")
    menu_id = parts[1]
    action_type = parts[2]

    if not hasattr(bot_instance, 'retracttrial_mappings'):
        bot_instance.retracttrial_mappings = {}

    if action_type == "cancel":
        bot_instance.retracttrial_mappings.pop(menu_id, None)
        await callback_query.message.edit_text("Cancelled.")
        await callback_query.answer()
        return

    if action_type == "back":
        await handle_retract_trial(client, callback_query.message, is_owner_func, bot_instance)
        await callback_query.answer()
        return

    if action_type == "reduce":
        idx = parts[3]
        hours_to_reduce = int(parts[4].replace("h", ""))
        user_mapping = bot_instance.retracttrial_mappings.get(menu_id, {})
        selected_user_id_str = user_mapping.get(idx)
        
        if selected_user_id_str in AUTO_ROLE_CONFIG['active_members']:
            member_data = AUTO_ROLE_CONFIG['active_members'][selected_user_id_str]
            expiry_str = member_data.get('expiry_time', '')
            if not expiry_str:
                await callback_query.answer("Expiry time not found.", show_alert=True)
                return
                
            expiry = datetime.fromisoformat(expiry_str)
            if expiry.tzinfo is None: expiry = AMSTERDAM_TZ.localize(expiry)
            
            new_expiry = expiry - timedelta(hours=hours_to_reduce)
            member_data['expiry_time'] = new_expiry.isoformat()
            
            if bot_instance.db_pool:
                try:
                    async with bot_instance.db_pool.acquire() as conn:
                        await conn.execute("UPDATE active_members SET expiry_time = $1 WHERE member_id = $2", 
                                         new_expiry, int(selected_user_id_str))
                except Exception as e:
                    logger.error(f"DB update error: {e}")
            
            await callback_query.message.edit_text(f"✅ Trial for User {selected_user_id_str} reduced by {hours_to_reduce}h.")
        await callback_query.answer()
        return

    if action_type == "select":
        idx = parts[3] if len(parts) > 3 else None
        user_mapping = bot_instance.retracttrial_mappings.get(menu_id, {})
        selected_user_id_str = user_mapping.get(idx)

        if not selected_user_id_str:
            await callback_query.answer("User not found.", show_alert=True)
            return

        buttons = []
        for hours in [1, 2, 3, 6, 12, 24]:
            buttons.append([InlineKeyboardButton(f"Reduce {hours}h", callback_data=f"retrt_{menu_id}_reduce_{idx}_h{hours}")])

        buttons.append([InlineKeyboardButton("Back", callback_data=f"retrt_{menu_id}_back")])
        buttons.append([InlineKeyboardButton("Cancel", callback_data=f"retrt_{menu_id}_cancel")])

        await callback_query.message.edit_text(
            f"**Reducing trial for User {selected_user_id_str}**\n\nSelect hours to subtract:",
            reply_markup=InlineKeyboardMarkup(buttons))
        await callback_query.answer()
        return

    await callback_query.answer()
