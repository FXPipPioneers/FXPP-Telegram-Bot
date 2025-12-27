from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import time
import logging
import asyncio
from src.features.core.config import MESSAGE_TEMPLATES

logger = logging.getLogger(__name__)

async def handle_send_welcome_dm(client: Client, message: Message, is_owner_func, bot_instance):
    """Owner-only widget to manually send welcome DMs to users"""
    if not await is_owner_func(message.from_user.id):
        await message.reply("This command is restricted to the bot owner.")
        return

    menu_id = f"{int(time.time() * 1000)}"
    if not hasattr(bot_instance, 'sendwelcomedm_context'):
        bot_instance.sendwelcomedm_context = {}
    
    bot_instance.sendwelcomedm_context[menu_id] = {
        'stage': 'waiting_for_user_id',
        'user_id': None,
        'msg_type': None
    }

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Enter User ID", callback_data=f"swdm_{menu_id}_input")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"swdm_{menu_id}_cancel")]
    ])

    await message.reply(
        "**Send Welcome DM Widget**\n\n"
        "Click **Enter User ID** to start sending a welcome message to a user.",
        reply_markup=keyboard
    )

async def handle_sendwelcomedm_callback(client: Client, callback_query: CallbackQuery, is_owner_func, bot_instance):
    user_id = callback_query.from_user.id
    if not await is_owner_func(user_id):
        await callback_query.answer("This is restricted to the bot owner.", show_alert=True)
        return

    data = callback_query.data
    parts = data.split("_")
    menu_id = parts[1]
    action = parts[2] if len(parts) > 2 else ""

    if not hasattr(bot_instance, 'sendwelcomedm_context'):
        bot_instance.sendwelcomedm_context = {}
    context = bot_instance.sendwelcomedm_context.get(menu_id, {})

    if action == "cancel":
        bot_instance.sendwelcomedm_context.pop(menu_id, None)
        await callback_query.message.edit_text("❌ Cancelled.")
        await callback_query.answer()
        return

    if action == "input":
        context['stage'] = 'waiting_for_user_id'
        bot_instance.sendwelcomedm_context[menu_id] = context
        if not hasattr(bot_instance, 'awaiting_sendwelcomedm_input'):
            bot_instance.awaiting_sendwelcomedm_input = {}
        bot_instance.awaiting_sendwelcomedm_input[callback_query.from_user.id] = menu_id

        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"swdm_{menu_id}_cancel")]])
        await callback_query.message.edit_text(
            "**Enter User ID**\n\nReply to this message with the Telegram user ID (numeric only).",
            reply_markup=keyboard
        )
        await callback_query.answer()
        return
    await callback_query.answer()

async def execute_send_welcome_dm(bot_instance, callback_query: CallbackQuery, menu_id: str, context: dict, user_message: Message = None):
    user_id = context.get('user_id')
    if not user_id:
        if callback_query:
            await callback_query.message.edit_text("❌ Missing user ID.")
            await callback_query.answer()
        else:
            await user_message.reply("❌ Missing user ID.")
        bot_instance.sendwelcomedm_context.pop(menu_id, None)
        return

    welcome_msg = MESSAGE_TEMPLATES["Engagement & Offers"]["Welcome (Free Group)"]["message"]
    try:
        await bot_instance.app.get_users([user_id])
        await asyncio.sleep(1)
        await bot_instance.app.send_message(user_id, welcome_msg)
        
        if bot_instance.db_pool:
            try:
                async with bot_instance.db_pool.acquire() as conn:
                    await conn.execute('UPDATE peer_id_checks SET welcome_dm_sent = TRUE WHERE user_id = $1', user_id)
            except Exception as db_err:
                logger.warning(f"Could not update peer_id_checks for user {user_id}: {db_err}")
        
        msg = f"✅ **Success!**\n\nWelcome DM sent to user **{user_id}**"
        if callback_query:
            await callback_query.message.edit_text(msg)
        else:
            await user_message.reply(msg)
        await bot_instance.log_to_debug(f"Owner sent welcome DM to user {user_id} via /sendwelcomedm widget", user_id=user_id)
    except Exception as e:
        await bot_instance.track_failed_welcome_dm(user_id, f"User {user_id}", "welcome", welcome_msg)
        msg = f"⚠️ **Could Not Send Immediately**\n\nAdded to automatic retry queue..."
        if callback_query:
            await callback_query.message.edit_text(msg)
        else:
            await user_message.reply(msg)
    
    if callback_query:
        await callback_query.answer()
    bot_instance.sendwelcomedm_context.pop(menu_id, None)
