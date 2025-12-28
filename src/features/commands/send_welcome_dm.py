import logging
import asyncio
import time
from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from src.features.core.config import MESSAGE_TEMPLATES

logger = logging.getLogger(__name__)

async def handle_sendwelcomedm(bot_instance, client: Client, message: Message):
    """Owner-only widget to manually send welcome DMs to users"""
    if not await bot_instance.is_owner(message.from_user.id):
        await message.reply("This command is restricted to the bot owner.")
        return

    # Store context for this menu session
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

async def handle_welcome_callback(bot_instance, client: Client, callback_query: CallbackQuery):
    """Handle welcome menu callbacks"""
    user_id = callback_query.from_user.id
    
    if not await bot_instance.is_owner(user_id):
        await callback_query.answer("This is restricted to the bot owner.", show_alert=True)
        return

    data = callback_query.data
    if not data.startswith("swdm_"):
        return

    parts = data.split("_")
    if len(parts) < 3:
        await callback_query.answer("Invalid callback data.", show_alert=True)
        return

    menu_id = parts[1]
    action = parts[2] if len(parts) > 2 else ""

    if not hasattr(bot_instance, 'sendwelcomedm_context'):
        bot_instance.sendwelcomedm_context = {}

    context = bot_instance.sendwelcomedm_context.get(menu_id, {})

    # Cancel button
    if action == "cancel":
        bot_instance.sendwelcomedm_context.pop(menu_id, None)
        await callback_query.message.edit_text("❌ Cancelled.")
        await callback_query.answer()
        return

    # Input button - ask for user ID
    if action == "input":
        context['stage'] = 'waiting_for_user_id'
        bot_instance.sendwelcomedm_context[menu_id] = context
        
        # Track that we're awaiting user ID input for this menu
        if not hasattr(bot_instance, 'awaiting_sendwelcomedm_input'):
            bot_instance.awaiting_sendwelcomedm_input = {}
        bot_instance.awaiting_sendwelcomedm_input[callback_query.from_user.id] = menu_id

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data=f"swdm_{menu_id}_cancel")]
        ])

        await callback_query.message.edit_text(
            "**Enter User ID**\n\n"
            "Reply to this message with the Telegram user ID (numeric only).",
            reply_markup=keyboard
        )
        await callback_query.answer()
        return

    await callback_query.answer()
