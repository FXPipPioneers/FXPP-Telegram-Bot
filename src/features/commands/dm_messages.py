from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import logging
from src.features.core.config import MESSAGE_TEMPLATES

logger = logging.getLogger(__name__)

async def handle_dmmessages(client: Client, message: Message, is_owner_func):
    """Show DM message topics menu"""
    if not await is_owner_func(message.from_user.id):
        return

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Free Trial Heads Up", callback_data="dmm_topic_ft")],
        [InlineKeyboardButton("📅 3/7/14 Day Follow-ups", callback_data="dmm_topic_fu")],
        [InlineKeyboardButton("👋 Welcome & Onboarding", callback_data="dmm_topic_wo")],
        [InlineKeyboardButton("🎁 Engagement & Offers", callback_data="dmm_topic_eo")],
        [InlineKeyboardButton("❌ Close", callback_data="dmm_close")]
    ])

    await message.reply(
        "**DM Message Templates**\n\n"
        "Select a topic to preview the DM messages:\n\n"
        "📢 **Free Trial Heads Up** - Trial expiration warnings (24hr, 3hr)\n"
        "📅 **3/7/14 Day Follow-ups** - Post-trial follow-up messages\n"
        "👋 **Welcome & Onboarding** - Welcome and activation messages\n"
        "🎁 **Engagement & Offers** - Discount and trial offer messages",
        reply_markup=keyboard
    )

async def handle_dmmessages_callback(client: Client, callback_query: CallbackQuery, is_owner_func):
    if not await is_owner_func(callback_query.from_user.id):
        await callback_query.answer("Restricted to bot owner.", show_alert=True)
        return

    data = callback_query.data
    if data == "dmm_close":
        await callback_query.message.edit_text("❌ Closed.")
        await callback_query.answer()
        return

    topic_map = {
        "dmm_topic_ft": "Free Trial Heads Up",
        "dmm_topic_fu": "3/7/14 Day Follow-ups",
        "dmm_topic_wo": "Welcome & Onboarding",
        "dmm_topic_eo": "Engagement & Offers"
    }

    if data in topic_map:
        topic_name = topic_map[data]
        topic = MESSAGE_TEMPLATES[topic_name]
        buttons = []
        for msg_title, msg_data in topic.items():
            buttons.append([InlineKeyboardButton(f"📝 {msg_title}", callback_data=f"dmm_msg_{msg_data['id']}")])
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="dmm_back")])
        await callback_query.message.edit_text(f"**{topic_name}**\n\nSelect a message to preview:", reply_markup=InlineKeyboardMarkup(buttons))
        await callback_query.answer()
        return

    if data == "dmm_back":
        await handle_dmmessages(client, callback_query.message, is_owner_func)
        await callback_query.answer()
        return

    if str(data).startswith("dmm_msg_"):
        msg_id = str(data).replace("dmm_msg_", "")
        for topic in MESSAGE_TEMPLATES.values():
            for msg_title, msg_data in topic.items():
                if str(msg_data["id"]) == msg_id:
                    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="dmm_back")]]
                    await callback_query.message.edit_text(f"**Preview: {msg_title}**\n\n{msg_data['message']}", reply_markup=InlineKeyboardMarkup(buttons))
                    await callback_query.answer()
                    logger.info(f"Owner previewed DM message: {msg_title}")
                    return
    await callback_query.answer()
