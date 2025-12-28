import logging
from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from src.features.core.config import AMSTERDAM_TZ
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

async def handle_dmstatus(bot_instance, client: Client, message: Message):
    if not await bot_instance.is_owner(message.from_user.id):
        return
    await show_dm_status(bot_instance, message)

async def show_dm_status(bot_instance, message: Message, is_edit=False):
    """Display DM delivery statistics and queue status"""
    db = bot_instance.db
    
    if not db.pool:
        status_text = "❌ **Database Offline**\n\nCannot retrieve DM statistics."
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data="dm_refresh")]])
    else:
        try:
            async with db.pool.acquire() as conn:
                # 1. Welcome DM Stats
                pending_welcome = await conn.fetchval("SELECT COUNT(*) FROM pending_welcome_dms WHERE failed_attempts < 5")
                failed_welcome = await conn.fetchval("SELECT COUNT(*) FROM pending_welcome_dms WHERE failed_attempts >= 5")
                
                # 2. Scheduled DM Stats
                pending_scheduled = await conn.fetchval("SELECT COUNT(*) FROM dm_schedule WHERE status = 'pending'")
                sent_scheduled = await conn.fetchval("SELECT COUNT(*) FROM dm_schedule WHERE status = 'sent'")
                failed_scheduled = await conn.fetchval("SELECT COUNT(*) FROM dm_schedule WHERE status = 'failed'")
                
                # 3. Peer ID Status
                established_peers = await conn.fetchval("SELECT COUNT(*) FROM peer_id_checks WHERE peer_id_established = TRUE")
                total_peers = await conn.fetchval("SELECT COUNT(*) FROM peer_id_checks")

                status_text = (
                    "📨 **DM & Engagement Status**\n\n"
                    "**Welcome DMs (Trial):**\n"
                    f"• Pending/In-Queue: **{pending_welcome}**\n"
                    f"• Critical Failures: **{failed_welcome}**\n\n"
                    "**Scheduled Follow-ups:**\n"
                    f"• Pending: **{pending_scheduled}**\n"
                    f"• Successfully Sent: **{sent_scheduled}**\n"
                    f"• Failed: **{failed_scheduled}**\n\n"
                    "**Peer ID Reliability:**\n"
                    f"• Established: **{established_peers}/{total_peers}** users"
                )
                
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Refresh Stats", callback_data="dm_refresh")],
                    [InlineKeyboardButton("❌ Close", callback_data="dm_close")]
                ])
        except Exception as e:
            status_text = f"⚠️ **Statistics Error**\n\nError: `{str(e)}`"
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Retry", callback_data="dm_refresh")]])

    if is_edit:
        await message.edit_text(status_text, reply_markup=keyboard)
    else:
        await message.reply(status_text, reply_markup=keyboard)

async def handle_dmstatus_callback(bot_instance, client: Client, callback_query: CallbackQuery):
    """Handle dmstatus widget callbacks"""
    data = callback_query.data
    
    if data == "dm_refresh":
        await show_dm_status(bot_instance, callback_query.message, is_edit=True)
    elif data == "dm_close":
        await callback_query.message.delete()
    
    await callback_query.answer()
