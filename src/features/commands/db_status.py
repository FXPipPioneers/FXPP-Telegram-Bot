import logging
from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import asyncio

logger = logging.getLogger(__name__)

async def handle_dbstatus(bot_instance, client: Client, message: Message):
    if not await bot_instance.is_owner(message.from_user.id):
        return
    await show_db_status(bot_instance, message)

async def show_db_status(bot_instance, message: Message, is_edit=False):
    """Display database health and table statistics"""
    db = bot_instance.db
    
    if not db.pool:
        status_text = "❌ **Database Offline**\n\nPool not initialized or connection failed."
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Retry Connection", callback_data="db_retry")]])
    else:
        try:
            async with db.pool.acquire() as conn:
                # 1. Connection Health
                start_time = asyncio.get_event_loop().time()
                await conn.execute("SELECT 1")
                latency = (asyncio.get_event_loop().time() - start_time) * 1000
                
                # 2. Table Statistics
                tables = [
                    "active_trades", "completed_trades", "vip_trial_activations", 
                    "active_members", "pending_welcome_dms", "dm_schedule",
                    "peer_id_checks", "trial_offer_history"
                ]
                
                stats = []
                for table in tables:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                    stats.append(f"• `{table}`: **{count}** rows")
                
                # 3. Connection Pool Stats
                pool_info = f"• Pool size: {db.pool.get_size()}\n• Available: {db.pool.get_idle_size()}"

                status_text = (
                    "📊 **Database Status**\n\n"
                    f"✅ **Connected** (Latency: {latency:.1f}ms)\n\n"
                    "**Table Statistics:**\n" + "\n".join(stats) + "\n\n"
                    "**Connection Details:**\n" + pool_info
                )
                
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Refresh", callback_data="db_refresh")],
                    [InlineKeyboardButton("❌ Close", callback_data="db_close")]
                ])
        except Exception as e:
            status_text = f"⚠️ **Database Error**\n\nError: `{str(e)}`"
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Retry", callback_data="db_refresh")]])

    if is_edit:
        await message.edit_text(status_text, reply_markup=keyboard)
    else:
        await message.reply(status_text, reply_markup=keyboard)

async def handle_dbstatus_callback(bot_instance, client: Client, callback_query: CallbackQuery):
    """Handle dbstatus widget callbacks"""
    data = callback_query.data
    
    if data == "db_refresh":
        await show_db_status(bot_instance, callback_query.message, is_edit=True)
    elif data == "db_retry":
        await bot_instance.db.connect()
        await show_db_status(bot_instance, callback_query.message, is_edit=True)
    elif data == "db_close":
        await callback_query.message.delete()
    
    await callback_query.answer()
