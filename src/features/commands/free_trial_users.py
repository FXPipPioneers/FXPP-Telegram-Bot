import logging
from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from src.features.core.config import AMSTERDAM_TZ, AUTO_ROLE_CONFIG
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

async def handle_freetrialusers(bot_instance, client: Client, message: Message):
    if not await bot_instance.is_owner(message.from_user.id):
        return
    await show_freetrial_menu(bot_instance, message)

async def show_freetrial_menu(bot_instance, message: Message, is_edit=False):
    """Display the free trial management menu"""
    db = bot_instance.db
    
    # 1. Active Trial Count
    active_count = len(AUTO_ROLE_CONFIG['active_members'])
    
    # 2. Get DB stats if available
    db_count = 0
    weekend_pending = 0
    if db.pool:
        try:
            async with db.pool.acquire() as conn:
                db_count = await conn.fetchval("SELECT COUNT(*) FROM active_members")
                weekend_pending = await conn.fetchval("SELECT COUNT(*) FROM active_members WHERE weekend_delayed = TRUE")
        except Exception as e:
            logger.error(f"Error fetching trial stats: {e}")

    status_text = (
        "💎 **Free Trial Management**\n\n"
        f"• Active Trials: **{active_count}** (Cache) / **{db_count}** (DB)\n"
        f"• Weekend Delayed: **{weekend_pending}**\n\n"
        "Select an action to manage trial users:"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📜 List Active Trials", callback_data="ft_list_active")],
        [InlineKeyboardButton("⏳ List Weekend Delayed", callback_data="ft_list_weekend")],
        [InlineKeyboardButton("🔄 Sync Cache with DB", callback_data="ft_sync")],
        [InlineKeyboardButton("❌ Close", callback_data="ft_close")]
    ])

    if is_edit:
        await message.edit_text(status_text, reply_markup=keyboard)
    else:
        await message.reply(status_text, reply_markup=keyboard)

async def handle_freetrial_callback(bot_instance, client: Client, callback_query: CallbackQuery):
    """Handle free trial menu callbacks"""
    data = callback_query.data
    db = bot_instance.db
    
    if data == "ft_list_active":
        await list_trials(bot_instance, callback_query, "active")
    elif data == "ft_list_weekend":
        await list_trials(bot_instance, callback_query, "weekend")
    elif data == "ft_sync":
        # Logic to reload AUTO_ROLE_CONFIG from DB
        await sync_trial_cache(bot_instance)
        await callback_query.answer("Cache synced with database")
        await show_freetrial_menu(bot_instance, callback_query.message, is_edit=True)
    elif data == "ft_close":
        await callback_query.message.delete()
    elif data == "ft_back":
        await show_freetrial_menu(bot_instance, callback_query.message, is_edit=True)
    
    await callback_query.answer()

async def list_trials(bot_instance, cb_query, list_type):
    """Display a list of trial users with expiry times"""
    db = bot_instance.db
    if not db.pool:
        await cb_query.answer("Database not available", show_alert=True)
        return

    try:
        async with db.pool.acquire() as conn:
            if list_type == "active":
                rows = await conn.fetch("SELECT member_id, expiry_time FROM active_members ORDER BY expiry_time ASC LIMIT 30")
                title = "📜 **Active Trial Users**"
            else:
                rows = await conn.fetch("SELECT member_id, expiry_time FROM active_members WHERE weekend_delayed = TRUE ORDER BY expiry_time ASC LIMIT 30")
                title = "⏳ **Weekend Delayed Trials**"

            if not rows:
                text = f"{title}\n\nNo users found in this category."
            else:
                text = f"{title}\n\n"
                for row in rows:
                    user_id = row['member_id']
                    expiry = row['expiry_time']
                    if isinstance(expiry, datetime):
                        expiry_str = expiry.strftime("%Y-%m-%d %H:%M")
                    else:
                        expiry_str = str(expiry)
                    text += f"• `{user_id}` - Expires: {expiry_str}\n"

            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="ft_back")]])
            await cb_query.message.edit_text(text, reply_markup=keyboard)
    except Exception as e:
        await cb_query.answer(f"Error: {str(e)}", show_alert=True)

async def sync_trial_cache(bot_instance):
    """Sync the memory cache with database state"""
    db = bot_instance.db
    if not db.pool: return
    try:
        async with db.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM active_members")
            AUTO_ROLE_CONFIG['active_members'] = {}
            for row in rows:
                AUTO_ROLE_CONFIG['active_members'][str(row['member_id'])] = {
                    'joined_at': row['role_added_time'].isoformat() if row['role_added_time'] else None,
                    'expiry_time': row['expiry_time'].isoformat() if row['expiry_time'] else None,
                    'weekend_delayed': row['weekend_delayed'],
                    'chat_id': -1002446702636 # Default VIP group
                }
    except Exception as e:
        logger.error(f"Cache sync error: {e}")
