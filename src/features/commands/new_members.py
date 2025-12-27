from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from datetime import datetime, timedelta
import pytz
import logging
from src.features.core.config import AMSTERDAM_TZ, AUTO_ROLE_CONFIG

logger = logging.getLogger(__name__)

async def handle_newmemberslist(client: Client, message: Message, is_owner_func):
    if not await is_owner_func(message.from_user.id):
        await message.reply("This command is restricted to the bot owner.")
        return

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Free Group Joiners", callback_data="nml_free_group")],
        [InlineKeyboardButton("⭐ VIP Trial Joiners", callback_data="nml_vip_trial")],
        [InlineKeyboardButton("❌ Close", callback_data="nml_close")]
    ])

    await message.reply(
        "**New Members Tracking**\n\n"
        "Select an option to view member data:\n\n"
        "📊 **Free Group Joiners** - Shows all free group joiners grouped by week\n"
        "⭐ **VIP Trial Joiners** - Shows members with active VIP trial and days remaining",
        reply_markup=keyboard
    )

async def handle_newmemberslist_callback(client: Client, callback_query: CallbackQuery, is_owner_func, db_pool):
    if not await is_owner_func(callback_query.from_user.id):
        await callback_query.answer("Restricted to bot owner.", show_alert=True)
        return

    data = callback_query.data
    if data == "nml_close":
        await callback_query.message.edit_text("❌ Closed.")
        await callback_query.answer()
        return

    if data == "nml_free_group":
        await _show_free_group_joiners(callback_query, db_pool)
    elif data == "nml_vip_trial":
        await _show_vip_trial_joiners(callback_query)

async def _show_free_group_joiners(callback_query: CallbackQuery, db_pool):
    if not db_pool:
        await callback_query.message.edit_text("❌ Database not available")
        await callback_query.answer()
        return

    try:
        current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
        monday = current_time - timedelta(days=current_time.weekday())
        sunday = monday + timedelta(days=6)

        async with db_pool.acquire() as conn:
            joins = await conn.fetch(
                '''SELECT user_id, joined_at FROM free_group_joins 
                   WHERE joined_at >= $1 AND joined_at <= $2
                   ORDER BY joined_at DESC''',
                monday, sunday
            )

        if not joins:
            await callback_query.message.edit_text(f"No new joiners this week.")
            await callback_query.answer()
            return

        joiners_by_date = {}
        for row in joins:
            date_key = row['joined_at'].strftime('%d-%m-%Y')
            if date_key not in joiners_by_date:
                joiners_by_date[date_key] = []
            joiners_by_date[date_key].append(f"User {row['user_id']}")

        text = f"**Free Group Joiners - This Week**\n\n"
        text += f"**📊 Total: {len(joins)} members**\n\n"
        for date in sorted(joiners_by_date.keys(), reverse=True):
            users = ", ".join(joiners_by_date[date])
            text += f"**{date}** ({len(joiners_by_date[date])}): {users}\n"

        await callback_query.message.edit_text(text)
        await callback_query.answer()
    except Exception as e:
        await callback_query.message.edit_text(f"❌ Error: {str(e)}")
        await callback_query.answer()

async def _show_vip_trial_joiners(callback_query: CallbackQuery):
    try:
        current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
        if not AUTO_ROLE_CONFIG['active_members']:
            await callback_query.message.edit_text("No active trial members.")
            await callback_query.answer()
            return

        members_by_date = {}
        total = 0
        for user_id_str, member_data in AUTO_ROLE_CONFIG['active_members'].items():
            joined = datetime.fromisoformat(member_data.get('joined_at', current_time.isoformat()))
            if joined.tzinfo is None: joined = AMSTERDAM_TZ.localize(joined)
            date_key = joined.strftime('%d-%m-%Y')
            if date_key not in members_by_date: members_by_date[date_key] = []
            
            # Fetch user info for name
            name_display = ""
            try:
                user = await client.get_users(int(user_id_str))
                first_name = user.first_name or ""
                last_name = user.last_name or ""
                name_display = f" ({first_name} {last_name})".strip()
            except Exception:
                pass
                
            members_by_date[date_key].append(f"User {user_id_str}{name_display}")
            total += 1

        text = f"**Active VIP Trial Members**\n\n**📊 Total: {total}**\n\n"
        for date in sorted(members_by_date.keys(), reverse=True):
            text += f"**{date}** ({len(members_by_date[date])}): {', '.join(members_by_date[date])}\n"

        await callback_query.message.edit_text(text)
        await callback_query.answer()
    except Exception as e:
        await callback_query.message.edit_text(f"❌ Error: {str(e)}")
        await callback_query.answer()
