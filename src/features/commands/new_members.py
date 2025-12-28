from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from datetime import datetime, timedelta
import pytz
import logging
from src.features.core.config import AMSTERDAM_TZ, AUTO_ROLE_CONFIG

logger = logging.getLogger(__name__)

async def handle_newmemberslist(client: Client, message: Message, is_owner_func):
    """Show new members list with subcommands for free group joiners and VIP trial activations"""
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
    """Handle newmemberslist subcommand callbacks"""
    if not await is_owner_func(callback_query.from_user.id):
        await callback_query.answer("Restricted to bot owner.", show_alert=True)
        return

    data = callback_query.data
    
    if data == "nml_close":
        await callback_query.message.edit_text("❌ Closed.")
        await callback_query.answer()
        return
    
    if data == "nml_back":
        # Return to main menu
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 Free Group Joiners", callback_data="nml_free_group")],
            [InlineKeyboardButton("⭐ VIP Trial Joiners", callback_data="nml_vip_trial")],
            [InlineKeyboardButton("❌ Close", callback_data="nml_close")]
        ])
        await callback_query.message.edit_text(
            "**New Members Tracking**\n\n"
            "Select an option to view member data:\n\n"
            "📊 **Free Group Joiners** - Shows all free group joiners grouped by week\n"
            "⭐ **VIP Trial Joiners** - Shows members with active VIP trial and days remaining",
            reply_markup=keyboard
        )
        await callback_query.answer()
        return

    if data == "nml_free_group":
        await _show_free_group_joiners(callback_query, db_pool)
    elif data == "nml_vip_trial":
        await _show_vip_trial_joiners(callback_query)

async def _show_free_group_joiners(callback_query: CallbackQuery, db_pool):
    """Show free group joiners grouped by week with daily and weekly totals"""
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
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="nml_back")]
            ])
            await callback_query.message.edit_text(
                f"**Free Group Joiners - This Week**\n\n"
                f"Monday {monday.strftime('%d-%m-%Y')} to Sunday {sunday.strftime('%d-%m-%Y')}\n\n"
                f"No new joiners this week.",
                reply_markup=keyboard
            )
            await callback_query.answer()
            return

        joiners_by_date = {}
        for row in joins:
            date_key = row['joined_at'].strftime('%d-%m-%Y')
            if date_key not in joiners_by_date:
                joiners_by_date[date_key] = []
            joiners_by_date[date_key].append(f"User {row['user_id']}")

        # Calculate weekly total
        total_weekly = len(joins)
        
        text = f"**Free Group Joiners - This Week**\n\n"
        text += f"Monday {monday.strftime('%d-%m-%Y')} to Sunday {sunday.strftime('%d-%m-%Y')}\n"
        text += f"**📊 Total This Week: {total_weekly} members**\n\n"
        
        for date in sorted(joiners_by_date.keys(), reverse=True):
            users = ", ".join(joiners_by_date[date])
            daily_count = len(joiners_by_date[date])
            text += f"**{date}** ({daily_count}): {users}\n"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data="nml_back")]
        ])
        
        await callback_query.message.edit_text(text, reply_markup=keyboard)
        await callback_query.answer()
    except Exception as e:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data="nml_back")]
        ])
        await callback_query.message.edit_text(f"❌ Error: {str(e)}", reply_markup=keyboard)
        await callback_query.answer()

async def _show_vip_trial_joiners(callback_query: CallbackQuery):
    """Show all active VIP trial members with time remaining and join dates"""
    try:
        current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
        
        if not AUTO_ROLE_CONFIG['active_members']:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="nml_back")]
            ])
            await callback_query.message.edit_text(
                "**Active VIP Trial Members**\n\n"
                "No active trial members at this time.",
                reply_markup=keyboard
            )
            await callback_query.answer()
            return

        # Get all active trial members with dates
        trials_with_time = []
        joiners_by_date = {}
        
        for user_id_str, member_data in AUTO_ROLE_CONFIG['active_members'].items():
            expiry = datetime.fromisoformat(
                member_data.get('expiry_time', current_time.isoformat()))
            if expiry.tzinfo is None:
                expiry = AMSTERDAM_TZ.localize(expiry)
            
            joined = datetime.fromisoformat(
                member_data.get('joined_at', current_time.isoformat()))
            if joined.tzinfo is None:
                joined = AMSTERDAM_TZ.localize(joined)
            
            join_date_str = joined.strftime('%d-%m-%Y')
            
            time_left = expiry - current_time
            total_seconds = max(0, time_left.total_seconds())
            
            trials_with_time.append({
                'user_id': user_id_str,
                'total_seconds': total_seconds,
                'expiry': expiry,
                'join_date': join_date_str,
                'joined': joined
            })
            
            # Group by date for weekly summary
            if join_date_str not in joiners_by_date:
                joiners_by_date[join_date_str] = 0
            joiners_by_date[join_date_str] += 1
        
        # Sort by time remaining (ascending - least time first)
        trials_with_time.sort(key=lambda x: x['total_seconds'])
        
        text = "**Active VIP Trial Members**\n\n"
        
        # Weekly summary
        total_weekly = len(trials_with_time)
        text += f"**📊 Total This Week: {total_weekly} members**\n\n"
        
        # Group members by date for compact display
        members_by_date = {}
        for trial in trials_with_time:
            date_key = trial['join_date']
            if date_key not in members_by_date:
                members_by_date[date_key] = []
            
            hours = int(trial['total_seconds'] // 3600)
            minutes = int((trial['total_seconds'] % 3600) // 60)
            members_by_date[date_key].append(f"User {trial['user_id']} ({hours}h {minutes}m)")

        # Compact display grouped by date
        for date in sorted(members_by_date.keys(), reverse=True):
            users = ", ".join(members_by_date[date])
            daily_count = len(members_by_date[date])
            text += f"**{date}** ({daily_count}): {users}\n"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data="nml_back")]
        ])
        
        await callback_query.message.edit_text(text, reply_markup=keyboard)
        await callback_query.answer()
    except Exception as e:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data="nml_back")]
        ])
        await callback_query.message.edit_text(f"❌ Error: {str(e)}", reply_markup=keyboard)
        await callback_query.answer()
