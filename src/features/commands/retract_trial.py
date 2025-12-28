from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import logging
from src.features.core.config import AUTO_ROLE_CONFIG, AMSTERDAM_TZ
from datetime import datetime, timedelta
import pytz

logger = logging.getLogger(__name__)

async def handle_retract_trial(client: Client, message: Message, is_owner_func, bot_instance):
    """Command to retract trial time for active members"""
    if not await is_owner_func(message.from_user.id):
        await message.reply("❌ This command can only be used by the bot owner.")
        return

    if not AUTO_ROLE_CONFIG['active_members']:
        await message.reply("No active trial members found.")
        return

    # Initialize mappings if needed
    if not hasattr(bot_instance, 'retracttrial_mappings'):
        bot_instance.retracttrial_mappings = {}

    menu_id = str(message.id)[-8:]
    user_mapping = {}
    buttons = []

    # Show first 20 active trial members
    for idx, (user_id_str, member_data) in enumerate(
            list(AUTO_ROLE_CONFIG['active_members'].items())[:20]):
        user_mapping[str(idx)] = user_id_str
        expiry = datetime.fromisoformat(member_data.get('expiry_time', ''))
        if expiry.tzinfo is None:
            expiry = AMSTERDAM_TZ.localize(expiry)

        total_seconds = max(0, (expiry - datetime.now(
            pytz.UTC).astimezone(AMSTERDAM_TZ)).total_seconds())
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)

        buttons.append([
            InlineKeyboardButton(
                f"User {user_id_str} ({hours}h {minutes}m left)",
                callback_data=f"retrt_{menu_id}_select_{idx}")
        ])

    bot_instance.retracttrial_mappings[menu_id] = user_mapping
    buttons.append([
        InlineKeyboardButton("Cancel",
                             callback_data=f"retrt_{menu_id}_cancel")
    ])

    keyboard = InlineKeyboardMarkup(buttons)
    await message.reply(
        "**Retract Trial Time**\n\n"
        "Select a user to adjust their trial expiry time:",
        reply_markup=keyboard)

async def handle_retracttrial_callback(client: Client,
                                       callback_query: CallbackQuery, is_owner_func, bot_instance):
    """Handle retract trial callbacks"""
    user_id = callback_query.from_user.id

    if not await is_owner_func(user_id):
        await callback_query.answer("This is restricted to the bot owner.",
                                    show_alert=True)
        return

    data = callback_query.data

    if not data.startswith("retrt_"):
        return

    parts = data.split("_")
    if len(parts) < 3:
        await callback_query.answer("Invalid callback data.",
                                    show_alert=True)
        return

    menu_id = parts[1]
    action_type = parts[2]

    if not hasattr(bot_instance, 'retracttrial_mappings'):
        bot_instance.retracttrial_mappings = {}

    if action_type == "cancel":
        bot_instance.retracttrial_mappings.pop(menu_id, None)
        await callback_query.message.edit_text("Cancelled.")
        await callback_query.answer()
        return

    if action_type == "select":
        idx = parts[3] if len(parts) > 3 else None
        if not idx:
            await callback_query.answer("Invalid selection.",
                                        show_alert=True)
            return

        user_mapping = bot_instance.retracttrial_mappings.get(menu_id, {})
        selected_user_id_str = user_mapping.get(idx)

        if not selected_user_id_str:
            await callback_query.answer("User not found.", show_alert=True)
            return

        # Show time adjustment menu
        buttons = []
        for hours in [1, 2, 3, 6, 12, 24]:
            buttons.append([
                InlineKeyboardButton(
                    f"Reduce {hours}h",
                    callback_data=f"retrt_{menu_id}_reduce_{idx}_h{hours}")
            ])

        buttons.append([
            InlineKeyboardButton(
                "Custom Hours/Minutes",
                callback_data=f"retrt_{menu_id}_custom_{idx}")
        ])
        buttons.append([
            InlineKeyboardButton("Back",
                                 callback_data=f"retrt_{menu_id}_back")
        ])
        buttons.append([
            InlineKeyboardButton("Cancel",
                                 callback_data=f"retrt_{menu_id}_cancel")
        ])

        keyboard = InlineKeyboardMarkup(buttons)
        await callback_query.message.edit_text(
            f"**Reducing trial for User {selected_user_id_str}**\n\n"
            f"Select hours to subtract:",
            reply_markup=keyboard)
        await callback_query.answer()
        return

    if action_type == "reduce":
        idx = parts[3] if len(parts) > 3 else None
        hours_str = parts[4] if len(parts) > 4 else None

        if not idx or not hours_str or not hours_str.startswith("h"):
            await callback_query.answer("Invalid action.", show_alert=True)
            return

        try:
            hours = int(hours_str[1:])
            minutes = hours * 60

            user_mapping = bot_instance.retracttrial_mappings.get(menu_id, {})
            user_id_str = user_mapping.get(idx)

            if not user_id_str or user_id_str not in AUTO_ROLE_CONFIG[
                    'active_members']:
                await callback_query.answer("User not found.",
                                            show_alert=True)
                return

            member_data = AUTO_ROLE_CONFIG['active_members'][user_id_str]
            old_expiry = datetime.fromisoformat(member_data['expiry_time'])
            if old_expiry.tzinfo is None:
                old_expiry = AMSTERDAM_TZ.localize(old_expiry)

            new_expiry = old_expiry - timedelta(minutes=minutes)
            member_data['expiry_time'] = new_expiry.isoformat()

            # Update database with integer user_id
            if bot_instance.db_pool:
                async with bot_instance.db_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE active_members SET expiry_time = $1 WHERE member_id = $2",
                        new_expiry, int(user_id_str))

            # Save config (if available on bot_instance)
            if hasattr(bot_instance, 'save_auto_role_config'):
                await bot_instance.save_auto_role_config()

            bot_instance.retracttrial_mappings.pop(menu_id, None)
            await callback_query.message.edit_text(
                f"✅ Subtracted {hours} hours from user {user_id_str}\n"
                f"New expiry: {new_expiry.strftime('%Y-%m-%d %H:%M:%S')}")
            await callback_query.answer()

        except (ValueError, IndexError):
            await callback_query.answer("Error processing request.",
                                        show_alert=True)
        except Exception as e:
            await callback_query.message.edit_text(
                f"Error updating trial: {str(e)}")
            await callback_query.answer()
        return

    if action_type == "custom":
        # Handle custom hours/minutes input
        idx = parts[3] if len(parts) > 3 else None
        if not idx:
            await callback_query.answer("Invalid selection.",
                                        show_alert=True)
            return

        user_mapping = bot_instance.retracttrial_mappings.get(menu_id, {})
        selected_user_id_str = user_mapping.get(idx)

        if not selected_user_id_str:
            await callback_query.answer("User not found.", show_alert=True)
            return

        # Ask for input - store the context
        if not hasattr(bot_instance, 'awaiting_retracttrial_input'):
            bot_instance.awaiting_retracttrial_input = {}

        bot_instance.awaiting_retracttrial_input[callback_query.from_user.id] = {
            'menu_id': menu_id,
            'idx': idx,
            'user_id_str': selected_user_id_str
        }

        buttons = [[
            InlineKeyboardButton("Cancel",
                                 callback_data=f"retrt_{menu_id}_cancel")
        ]]
        keyboard = InlineKeyboardMarkup(buttons)

        await callback_query.message.edit_text(
            f"**Custom Time for User {selected_user_id_str}**\n\n"
            f"Send me the time to subtract.\n\n"
            f"Examples:\n"
            f"• `5h` (5 hours)\n"
            f"• `30m` (30 minutes)\n"
            f"• `2h 15m` (2 hours 15 minutes)\n\n"
            f"Just type the number and unit (h or m).",
            reply_markup=keyboard)
        await callback_query.answer()
        return

    if action_type == "back":
        await handle_retract_trial(client, callback_query.message, is_owner_func, bot_instance)
        await callback_query.answer()
        return

    await callback_query.answer()
