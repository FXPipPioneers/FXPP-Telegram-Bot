import logging
from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from src.features.core.config import PENDING_ENTRIES, VIP_GROUP_ID, FREE_GROUP_ID, DEBUG_GROUP_ID, PAIR_CONFIG, EXCLUDED_FROM_TRACKING
from src.features.trading.engine import TradingEngine, SignalParser
from src.features.core.config import PRICE_TRACKING_CONFIG
import asyncio

logger = logging.getLogger(__name__)

async def handle_entry(bot_instance, client: Client, message: Message):
    """Handle /entry command (from telegram_bot.py:954)"""
    if not await bot_instance.is_owner(message.from_user.id):
        return

    user_id = message.from_user.id
    PENDING_ENTRIES[user_id] = {
        'action': None,
        'entry_type': None,
        'pair': None,
        'price': None,
        'groups': [],
        'track_price': True
    }

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("BUY", callback_data="entry_action_buy"),
        InlineKeyboardButton("SELL", callback_data="entry_action_sell")
    ], [InlineKeyboardButton("Cancel", callback_data="entry_cancel")]])

    await message.reply(
        "**Create Trading Signal**\n\nStep 1: Select action:",
        reply_markup=keyboard)

async def handle_entry_callback(bot_instance, client: Client, callback_query: CallbackQuery):
    """Handle entry command callbacks (from telegram_bot.py:977)"""
    user_id = callback_query.from_user.id

    if not await bot_instance.is_owner(user_id):
        await callback_query.answer("This is restricted to the bot owner.",
                                    show_alert=True)
        return

    data = str(callback_query.data or "")

    if data == "entry_cancel":
        PENDING_ENTRIES.pop(user_id, None)
        bot_instance.awaiting_price_input.pop(user_id, None)
        bot_instance.awaiting_custom_pair.pop(user_id, None)
        await callback_query.message.edit_text("Signal creation cancelled.")
        return

    if user_id not in PENDING_ENTRIES:
        await callback_query.answer(
            "Session expired. Please use /entry again.", show_alert=True)
        return

    entry_data = PENDING_ENTRIES[user_id]

    if data.startswith("entry_action_"):
        action = str(data.replace("entry_action_", "")).upper()
        entry_data['action'] = action

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Execution (Market)",
                                    callback_data="entry_type_execution"),
            InlineKeyboardButton("Limit Order",
                                    callback_data="entry_type_limit")
        ], [InlineKeyboardButton("Cancel", callback_data="entry_cancel")]])

        await callback_query.message.edit_text(
            f"**Create Trading Signal**\n\n"
            f"Action: **{action}**\n\n"
            f"Step 2: Select order type:",
            reply_markup=keyboard)

    elif data.startswith("entry_type_"):
        entry_type = str(data.replace("entry_type_", ""))
        entry_data['entry_type'] = entry_type

        bot_instance.awaiting_custom_pair[user_id] = callback_query.message.id
        await callback_query.message.edit_text(
            f"**Create Trading Signal**\n\n"
            f"Action: **{entry_data['action']}**\n"
            f"Type: **{entry_type.upper()}**\n\n"
            f"Step 3: Type the trading pair (e.g., EURUSD, GBPJPY, XAUUSD):"
        )

    elif data.startswith("entry_group_"):
        group_choice = str(data.replace("entry_group_", ""))

        if group_choice == "vip":
            entry_data['groups'] = [VIP_GROUP_ID] if VIP_GROUP_ID else []
            entry_data['track_price'] = True
        elif group_choice == "free":
            entry_data['groups'] = [FREE_GROUP_ID] if FREE_GROUP_ID else []
            entry_data['track_price'] = True
        elif group_choice == "both":
            entry_data['groups'] = []
            if VIP_GROUP_ID:
                entry_data['groups'].append(VIP_GROUP_ID)
            if FREE_GROUP_ID:
                entry_data['groups'].append(FREE_GROUP_ID)
            entry_data['track_price'] = True
        elif group_choice == "manual":
            entry_data['groups'] = [callback_query.message.chat.id]
            entry_data['track_price'] = False
            entry_data['manual_signal'] = True

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Confirm & Send", callback_data="entry_confirm"),
            InlineKeyboardButton("Cancel", callback_data="entry_cancel")
        ]])

        await callback_query.message.edit_text(
            f"**Confirm & Send Signal**\n\n"
            f"Action: **{entry_data['action']}**\n"
            f"Type: **{entry_data['entry_type'].upper()}**\n"
            f"Pair: **{entry_data['pair']}**\n"
            f"Entry: **{entry_data['price']}**\n"
            f"Groups: **{len(entry_data['groups'])} group(s)**\n"
            f"Tracking: **{'Yes' if entry_data['track_price'] else 'No'}**",
            reply_markup=keyboard)

    elif data == "entry_confirm":
        await execute_entry_signal(bot_instance, client, callback_query, entry_data)
        PENDING_ENTRIES.pop(user_id, None)

    elif data == "entry_back_groups":
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("VIP Group Only", callback_data="entry_group_vip"),
            InlineKeyboardButton("Free Group Only", callback_data="entry_group_free")
        ], [
            InlineKeyboardButton("Both Groups", callback_data="entry_group_both"),
            InlineKeyboardButton("Cancel", callback_data="entry_cancel")
        ]])
        await callback_query.message.edit_text("Step 5: Select where to send:", reply_markup=keyboard)

    await callback_query.answer()

async def execute_entry_signal(bot_instance, client: Client, callback_query, entry_data: dict):
    """Execute and track the signal (from telegram_bot.py:1139)"""
    try:
        pair = entry_data['pair']
        action = entry_data['action']
        entry_price = entry_data['price']
        groups = entry_data.get('groups', [])
        track_price = entry_data.get('track_price', True)
        
        engine = TradingEngine(bot_instance.db)
        levels = engine.calculate_tp_sl_levels(entry_price, pair, action)
        
        signal_text = (
            f"**Trade Signal For: {pair}**\n\n"
            f"Action: {action}\n"
            f"Entry: {entry_price}\n"
            f"Take Profit 1: {levels['tp1']:.5f}\n"
            f"Take Profit 2: {levels['tp2']:.5f}\n"
            f"Take Profit 3: {levels['tp3']:.5f}\n"
            f"Stop Loss: {levels['sl']:.5f}"
        )
        
        message_ids = {}
        for group_id in groups:
            try:
                msg = await client.send_message(group_id, signal_text)
                message_ids[str(group_id)] = msg.id
            except Exception as e:
                logger.error(f"Failed to send to group {group_id}: {e}")
        
        if message_ids and track_price and pair not in EXCLUDED_FROM_TRACKING:
            trade_key = f"{groups[0]}_{list(message_ids.values())[0]}" if message_ids else None
            if trade_key:
                trade_data = {
                    'pair': pair,
                    'action': action,
                    'entry_price': entry_price,
                    'tp1_price': levels['tp1'],
                    'tp2_price': levels['tp2'],
                    'tp3_price': levels['tp3'],
                    'sl_price': levels['sl'],
                    'status': 'active',
                    'tp_hits': [],
                    'breakeven_active': False,
                    'chat_id': groups[0],
                    'channel_id': groups[0],
                    'message_id': list(message_ids.values())[0],
                    'created_at': None,
                    'channel_message_map': message_ids,
                    'all_channel_ids': groups
                }
                
                PRICE_TRACKING_CONFIG['active_trades'][trade_key] = trade_data
                
                if hasattr(bot_instance.db, 'save_active_trade'):
                    await bot_instance.db.save_active_trade(trade_data)
        
        await callback_query.message.edit_text("✅ Signal sent successfully!")
    except Exception as e:
        logger.error(f"Error executing signal: {e}")
        await callback_query.message.edit_text(f"❌ Error sending signal: {str(e)}")
