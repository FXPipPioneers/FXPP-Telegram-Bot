import logging
import asyncio
import pytz
from datetime import datetime
from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from src.features.core.config import (PENDING_ENTRIES, VIP_GROUP_ID, FREE_GROUP_ID, 
                                    DEBUG_GROUP_ID, PAIR_CONFIG, EXCLUDED_FROM_TRACKING, 
                                    PRICE_TRACKING_CONFIG, AMSTERDAM_TZ)
from src.features.trading.engine import TradingEngine, SignalParser

logger = logging.getLogger(__name__)

async def handle_entry(bot_instance, client: Client, message: Message):
    """Handle /entry command (Step 1: Action Selection)"""
    if not await bot_instance.is_owner(message.from_user.id):
        return

    user_id = message.from_user.id
    PENDING_ENTRIES[user_id] = {
        'action': None,
        'entry_type': None,
        'pair': None,
        'price': None,
        'groups': [],
        'track_price': True,
        'manual_signal': False
    }

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("BUY", callback_data="entry_action_buy"),
        InlineKeyboardButton("SELL", callback_data="entry_action_sell")
    ], [InlineKeyboardButton("Cancel", callback_data="entry_cancel")]])

    await message.reply(
        "**Create Trading Signal**\n\nStep 1: Select action:",
        reply_markup=keyboard)

async def handle_entry_callback(bot_instance, client: Client, callback_query: CallbackQuery):
    """Handle entry command callbacks (Main state machine)"""
    user_id = callback_query.from_user.id

    if not await bot_instance.is_owner(user_id):
        await callback_query.answer("This is restricted to the bot owner.", show_alert=True)
        return

    data = str(callback_query.data or "")

    if data == "entry_cancel":
        PENDING_ENTRIES.pop(user_id, None)
        bot_instance.awaiting_price_input.pop(user_id, None)
        bot_instance.awaiting_custom_pair.pop(user_id, None)
        await callback_query.message.edit_text("Signal creation cancelled.")
        return

    if user_id not in PENDING_ENTRIES:
        await callback_query.answer("Session expired. Please use /entry again.", show_alert=True)
        return

    entry_data = PENDING_ENTRIES[user_id]

    if data.startswith("entry_action_"):
        action = str(data.replace("entry_action_", "")).upper()
        entry_data['action'] = action
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Execution (Market)", callback_data="entry_type_execution"),
            InlineKeyboardButton("Limit Order", callback_data="entry_type_limit")
        ], [InlineKeyboardButton("Cancel", callback_data="entry_cancel")]])
        await callback_query.message.edit_text(
            f"**Create Trading Signal**\n\nAction: **{action}**\n\nStep 2: Select order type:",
            reply_markup=keyboard)

    elif data.startswith("entry_type_"):
        entry_type = str(data.replace("entry_type_", ""))
        entry_data['entry_type'] = entry_type
        bot_instance.awaiting_custom_pair[user_id] = callback_query.message.id
        await callback_query.message.edit_text(
            f"**Create Trading Signal**\n\nAction: **{entry_data['action']}**\nType: **{entry_type.upper()}**\n\nStep 3: Type the trading pair (e.g., EURUSD, GBPJPY):"
        )

    elif data.startswith("entry_pair_"):
        pair = str(data.replace("entry_pair_", ""))
        entry_data['pair'] = pair
        if entry_data['entry_type'] == 'limit':
            bot_instance.awaiting_price_input[user_id] = True
            await callback_query.message.edit_text(f"**Step 3b: Limit Order Price**\nEnter the entry price for **{pair}**:")
        else:
            await show_group_selection(bot_instance, callback_query, entry_data)

    elif data.startswith("entry_group_"):
        group_choice = str(data.replace("entry_group_", ""))
        entry_data['groups'] = []
        entry_data['manual_signal'] = False
        
        if group_choice == "vip":
            if VIP_GROUP_ID: entry_data['groups'].append(VIP_GROUP_ID)
            entry_data['track_price'] = True
        elif group_choice == "free":
            if FREE_GROUP_ID: entry_data['groups'].append(FREE_GROUP_ID)
            entry_data['track_price'] = True
        elif group_choice == "both":
            if VIP_GROUP_ID: entry_data['groups'].append(VIP_GROUP_ID)
            if FREE_GROUP_ID: entry_data['groups'].append(FREE_GROUP_ID)
            entry_data['track_price'] = True
        elif group_choice == "manual":
            entry_data['groups'] = [callback_query.message.chat.id]
            entry_data['track_price'] = False
            entry_data['manual_signal'] = True

        await show_confirmation(bot_instance, callback_query, entry_data)

    elif data == "entry_confirm":
        await execute_entry_signal(bot_instance, client, callback_query, entry_data)

    elif data == "entry_back_groups":
        await show_group_selection(bot_instance, callback_query, entry_data)

    await callback_query.answer()

async def show_group_selection(bot_instance, callback_query: CallbackQuery, entry_data: dict):
    """Step 4: Select target groups"""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("VIP Group Only", callback_data="entry_group_vip")],
        [InlineKeyboardButton("Free Group Only", callback_data="entry_group_free")],
        [InlineKeyboardButton("Both Groups", callback_data="entry_group_both")],
        [InlineKeyboardButton("Manual Signal (untracked)", callback_data="entry_group_manual")],
        [InlineKeyboardButton("Cancel", callback_data="entry_cancel")]
    ])
    price_text = f"\nPrice: **{entry_data['price']}**" if entry_data['price'] else ""
    await callback_query.message.edit_text(
        f"**Create Trading Signal**\n\nAction: **{entry_data['action']}**\nType: **{entry_data['entry_type'].upper()}**\nPair: **{entry_data['pair']}**{price_text}\n\nStep 4: Select where to send:",
        reply_markup=keyboard)

async def show_confirmation(bot_instance, callback_query: CallbackQuery, entry_data: dict):
    """Step 5: Final Review"""
    group_names = []
    if VIP_GROUP_ID in entry_data['groups']: group_names.append("VIP Group")
    if FREE_GROUP_ID in entry_data['groups']: group_names.append("Free Group")
    if entry_data.get('manual_signal'): group_names.append("Manual Signal")
    
    pair = entry_data['pair']
    decimals = PAIR_CONFIG.get(pair, {}).get('decimals', 5)
    price_text = f"Price: **${entry_data['price']:.{decimals}f}**\n" if entry_data['price'] else "Price: **Live (auto-fetch)**\n"
    tracking_text = "Price Tracking: **Enabled**" if entry_data.get('track_price', True) else "Price Tracking: **Disabled**"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Confirm & Send Signal", callback_data="entry_confirm")],
        [InlineKeyboardButton("Change Groups", callback_data="entry_back_groups")],
        [InlineKeyboardButton("Cancel", callback_data="entry_cancel")]
    ])

    await callback_query.message.edit_text(
        f"**Confirm Trading Signal**\n\n"
        f"Type: **{entry_data['action']} {entry_data['entry_type'].upper()}**\n"
        f"Pair: **{pair}**\n{price_text}"
        f"Send to: **{', '.join(group_names)}**\n{tracking_text}\n\nReady to send?",
        reply_markup=keyboard)

async def execute_entry_signal(bot_instance, client: Client, callback_query: CallbackQuery, entry_data: dict):
    """Execution Phase: Fetch price, calculate levels, send messages, and initiate tracking"""
    user_id = callback_query.from_user.id
    pair = entry_data['pair']
    action = entry_data['action']
    entry_price = entry_data['price']
    
    # Auto-fetch price if not provided (Market Execution)
    if not entry_price:
        entry_price = await bot_instance.get_live_price(pair)
        if not entry_price:
            # Re-edit the original menu message if possible
            try:
                await callback_query.message.edit_text(f"❌ Could not get live price for {pair}. Use Limit Order.")
            except:
                await client.send_message(user_id, f"❌ Could not get live price for {pair}. Use Limit Order.")
            return

    engine = TradingEngine(bot_instance.db.pool)
    levels = engine.calculate_tp_sl_levels(entry_price, pair, action)
    decimals = PAIR_CONFIG.get(pair, {}).get('decimals', 5)
    fmt = lambda p: f"{p:.{decimals}f}"

    signal_text = (f"**Trade Signal For: {pair}**\n"
                   f"Entry Type: {action.capitalize()} {entry_data['entry_type']}\n"
                   f"Entry Price: {fmt(entry_price)}\n\n"
                   f"**Take Profit Levels:**\n"
                   f"Take Profit 1: {fmt(levels['tp1'])}\n"
                   f"Take Profit 2: {fmt(levels['tp2'])}\n"
                   f"Take Profit 3: {fmt(levels['tp3'])}\n\n"
                   f"Stop Loss: {fmt(levels['sl'])}")

    if pair.upper() in ['US100', 'GER40']:
        signal_text += "\n\n⚠️ *Note: Prices for this pair vary significantly by broker. Recalculate levels if needed.*"

    sent_messages = []
    for group_id in entry_data['groups']:
        try:
            sent_msg = await client.send_message(group_id, signal_text)
            sent_messages.append({'msg': sent_msg, 'cid': group_id})
        except Exception as e:
            logger.error(f"Send failed to {group_id}: {e}")

    # Initiate Tracking
    is_excluded = pair.upper() in EXCLUDED_FROM_TRACKING
    if sent_messages and entry_data.get('track_price') and not is_excluded:
        for info in sent_messages:
            trade_key = f"{info['cid']}_{info['msg'].id}"
            trade_data = {
                'message_id': str(info['msg'].id),
                'chat_id': info['cid'],
                'pair': pair, 'action': action, 'entry_price': float(entry_price),
                'tp1_price': float(levels['tp1']), 'tp2_price': float(levels['tp2']),
                'tp3_price': float(levels['tp3']), 'sl_price': float(levels['sl']),
                'status': 'active', 'tp_hits': [], 'breakeven_active': False,
                'created_at': datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ).isoformat()
            }
            PRICE_TRACKING_CONFIG['active_trades'][trade_key] = trade_data
            if hasattr(bot_instance.db, 'save_active_trade'):
                await bot_instance.db.save_active_trade(trade_data)

    # Final success message
    try:
        await callback_query.message.edit_text(f"✅ Signal for **{pair}** sent successfully to {len(sent_messages)} group(s)!")
    except:
        await client.send_message(user_id, f"✅ Signal for **{pair}** sent successfully to {len(sent_messages)} group(s)!")
    
    PENDING_ENTRIES.pop(user_id, None)
    await bot_instance.log_to_debug(f"🚀 New Signal: {pair} {action} @ {fmt(entry_price)}")