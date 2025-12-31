import logging
from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from src.features.core.config import PRICE_TRACKING_CONFIG
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)
AMSTERDAM_TZ = pytz.timezone('Europe/Amsterdam')

async def handle_active_trades(bot_instance, client: Client, message: Message):
    if not await bot_instance.is_owner(message.from_user.id):
        return
    await show_active_trades_list(bot_instance, message)

async def show_active_trades_list(bot_instance, message: Message, is_edit=False):
    """Display list of active trades with current prices and positions"""
    trades = PRICE_TRACKING_CONFIG['active_trades']

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Refresh", callback_data="at_refresh_list")],
        [InlineKeyboardButton("📖 Position Guide", callback_data="at_view_guide")],
        [InlineKeyboardButton("❌ Close", callback_data="at_close")]
    ])

    if not trades:
        text = "**No Active Trades**\n\nThere are currently no active trading signals being tracked."
        if is_edit:
            await message.edit_text(text, reply_markup=keyboard)
        else:
            await message.reply(text, reply_markup=keyboard)
        return

    # Calculate remaining time until next refresh
    remaining_seconds = PRICE_TRACKING_CONFIG['check_interval']
    last_check_time = PRICE_TRACKING_CONFIG.get('last_price_check_time')
    if last_check_time:
        if isinstance(last_check_time, str):
            last_check_time = datetime.fromisoformat(last_check_time)
        elapsed = (datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ) - last_check_time).total_seconds()
        remaining_seconds = max(0, int(PRICE_TRACKING_CONFIG['check_interval'] - elapsed))

    response = f"**Active Trades ({len(trades)})**\n"
    response += f"Next refresh in: {remaining_seconds}s\n\n"

    for msg_id, trade in list(trades.items())[:10]:
        pair = trade.get('pair', 'Unknown')
        action = trade.get('action', 'Unknown')
        entry = trade.get('entry_price', trade.get('entry', 0))
        tp1 = trade.get('tp1_price', trade.get('tp1', 0))
        tp2 = trade.get('tp2_price', trade.get('tp2', 0))
        tp3 = trade.get('tp3_price', trade.get('tp3', 0))
        sl = trade.get('sl_price', trade.get('sl', 0))

        live_price = await bot_instance.get_live_price(pair)

        if live_price:
            position_info = bot_instance.analyze_trade_position(
                action, entry, tp1, tp2, tp3, sl, live_price)
            price_line = f"**Current: {live_price:.5f}** {position_info['emoji']}"
            position_text = f"_{position_info['position']}_"
        else:
            price_line = "**Current: N/A**"
            position_text = "_Unable to get price_"

        response += f"**{pair}** - {action} | {price_line} | {position_text}\n"

    if len(trades) > 10:
        response += f"\n_...and {len(trades) - 10} more trades_"

    if is_edit:
        await message.edit_text(response, reply_markup=keyboard)
    else:
        await message.reply(response, reply_markup=keyboard)

async def show_position_guide(bot_instance, message: Message, is_edit=False):
    """Display the position guide explaining all colors and their meanings"""
    guide = ("**Position Guide - Trade Status Indicators**\n\n"
            "🔴 = **At/Beyond SL**\n"
            "   Price is at or has crossed the stop loss level\n\n"
            "🟡 = **At/Near Entry**\n"
            "   Price is at or very close to entry\n\n"
            "🟠 = **Between Entry & TP1**\n"
            "   Price is in profit but hasn't reached TP1\n\n"
            "🟢 = **Between TP1 & TP2**\n"
            "   Price has passed TP1 and targeting TP2\n\n"
            "💚 = **Between TP2 & TP3**\n"
            "   Price has passed TP2 (breakeven active) and targeting TP3\n\n"
            "🚀 = **Max Profit (Beyond TP3)**\n"
            "   Price has exceeded all take profit levels!")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to List", callback_data="at_view_list")],
        [InlineKeyboardButton("❌ Close", callback_data="at_close")]
    ])
    
    if is_edit:
        await message.edit_text(guide, reply_markup=keyboard)
    else:
        await message.reply(guide, reply_markup=keyboard)

def analyze_trade_position(action: str, entry: float, tp1: float,
                           tp2: float, tp3: float, sl: float,
                           current_price: float) -> dict:
    """
    Feature 11: Fix Sell Position Logic & Color-coded emojis
    Analyze trade progress and return appropriate status emoji
    """
    is_buy = action.upper() == "BUY"
    
    if is_buy:
        # BUY logic: Price goes up = Profit
        if current_price <= sl:
            return {"emoji": "🔴", "position": "At/Below SL"}
        elif current_price <= entry:
            return {"emoji": "🟡", "position": "Below Entry"}
        elif current_price <= tp1:
            return {"emoji": "🟠", "position": "Between Entry and TP1"}
        elif current_price <= tp2:
            return {"emoji": "🟢", "position": "Between TP1 and TP2"}
        elif current_price <= tp3:
            return {"emoji": "💚", "position": "Between TP2 and TP3"}
        else:
            return {"emoji": "🚀", "position": "Above TP3 - Max Profit"}
    else:
        # SELL logic: Price goes down = Profit
        if current_price >= sl:
            return {"emoji": "🔴", "position": "At/Above SL"}
        elif current_price >= entry:
            return {"emoji": "🟡", "position": "Above Entry"}
        elif current_price <= tp3: # Price reached TP3
            return {"emoji": "🚀", "position": "Below TP3 - Max Profit"}
        elif current_price <= tp2: # Price reached TP2
            return {"emoji": "💚", "position": "Between TP2 and TP3"}
        elif current_price <= tp1: # Price reached TP1
            return {"emoji": "🟢", "position": "Between TP1 and TP2"}
        else: # Price is between Entry and TP1
            return {"emoji": "🟠", "position": "Between Entry and TP1"}

async def handle_activetrades_callback(bot_instance, client: Client, callback_query: CallbackQuery):
    """Handle activetrades widget navigation callbacks"""
    data = callback_query.data
    
    if data == "at_refresh_list" or data == "at_view_list":
        await show_active_trades_list(bot_instance, callback_query.message, is_edit=True)
    elif data == "at_view_guide":
        await show_position_guide(bot_instance, callback_query.message, is_edit=True)
    elif data == "at_close":
        await callback_query.message.delete()
    
    await callback_query.answer()
