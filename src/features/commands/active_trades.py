import logging
from pyrogram.client import Client
from pyrogram.types import Message
from src.features.core.config import PRICE_TRACKING_CONFIG
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)
AMSTERDAM_TZ = pytz.timezone('Europe/Amsterdam')

async def handle_active_trades(bot_instance, client: Client, message: Message):
    if not await bot_instance.is_owner(message.from_user.id):
        return

    # Parse subcommand from message text
    text_parts = message.text.split()
    subcommand = text_parts[1].lower() if len(text_parts) > 1 else None
    
    if subcommand == "list":
        await bot_instance.show_active_trades_list(message)
    elif subcommand == "position" and len(text_parts) > 2 and text_parts[2].lower() == "guide":
        await bot_instance.show_position_guide(message)
    else:
        # Show help message with subcommand options
        help_text = ("**Active Trades Command**\n\n"
                    "Usage:\n"
                    "• `/activetrades list` - Show list of active trades with prices\n"
                    "• `/activetrades position guide` - Show what each color/emoji means")
        await message.reply(help_text)

async def show_active_trades_list(bot_instance, message: Message):
    """Display list of active trades with current prices and positions"""
    trades = PRICE_TRACKING_CONFIG['active_trades']

    if not trades:
        await message.reply(
            "**No Active Trades**\n\nThere are currently no active trading signals being tracked."
        )
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
        response += f"_...and {len(trades) - 10} more trades_"

    await message.reply(response)

async def show_position_guide(bot_instance, message: Message):
    """Display the position guide explaining all colors and their meanings"""
    guide = ("**Position Guide - Trade Status Indicators**\n\n"
            "🔴 = **At/Beyond SL**\n"
            "   Price is at or has crossed the stop loss level (trade should be closed)\n\n"
            "🟡 = **At/Near Entry**\n"
            "   Price is at or very close to the entry point\n\n"
            "🟠 = **Between Entry & TP1**\n"
            "   Price is in profit but hasn't reached the first take profit level yet\n\n"
            "🟢 = **Between TP1 & TP2**\n"
            "   Price has passed TP1 and is now targeting TP2\n\n"
            "💚 = **Between TP2 & TP3**\n"
            "   Price has passed TP2 (breakeven is now active) and is targeting TP3\n\n"
            "🚀 = **Max Profit (Beyond TP3)**\n"
            "   Price has exceeded all take profit levels - maximum profit achieved!")
    
    await message.reply(guide)

def analyze_trade_position(action: str, entry: float, tp1: float,
                           tp2: float, tp3: float, sl: float,
                           current_price: float) -> dict:
    if action == "BUY":
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
        if current_price >= sl:
            return {"emoji": "🔴", "position": "At/Above SL"}
        elif current_price >= entry:
            return {"emoji": "🟡", "position": "Above Entry"}
        elif current_price >= tp1:
            return {"emoji": "🟠", "position": "Between Entry and TP1"}
        elif current_price <= tp2: # Corrected comparison for SELL
            return {"emoji": "🟢", "position": "Between TP1 and TP2"}
        elif current_price <= tp3: # Corrected comparison for SELL
            return {"emoji": "💚", "position": "Between TP2 and TP3"}
        else:
            return {"emoji": "🚀", "position": "Below TP3 - Max Profit"}

async def handle_activetrades_callback(bot_instance, client: Client, callback_query):
    """Handle activetrades list navigation callbacks"""
    data = callback_query.data
    if data == "at_refresh":
        await handle_active_trades(bot_instance, client, callback_query.message)
        await callback_query.answer("Refreshed")
    elif data == "at_close":
        await callback_query.message.edit_text("❌ Closed.")
        await callback_query.answer()
