from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import CallbackQuery, Message
import logging
from src.features.core.config import VIP_GROUP_ID, FREE_GROUP_ID, BOT_OWNER_USER_ID

logger = logging.getLogger(__name__)

async def send_tp_notification(client, message_id: str, trade_data: dict, tp_level: str, price: float, bot):
    """Notify groups about a TP hit"""
    pair = trade_data.get('pair', 'Unknown')
    action = trade_data.get('action', 'Unknown')
    group_id = trade_data.get('chat_id')
    
    msg = f"🎯 **{pair} {action} - {tp_level} HIT!**\n\nPrice: `{price:.5f}`\n\n@fx_pippioneers"
    try:
        if group_id:
            await client.send_message(group_id, msg)
        await bot.log_to_debug(f"✅ TP Notification sent for {pair} {tp_level}")
    except Exception as e:
        logger.error(f"Error sending TP notification: {e}")

async def send_sl_notification(client, message_id: str, trade_data: dict, price: float, bot):
    """Notify groups about a SL hit"""
    pair = trade_data.get('pair', 'Unknown')
    action = trade_data.get('action', 'Unknown')
    group_id = trade_data.get('chat_id')
    
    msg = f"🛑 **{pair} {action} - STOP LOSS HIT**\n\nPrice: `{price:.5f}`\n\n@fx_pippioneers"
    try:
        if group_id:
            await client.send_message(group_id, msg)
        await bot.log_to_debug(f"🚨 SL Notification sent for {pair}")
    except Exception as e:
        logger.error(f"Error sending SL notification: {e}")

async def send_breakeven_notification(client, message_id: str, trade_data: dict, bot):
    """Notify groups about a breakeven hit"""
    pair = trade_data.get('pair', 'Unknown')
    action = trade_data.get('action', 'Unknown')
    group_id = trade_data.get('chat_id')
    
    msg = f"⚖️ **{pair} {action} - CLOSED AT BREAKEVEN**\n\nPrice returned to entry after TP2.\n\n@fx_pippioneers"
    try:
        if group_id:
            await client.send_message(group_id, msg)
        await bot.log_to_debug(f"⚖️ Breakeven Notification sent for {pair}")
    except Exception as e:
        logger.error(f"Error sending BE notification: {e}")

def register_trading_handlers(app, db_pool):
    """Register non-command trading event handlers"""
    
    @app.on_message(filters.group & ~filters.service)
    async def _group_msg(client, message: Message):
        """Handle manual signals in groups"""
        if message.chat.id not in [VIP_GROUP_ID, FREE_GROUP_ID]:
            return
            
        if not message.text or "Trade Signal For:" not in message.text:
            return
            
        # Only owner can trigger manual signals
        if message.from_user and message.from_user.id != BOT_OWNER_USER_ID:
            return
            
        try:
            from src.features.trading.engine import TradingEngine
            engine = TradingEngine(db_pool, app)
            await engine.handle_manual_signal(message.text, message.chat.id, message.id, app)
        except Exception as e:
            logger.error(f"Error processing manual signal in handler: {e}")
