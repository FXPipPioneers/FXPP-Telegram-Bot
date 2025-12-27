from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import CallbackQuery, Message
import logging
from src.features.core.config import VIP_GROUP_ID, FREE_GROUP_ID, BOT_OWNER_USER_ID

logger = logging.getLogger(__name__)

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
