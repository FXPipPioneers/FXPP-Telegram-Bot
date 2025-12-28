import logging
import os
import asyncio
from datetime import datetime
from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from src.features.core.config import DEBUG_GROUP_ID, BOT_OWNER_USER_ID

# Standard logging to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DebugLogger:
    def __init__(self, app: Client, bot_owner_username: str = "@fx_pippioneers"):
        self.app = app
        self.bot_owner_username = bot_owner_username

    async def log_to_debug(self, message: str, is_error: bool = False, user_id: int | None = None):
        """Log message to debug group with professional formatting and standardized headers"""
        from src.features.core.config import DEBUG_GROUP_ID
        if not DEBUG_GROUP_ID:
            logger.info(f"Debug logging skipped (no group ID): {message}")
            return

        try:
            target_id = int(DEBUG_GROUP_ID)

            # Ensure client is connected before sending
            if not self.app.is_connected:
                try:
                    await self.app.start()
                    logger.info("Bot client connected via logger")
                except Exception as e:
                    logger.error(f"Failed to connect bot client in logger: {e}")
                    return

            # Resolve the chat peer if it's a channel/group ID to avoid CHAT_ID_INVALID
            try:
                await self.app.get_chat(target_id)
            except Exception as e:
                logger.warning(f"Could not resolve chat {target_id} before sending: {e}")

            header = "🚨 **SYSTEM ERROR**" if is_error else "📊 **SYSTEM LOG**"
            footer = f"\n\n{self.bot_owner_username}" if is_error else ""
            msg_text = f"{header}\n\n**Event:** {message}{footer}"
            
            keyboard: InlineKeyboardMarkup | None = None
            if user_id:
                msg_text += f"\n\n👤 **User ID:** `{user_id}`"
                buttons = [[InlineKeyboardButton("👤 View User Profile", url=f"tg://user?id={user_id}")]]
                if is_error:
                    recovery_text = "Hello! I noticed an issue with your trial setup. Can you please message me so I can fix it for you?"
                    buttons.append([InlineKeyboardButton("💬 Open DM with User", url=f"tg://msg?to={user_id}&text={recovery_text}")])
                keyboard = InlineKeyboardMarkup(buttons)

            # Use a short timeout for the message send to avoid hanging
            try:
                await asyncio.wait_for(
                    self.app.send_message(
                        target_id, 
                        msg_text, 
                        reply_markup=keyboard # type: ignore
                    ),
                    timeout=15
                )
                logger.info(f"Successfully sent debug log to {target_id}")
            except asyncio.TimeoutError:
                logger.error(f"Timeout sending debug log to {target_id}")
            except Exception as e:
                logger.error(f"Telegram API Error sending to debug group {target_id}: {e}")

        except Exception as e:
            logger.error(f"Internal logger error: {e}")

    async def log_system_status(self, loop_name: str, status: str):
        """Standardized loop status logging for interval visibility"""
        await self.log_to_debug(f"⏳ **{loop_name}**: {status}")
