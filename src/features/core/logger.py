import logging
import os
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
            # Ensure DEBUG_GROUP_ID is a negative integer if possible
            target_id = DEBUG_GROUP_ID
            if isinstance(target_id, str):
                try:
                    target_id = int(target_id)
                except:
                    pass

            if not self.app.is_connected:
                logger.warning("Bot client not connected. Skipping debug log.")
                return

            if is_error:
                header = "🚨 **SYSTEM ERROR**"
                footer = f"\n\n{self.bot_owner_username}"
            else:
                header = "📊 **SYSTEM LOG**"
                footer = ""

            msg_text = f"{header}\n\n**Event:** {message}{footer}"
            
            keyboard: InlineKeyboardMarkup | None = None
            if user_id:
                msg_text += f"\n\n👤 **User ID:** `{user_id}`"
                # Deep link for user profile
                buttons = [[InlineKeyboardButton("👤 View User Profile", url=f"tg://user?id={user_id}")]]
                
                if is_error:
                    # Pre-filled error recovery message text for deep link
                    recovery_text = "Hello! I noticed an issue with your trial setup. Can you please message me so I can fix it for you?"
                    buttons.append([InlineKeyboardButton("💬 Open DM with User", url=f"tg://msg?to={user_id}&text={recovery_text}")])
                
                keyboard = InlineKeyboardMarkup(buttons)

            await self.app.send_message(
                target_id, 
                msg_text, 
                reply_markup=keyboard # type: ignore
            )
            # Also log to console for standard traceability
            if is_error:
                logger.error(message)
            else:
                logger.info(message)

        except Exception as e:
            logger.error(f"Failed to send debug log: {e}")

    async def log_system_status(self, loop_name: str, status: str):
        """Standardized loop status logging for interval visibility"""
        await self.log_to_debug(f"⏳ **{loop_name}**: {status}")
