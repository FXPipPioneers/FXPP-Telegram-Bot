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

    async def log_startup_report(self, stats: dict):
        """Send a detailed startup report with system status indicators"""
        report = (
            "🚀 **FX Pip Pioneers - System Online**\n\n"
            f"✅ **Database**: {stats.get('db', 'Connected')}\n"
            f"✅ **Owner**: {stats.get('owner', 'Verified')}\n"
            f"✅ **Loops**: {stats.get('loops', '8/8 Active')}\n"
            f"✅ **Health**: {stats.get('health', 'Port 5000')}\n\n"
            "✨ *Bot is fully operational and ready for signals.*"
        )
        await self.log_to_debug(report)

    async def log_to_debug(self, message: str, is_error: bool = False, user_id: int | None = None):
        """Log message to debug group with professional formatting and standardized headers"""
        from src.features.core.config import DEBUG_GROUP_ID
        if not DEBUG_GROUP_ID:
            logger.info(f"Debug logging skipped (no group ID): {message}")
            return

        try:
            raw_id = DEBUG_GROUP_ID
            if isinstance(raw_id, str):
                target_id = raw_id
            else:
                target_id = int(raw_id)

            # Standardized headers
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

            # Send function with built-in retry logic
            async def send_with_retry():
                try:
                    # Explicitly attempt to resolve chat peer before first send if it's a channel/group
                    if str(target_id).startswith("-100"):
                        try:
                            await self.app.get_chat(target_id)
                        except Exception:
                            pass
                    return await self.app.send_message(target_id, msg_text, reply_markup=keyboard) # type: ignore
                except Exception as e:
                    if any(err in str(e) for err in ["CHAT_ID_INVALID", "PEER_ID_INVALID"]):
                        logger.info(f"Resolving chat {target_id} before retry...")
                        try:
                            await self.app.get_chat(target_id)
                            return await self.app.send_message(target_id, msg_text, reply_markup=keyboard) # type: ignore
                        except Exception as retry_err:
                            # If still failing, suppress the noisy error but log the result
                            logger.debug(f"Resolution retry failed: {retry_err}")
                            raise
                    raise

            # Use a short timeout for the message send to avoid hanging
            try:
                await asyncio.wait_for(send_with_retry(), timeout=15)
                logger.info(f"Successfully sent debug log to {target_id}")
            except Exception:
                # Silently catch to keep the console clean of Telegram's transient chat resolution errors
                pass

        except Exception as e:
            logger.error(f"Internal logger error: {e}")

    async def log_system_status(self, loop_name: str, status: str):
        """Standardized loop status logging for interval visibility"""
        await self.log_to_debug(f"⏳ **{loop_name}**: {status}")
