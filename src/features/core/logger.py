import logging
import os
import asyncio
import time
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
        self.startup_time = time.time()
        self.delay_seconds = 60 # 1 minute delay
        self.log_queue = []
        self.is_flushing = False

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

        # Check if we are within the startup delay period
        elapsed = time.time() - self.startup_time
        if elapsed < self.delay_seconds:
            wait_remaining = int(self.delay_seconds - elapsed)
            logger.info(f"Debug log queued (waiting {wait_remaining}s for Telegram sync): {message[:50]}...")
            self.log_queue.append((message, is_error, user_id))
            
            # Start a single flusher if not already running
            if not self.is_flushing:
                self.is_flushing = True
                asyncio.create_task(self._flush_queue_after_delay(wait_remaining))
            return

        await self._execute_log(message, is_error, user_id)

    async def _flush_queue_after_delay(self, delay: int):
        """Wait for the startup delay to expire and then flush all queued logs"""
        await asyncio.sleep(delay)
        while self.log_queue:
            msg, is_err, uid = self.log_queue.pop(0)
            await self._execute_log(msg, is_err, uid)
            await asyncio.sleep(1) # Rate limit protection
        self.is_flushing = False

    async def _execute_log(self, message: str, is_error: bool = False, user_id: int | None = None):
        """Actual log execution logic"""
        from src.features.core.config import DEBUG_GROUP_ID
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
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        # Explicitly attempt to resolve chat peer before first send
                        if str(target_id).startswith("-100"):
                            try:
                                await self.app.get_chat(target_id)
                            except Exception:
                                pass
                        return await self.app.send_message(target_id, msg_text, reply_markup=keyboard) # type: ignore
                    except Exception as e:
                        if any(err in str(e) for err in ["CHAT_ID_INVALID", "PEER_ID_INVALID"]) and attempt < max_retries - 1:
                            logger.info(f"Attempt {attempt+1}: Resolving chat {target_id} before retry...")
                            await asyncio.sleep(5)
                            try:
                                await self.app.get_chat(target_id)
                            except: pass
                        else:
                            raise e
                return None

            # Use a short timeout for the message send to avoid hanging
            try:
                await asyncio.wait_for(send_with_retry(), timeout=20)
                logger.info(f"Successfully sent debug log to {target_id}")
            except Exception:
                # Silently catch to keep the console clean
                pass

        except Exception as e:
            logger.error(f"Internal logger error: {e}")

    async def log_system_status(self, loop_name: str, status: str):
        """Standardized loop status logging for interval visibility"""
        await self.log_to_debug(f"⏳ **{loop_name}**: {status}")
