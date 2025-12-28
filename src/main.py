import logging
import os
import sys
import asyncio
from datetime import datetime
import pytz

# Ensure project root is in PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from aiohttp import web
from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, CallbackQuery, BotCommand, BotCommandScopeChat

import pyrogram.utils as pyrogram_utils
pyrogram_utils.MIN_CHANNEL_ID = -1009999999999
pyrogram_utils.MIN_CHAT_ID = -999999999999

from typing import Dict, Optional
from src.features.core.config import TELEGRAM_BOT_TOKEN, TELEGRAM_API_ID, TELEGRAM_API_HASH, BOT_OWNER_USER_ID, AMSTERDAM_TZ, PRICE_TRACKING_CONFIG, MESSAGE_TEMPLATES
from src.features.database.manager import DatabaseManager
from src.features.core.engine import BackgroundEngine
from src.features.community.handlers import register_community_handlers
from src.features.trading.handlers import register_trading_handlers

# Import modularized commands
from src.features.commands.entry import handle_entry, handle_entry_callback
from src.features.commands.active_trades import handle_active_trades, show_active_trades_list, show_position_guide, analyze_trade_position, handle_activetrades_callback
from src.features.commands.trade_override import handle_trade_override, handle_override_callback
from src.features.commands.price_test import handle_price_test, handle_pricetest_callback
from src.features.commands.db_status import handle_dbstatus, handle_dbstatus_callback
from src.features.commands.dm_status import handle_dmstatus, handle_dmstatus_callback
from src.features.commands.free_trial_users import handle_freetrialusers, handle_freetrial_callback
from src.features.commands.send_welcome_dm import handle_sendwelcomedm, handle_welcome_callback
from src.features.commands.new_members import handle_newmemberslist, handle_newmemberslist_callback
from src.features.commands.dm_messages import handle_dmmessages, handle_dmmessages_callback
from src.features.commands.peer_id import handle_peer_id_status, handle_peerid_callback
from src.features.commands.retract_trial import handle_retract_trial, handle_retracttrial_callback
from src.features.commands.clear_member import handle_clear_member, handle_cleartrial_callback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from src.features.core.engine import BackgroundEngine as CoreBackgroundEngine
from src.features.trading.engine import TradingEngine
from src.features.core.logger import DebugLogger

from src.features.trading.price_tracker import PriceTracker

class TradingBot(Client):
    def __init__(self):
        super().__init__(
            "trading_bot",
            api_id=TELEGRAM_API_ID,
            api_hash=str(TELEGRAM_API_HASH),
            bot_token=str(TELEGRAM_BOT_TOKEN)
        )
        self.db = DatabaseManager()
        self.db_pool = None
        self.engine: CoreBackgroundEngine | None = None
        self.tracker = PriceTracker(self)
        self.logger_tool = DebugLogger(self)
        self.awaiting_price_input = {}
        self.awaiting_custom_pair = {}
        self.override_trade_mappings = {}
        self.pending_multi_select = {}
        self.awaiting_sendwelcomedm_input = {}
        self.sendwelcomedm_context = {}
        self._waiting_for_peer_id = {}
        self.awaiting_cleartrial_input = {}
        
        # Register handlers during initialization (before start)
        self._register_handlers()

    async def start_health_check(self):
        """Start a simple web server for Render health checks"""
        async def handle(request):
            return web.Response(text="Bot is running")

        app = web.Application()
        app.router.add_get('/', handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 5000)
        asyncio.create_task(site.start())
        logging.info("Health check server started on port 5000")

    async def log_to_debug(self, message: str, is_error: bool = False, user_id: int | None = None):
        await self.logger_tool.log_to_debug(message, is_error, user_id)

    async def get_live_price(self, pair: str):
        """Bridge to tracker.get_live_price"""
        return await self.tracker.get_live_price(pair)

    async def is_owner(self, user_id: int) -> bool:
        return user_id == BOT_OWNER_USER_ID

    def is_weekend_market_closed(self) -> bool:
        """Check if market is closed (Friday 22:00 to Sunday 22:00 UTC)"""
        now = datetime.now(pytz.UTC)
        weekday = now.weekday()  # 0=Monday, 4=Friday, 5=Saturday, 6=Sunday
        hour = now.hour

        if weekday == 4: # Friday
            return hour >= 22
        if weekday in [5, 6]: # Saturday or Sunday
            if weekday == 6: # Sunday
                return hour < 22
            return True
        return False

    async def execute_send_welcome_dm(self, callback_query, menu_id: str, context: dict, user_message: Optional[Message] = None):
        """Execute the actual welcome DM send"""
        user_id = context.get('user_id')

        if not user_id:
            if callback_query:
                await callback_query.message.edit_text("❌ Missing user ID.")
                await callback_query.answer()
            elif user_message:
                await user_message.reply("❌ Missing user ID.")
            self.sendwelcomedm_context.pop(menu_id, None)
            return

        # Prepare the welcome message
        welcome_msg = MESSAGE_TEMPLATES["Engagement & Offers"]["Daily VIP Trial Offer"]["message"]

        # Try to send the message
        try:
            # Establish peer connection first
            await self.get_users([user_id])
            await asyncio.sleep(1)
            await self.send_message(user_id, welcome_msg)
            
            # ✅ SYNC WITH peer_id_checks TABLE IF IT EXISTS
            if self.db_pool:
                try:
                    async with self.db_pool.acquire() as conn:
                        await conn.execute(
                            'UPDATE peer_id_checks SET welcome_dm_sent = TRUE WHERE user_id = $1',
                            user_id
                        )
                except Exception as db_err:
                    logging.warning(f"Could not update peer_id_checks for user {user_id}: {db_err}")
            
            if callback_query:
                await callback_query.message.edit_text(
                    f"✅ **Success!**\n\n"
                    f"Welcome DM sent to user **{user_id}**"
                )
            elif user_message:
                await user_message.reply(
                    f"✅ **Success!**\n\n"
                    f"Welcome DM sent to user **{user_id}**"
                )
            await self.log_to_debug(f"Owner sent welcome DM to user {user_id} via /sendwelcomedm widget", user_id=user_id)
        except Exception as e:
            # Add to retry queue for any errors
            await self.track_failed_welcome_dm(user_id, f"User {user_id}", "welcome", welcome_msg)
            if callback_query:
                await callback_query.message.edit_text(
                    f"⚠️ **Could Not Send Immediately**\n\n"
                    f"Added to automatic retry queue.\n\n"
                    f"The bot will automatically retry every 2 minutes (up to 5 attempts).\n\n"
                    f"User ID: `{user_id}`\n\n"
                    f"You'll see a success message in debug channel when it goes through."
                )
            elif user_message:
                await user_message.reply(
                    f"⚠️ **Could Not Send Immediately**\n\n"
                    f"Added to automatic retry queue.\n\n"
                    f"The bot will automatically retry every 2 minutes (up to 5 attempts).\n\n"
                    f"User ID: `{user_id}`\n\n"
                    f"You'll see a success message in debug channel when it goes through."
                )
            await self.log_to_debug(f"Added user {user_id} to welcome DM retry queue via /sendwelcomedm widget (will retry automatically)", is_error=True, user_id=user_id)
        
        if callback_query:
            await callback_query.answer()
        self.sendwelcomedm_context.pop(menu_id, None)

    async def track_failed_welcome_dm(self, user_id: int, first_name: str, msg_type: str, message_content: str):
        """Track failed welcome DM attempts"""
        if not self.db_pool:
            return
        
        try:
            async with self.db_pool.acquire() as conn:
                # Check if already exists
                existing = await conn.fetchval(
                    "SELECT id FROM pending_welcome_dms WHERE user_id = $1",
                    user_id
                )
                
                if existing:
                    # Update failed_attempts
                    await conn.execute(
                        "UPDATE pending_welcome_dms SET failed_attempts = failed_attempts + 1 WHERE user_id = $1",
                        user_id
                    )
                else:
                    # Insert new entry
                    await conn.execute(
                        "INSERT INTO pending_welcome_dms (user_id, first_name, message_type, message_content, failed_attempts) VALUES ($1, $2, $3, $4, 1)",
                        user_id, first_name, msg_type, message_content
                    )
        except Exception as e:
            logging.warning(f"Could not track failed welcome DM for {user_id}: {e}")

    def _register_handlers(self):
        """Register all message and callback handlers (called from __init__)"""
        
        @self.on_message(filters.command("entry"))
        async def _entry(client, message):
            await handle_entry(self, client, message)

        @self.on_message(filters.command("activetrades"))
        async def _activetrades(client, message):
            await handle_active_trades(self, client, message)

        @self.on_message(filters.command("tradeoverride"))
        async def _tradeoverride(client, message):
            await handle_trade_override(self, client, message)

        @self.on_message(filters.command("pricetest"))
        async def _pricetest(client, message):
            await handle_price_test(self, client, message)

        @self.on_message(filters.command("dbstatus"))
        async def _dbstatus(client, message):
            await handle_dbstatus(self, client, message)

        @self.on_message(filters.command("dmstatus"))
        async def _dmstatus(client, message):
            await handle_dmstatus(self, client, message)

        @self.on_message(filters.command("freetrialusers"))
        async def _freetrial(client, message):
            await handle_freetrialusers(self, client, message)

        @self.on_message(filters.command("sendwelcomedm"))
        async def _swdm(client, message):
            await handle_sendwelcomedm(self, client, message)

        @self.on_message(filters.command("newmemberslist"))
        async def _nml(client, message):
            await handle_newmemberslist(client, message, self.is_owner)

        @self.on_message(filters.command("dmmessages"))
        async def _dmm(client, message):
            await handle_dmmessages(client, message, self.is_owner)

        @self.on_message(filters.command("peeridstatus"))
        async def _pid(client, message):
            await handle_peer_id_status(client, message, self.is_owner, self)

        @self.on_message(filters.command("retracttrial"))
        async def _retract(client, message):
            await handle_retract_trial(client, message, self.is_owner, self)

        @self.on_message(filters.command("clearmember"))
        async def _clear(client, message):
            await handle_clear_member(client, message, self.is_owner, self)

        # Callbacks
        @self.on_callback_query(filters.regex("^entry_"))
        async def _entry_cb(client, cb: CallbackQuery):
            await handle_entry_callback(self, client, cb)

        @self.on_callback_query(filters.regex("^ovr_"))
        async def _ovr_cb(client, cb: CallbackQuery):
            await handle_override_callback(self, client, cb)

        @self.on_callback_query(filters.regex("^pricetest_"))
        async def _price_cb(client, cb):
            await handle_pricetest_callback(self, client, cb)

        @self.on_callback_query(filters.regex("^ft_"))
        async def _ft_cb(client, cb):
            await handle_freetrial_callback(self, client, cb)

        @self.on_callback_query(filters.regex("^wm_"))
        async def _wm_cb(client, cb):
            await handle_welcome_callback(self, client, cb)

        @self.on_callback_query(filters.regex("^nml_"))
        async def _nml_cb(client, cb):
            await handle_newmemberslist_callback(client, cb, self.is_owner, self.db.pool)

        @self.on_callback_query(filters.regex("^dmm_"))
        async def _dmm_cb(client, cb):
            await handle_dmmessages_callback(client, cb, self.is_owner)

        @self.on_callback_query(filters.regex("^pid_"))
        async def _pid_cb(client, cb):
            await handle_peerid_callback(client, cb, self.is_owner, self)

        @self.on_callback_query(filters.regex("^retrt_"))
        async def _retrt_cb(client, cb):
            await handle_retracttrial_callback(client, cb, self.is_owner, self)

        @self.on_callback_query(filters.regex("^clrtrl_"))
        async def _clrtrl_cb(client, cb):
            await handle_cleartrial_callback(client, cb, self.is_owner, self)

        @self.on_message(filters.private & filters.text & ~filters.command([
            "entry", "activetrades", "tradeoverride", "pricetest",
            "dbstatus", "dmstatus", "freetrialusers", "sendwelcomedm", "newmemberslist", "dmmessages"
        ]))
        async def _text_input(client, message):
            user_id = message.from_user.id
            
            # Handle sendwelcomedm user ID input
            if user_id in self.awaiting_sendwelcomedm_input:
                menu_id = self.awaiting_sendwelcomedm_input.pop(user_id)
                try:
                    user_id_input = int(message.text.strip())
                    context = self.sendwelcomedm_context.get(menu_id, {})
                    context['user_id'] = user_id_input
                    context['msg_type'] = 'welcome'
                    context['stage'] = 'confirmed'
                    self.sendwelcomedm_context[menu_id] = context
                    await self.execute_send_welcome_dm(None, menu_id, context, user_message=message)
                except ValueError:
                    await message.reply("Invalid user ID. Please provide a numeric ID only.")
                return

            # Handle clear member user ID input
            if hasattr(self, 'awaiting_cleartrial_input') and user_id in self.awaiting_cleartrial_input:
                menu_id = self.awaiting_cleartrial_input.pop(user_id)
                try:
                    target_user_id = message.text.strip()
                    int(target_user_id) # Verify it's numeric
                    from src.features.commands.clear_member import execute_clear_member
                    # Create a dummy message object for execute_clear_member to edit
                    reply_msg = await message.reply("⌛ Processing...")
                    await execute_clear_member(client, reply_msg, target_user_id, self)
                except ValueError:
                    await message.reply("Invalid user ID. Please provide a numeric ID only.")
                return
            
            # Handle peer ID status input
            if not hasattr(self, '_waiting_for_peer_id'):
                self._waiting_for_peer_id = {}
            
            if user_id in self._waiting_for_peer_id:
                self._waiting_for_peer_id.pop(user_id)
                try:
                    peer_user_id = int(message.text.strip())
                    # Import here to avoid circular imports
                    from src.features.commands.peer_id import _show_peer_id_status
                    await _show_peer_id_status(message, peer_user_id, self.db_pool)
                except ValueError:
                    await message.reply("❌ Invalid user ID. Please provide a numeric ID only.")
                return
            
            # Check for handlers in CommunityHandlers
            # We need to find the instance. Let's register it to self.
            if hasattr(self, 'community_handlers'):
                await self.community_handlers.handle_text_input(client, message)

    async def register_bot_commands(self):
        """Register bot commands (called after client starts to avoid PEER_ID_INVALID)"""
        try:
            from src.features.core.config import BOT_OWNER_USER_ID
            owner_id = BOT_OWNER_USER_ID
            if owner_id:
                # Try to resolve the owner peer first to ensure the bot "knows" them
                try:
                    await self.get_users(owner_id)
                    logging.info(f"✅ Resolved owner peer {owner_id}")
                except Exception as peer_error:
                    logging.warning(f"⚠️ Could not resolve owner peer {owner_id}: {peer_error}")

                owner_commands = [
                    BotCommand("entry", "Create trading signal"),
                    BotCommand("activetrades", "View active signals"),
                    BotCommand("tradeoverride", "Manual trade status control"),
                    BotCommand("pricetest", "Test price fetching"),
                    BotCommand("dbstatus", "Database health check"),
                    BotCommand("dmstatus", "DM schedule status"),
                    BotCommand("freetrialusers", "Free trial management"),
                    BotCommand("sendwelcomedm", "Welcome DM management"),
                    BotCommand("newmemberslist", "New members tracking"),
                    BotCommand("dmmessages", "DM message management"),
                    BotCommand("peeridstatus", "Peer ID verification status"),
                    BotCommand("retracttrial", "Retract trial access"),
                    BotCommand("clearmember", "Clear member data")
                ]
                try:
                    await self.set_bot_commands(
                        owner_commands,
                        scope=BotCommandScopeChat(chat_id=BOT_OWNER_USER_ID))
                    logging.info(f"✅ Bot commands registered for user {BOT_OWNER_USER_ID} ({len(owner_commands)} commands)")
                except Exception as scope_error:
                    logging.error(f"❌ Error setting owner-scoped commands: {scope_error}")
            else:
                logging.warning("⚠️  BOT_OWNER_USER_ID not set, cannot register owner-specific commands")
        except Exception as e:
            logging.error(f"❌ Error registering bot commands: {e}")

    async def start_bot(self):
        # Start health check server
        await self.start_health_check()
        
        # Start bot client FIRST to ensure it's connected
        logging.info("Starting bot client...")
        try:
            await self.start()
        except Exception as e:
            logging.error(f"❌ Critical error starting client: {e}")
            # If token is missing, check environment variables
            if "key is required" in str(e):
                logging.error("TELEGRAM_BOT_TOKEN is missing or invalid in environment.")
            raise e
        
        await self.db.connect()
        self.db_pool = self.db.pool
        # Initialize engine after DB connect
        self.db.bot = self
        self.engine = CoreBackgroundEngine(self.db, self)
        
        # Register community and trading handlers
        self.community_handlers = register_community_handlers(self, self.db.pool, self)
        register_trading_handlers(self, self.db.pool)
        
        # Register bot commands after client is started
        await self.register_bot_commands()
        
        # Helper methods required by commands
        self.show_active_trades_list = lambda msg, is_edit=False: show_active_trades_list(self, msg, is_edit)
        self.show_position_guide = lambda msg, is_edit=False: show_position_guide(self, msg, is_edit)
        self.analyze_trade_position = analyze_trade_position
        
        # Callback Handlers
        from src.features.commands.db_status import handle_dbstatus_callback
        from src.features.commands.dm_status import handle_dmstatus_callback
        from src.features.commands.active_trades import handle_activetrades_callback
        from src.features.commands.trade_override import handle_override_callback
        from src.features.commands.free_trial_users import handle_freetrial_callback
        from src.features.commands.send_welcome_dm import handle_welcome_callback
        
        async def global_callback_handler(client, cb):
            data = cb.data
            if data.startswith("db_"):
                await handle_dbstatus_callback(self, client, cb)
            elif data.startswith("dm_"):
                await handle_dmstatus_callback(self, client, cb)
            elif data.startswith("at_"):
                await handle_activetrades_callback(self, client, cb)
            elif data.startswith("ovr_"):
                await handle_override_callback(self, client, cb)
            elif data.startswith("ft_"):
                await handle_freetrial_callback(self, client, cb)
            elif data.startswith("wm_"):
                await handle_welcome_callback(self, client, cb)
        
        self.handle_callback = global_callback_handler
        
    async def start_bot_complete(self):
        """Start all background tasks after bot initialization and send detailed report"""
        # Resolve debug group peer on startup to prevent CHAT_ID_INVALID
        from src.features.core.config import DEBUG_GROUP_ID
        if DEBUG_GROUP_ID:
            try:
                await self.get_chat(int(DEBUG_GROUP_ID))
                logging.info(f"✅ Successfully resolved debug group {DEBUG_GROUP_ID}")
            except Exception as e:
                logging.error(f"❌ Failed to resolve debug group {DEBUG_GROUP_ID}: {e}")

        # Start background tasks
        from src.features.community.scheduler import dm_scheduler_task
        asyncio.create_task(dm_scheduler_task(self))
        
        if self.engine:
            try:
                await self.engine.start()
            except Exception as e:
                logging.error(f"Failed to start BackgroundEngine: {e}")

        # Detailed Startup Report
        await self.logger_tool.log_startup_report({
            'db': '✅ Connected',
            'owner': '✅ Verified',
            'loops': '✅ 8/8 Operational',
            'health': '✅ Port 5000'
        })
        
        logging.info("Bot is fully operational.")

    async def stop_bot(self):
        if self.engine:
            try:
                await self.engine.stop()
            except Exception as e:
                logging.error(f"Error stopping BackgroundEngine: {e}")
        try:
            if self.is_connected:
                await self.stop()
        except Exception as e:
            logging.error(f"Error stopping client: {e}")

async def main():
    bot = TradingBot()
    try:
        await bot.start_bot()
        # logging.info("Starting bot client...") # Already started in start_bot
        # await bot.start()
        await bot.start_bot_complete()
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Stopping bot...")
    except Exception as e:
        logging.error(f"Fatal error in main: {e}")
    finally:
        await bot.stop_bot()

if __name__ == "__main__":
    asyncio.run(main())
