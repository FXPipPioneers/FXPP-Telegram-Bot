import logging
import os
import sys
import asyncio

# Ensure project root is in PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from aiohttp import web
from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message, CallbackQuery, BotCommand, BotCommandScopeChat
from src.features.core.config import TELEGRAM_BOT_TOKEN, TELEGRAM_API_ID, TELEGRAM_API_HASH, BOT_OWNER_USER_ID, AMSTERDAM_TZ, PRICE_TRACKING_CONFIG
from src.features.database.manager import DatabaseManager
from src.features.core.engine import BackgroundEngine
from src.features.community.handlers import register_community_handlers
from src.features.trading.handlers import register_trading_handlers

# Import modularized commands
from src.features.commands.entry import handle_entry, handle_entry_callback
from src.features.commands.active_trades import handle_active_trades, show_active_trades_list, show_position_guide, analyze_trade_position, handle_activetrades_callback
from src.features.commands.trade_override import handle_trade_override, handle_override_callback
from src.features.commands.price_test import handle_price_test, handle_pricetest_callback
from src.features.commands.status import handle_db_status, handle_dm_status
from src.features.commands.free_trial import handle_timed_auto_role, handle_timedautorole_callback
from src.features.commands.send_welcome_dm import handle_send_welcome_dm, handle_sendwelcomedm_callback
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
            api_hash=TELEGRAM_API_HASH,
            bot_token=TELEGRAM_BOT_TOKEN
        )
        self.db = DatabaseManager()
        self.db_pool = None # Added to fix AttributeError
        self.engine: CoreBackgroundEngine | None = None # Will be initialized in start_bot
        self.tracker = PriceTracker(self)
        self.logger_tool = DebugLogger(self)
        self.awaiting_price_input = {}
        self.awaiting_custom_pair = {}
        self.override_trade_mappings = {}
        self.pending_multi_select = {}

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

    async def start_bot(self):
        # Start health check server
        await self.start_health_check()
        
        # Start bot client FIRST to ensure it's connected
        logging.info("Starting bot client...")
        await self.start()
        
        await self.db.connect()
        self.db_pool = self.db.pool # Expose pool for tracker and engine
        # Initialize engine after DB connect
        self.db.bot = self
        self.engine = CoreBackgroundEngine(self.db, self)
        
        # Register Handlers
        register_community_handlers(self, self.db.pool, self)
        register_trading_handlers(self, self.db.pool)
        
        # Register Modularized Commands
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
            await handle_db_status(self, client, message)

        @self.on_message(filters.command("dmstatus"))
        async def _dmstatus(client, message):
            await handle_dm_status(self, client, message)

        @self.on_message(filters.command("freetrialusers"))
        async def _freetrial(client, message):
            await handle_timed_auto_role(client, message, self.is_owner)

        @self.on_message(filters.command("sendwelcomedm"))
        async def _swdm(client, message):
            await handle_send_welcome_dm(client, message, self.is_owner, self)

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

        # Set bot commands menu
        try:
            if not self.is_connected:
                await self.start()
            
            await self.set_bot_commands([
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
            ], scope=BotCommandScopeChat(chat_id=BOT_OWNER_USER_ID))
        except Exception as e:
            logging.error(f"Failed to set bot commands: {e}")

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

        @self.on_callback_query(filters.regex("^tar_"))
        async def _tar_cb(client, cb):
            await handle_timedautorole_callback(client, cb, self.is_owner)

        @self.on_callback_query(filters.regex("^swdm_"))
        async def _swdm_cb(client, cb):
            await handle_sendwelcomedm_callback(client, cb, self.is_owner, self)

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

        @self.on_callback_query(filters.regex("^at_"))
        async def _at_cb(client, cb):
            await handle_activetrades_callback(self, client, cb)

        # Helper methods required by commands
        self.show_active_trades_list = lambda msg: show_active_trades_list(self, msg)
        self.show_position_guide = lambda msg: show_position_guide(self, msg)
        self.analyze_trade_position = analyze_trade_position
        
    async def start_bot_complete(self):
        """Start all background tasks after bot initialization"""
        # Start background tasks
        from src.features.community.scheduler import dm_scheduler_task
        asyncio.create_task(dm_scheduler_task(self))
        
        await self.log_to_debug("🚀 **Trading Bot Initialized** - All systems operational.")
        
        if self.engine:
            try:
                await self.engine.start()
            except Exception as e:
                logging.error(f"Failed to start BackgroundEngine: {e}")
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
