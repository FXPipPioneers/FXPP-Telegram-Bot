"""
Telegram Trading Bot - Professional Signal Distribution System
Migrated from Discord to Telegram

DEPLOYMENT INFO:
- Hosted on Render.com web service (24/7 uptime)
- PostgreSQL database managed by Render
- Environment variables set in Render dashboard

Author: Advanced Trading Bot System
Version: Telegram Edition
"""

import os
import asyncio
import logging
import json
import random
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Union
import asyncpg
import aiohttp
from aiohttp import web, ClientTimeout
import requests
import re

from pyrogram import filters
from pyrogram.client import Client
from pyrogram.types import (Message, InlineKeyboardMarkup,
                            InlineKeyboardButton, ChatJoinRequest,
                            ChatMemberUpdated, BotCommand, BotCommandScopeChat,
                            CallbackQuery)
from pyrogram.enums import ChatMemberStatus, ChatType
from pyrogram.errors import FloodWait, UserNotParticipant, ChatAdminRequired

import pyrogram.utils as pyrogram_utils

pyrogram_utils.MIN_CHANNEL_ID = -1009999999999
pyrogram_utils.MIN_CHAT_ID = -999999999999

from dotenv import load_dotenv

import pytz

PYTZ_AVAILABLE = True
print("Pytz loaded - Full timezone support enabled")

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")


def safe_int(value: str, default: int = 0) -> int:
    try:
        return int(value) if value else default
    except (ValueError, TypeError):
        return default


TELEGRAM_API_ID = safe_int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")

BOT_OWNER_USER_ID = safe_int(os.getenv("BOT_OWNER_USER_ID", "0"))
FREE_GROUP_ID = safe_int(os.getenv("FREE_GROUP_ID", "0"))
VIP_GROUP_ID = safe_int(os.getenv("VIP_GROUP_ID", "0"))
DEBUG_GROUP_ID = safe_int(os.getenv("DEBUG_GROUP_ID", "0"))
FREE_GROUP_LINK = os.getenv("FREE_GROUP_LINK", "")
VIP_GROUP_LINK = os.getenv("VIP_GROUP_LINK", "")
VIP_TRIAL_INVITE_LINK = "https://t.me/+uM_Ug2wTKFpiMDVk"
WHOP_PURCHASE_LINK = "https://whop.com/gold-pioneer/gold-pioneer/"

if TELEGRAM_BOT_TOKEN:
    print(f"✅ Telegram bot token loaded")
if TELEGRAM_API_ID:
    print(f"✅ Telegram API ID loaded")
if TELEGRAM_API_HASH:
    print(f"✅ Telegram API Hash loaded")

# Always print owner ID for debugging
print(f"🔑 BOT_OWNER_USER_ID = {BOT_OWNER_USER_ID}")
if BOT_OWNER_USER_ID == 0:
    print(
        f"⚠️  WARNING: BOT_OWNER_USER_ID not set! Owner commands will not work. Check Render environment variables."
    )
else:
    print(f"✅ Bot owner commands enabled for user {BOT_OWNER_USER_ID}")

print(f"🔗 VIP Trial invite link configured: https://t.me/+uM_Ug2wTKFpiMDVk")
print(
    f"🔗 Whop purchase link configured: https://whop.com/gold-pioneer/gold-pioneer/"
)

AMSTERDAM_TZ = pytz.timezone('Europe/Amsterdam')

PAIR_CONFIG = {
    'XAUUSD': {
        'decimals': 2,
        'pip_value': 0.1,
        'name': 'Gold (XAU/USD)'
    },
    'GBPJPY': {
        'decimals': 3,
        'pip_value': 0.01,
        'name': 'GBP/JPY'
    },
    'GBPUSD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'GBP/USD'
    },
    'EURUSD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'EUR/USD'
    },
    'AUDUSD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'AUD/USD'
    },
    'NZDUSD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'NZD/USD'
    },
    'US100': {
        'decimals': 1,
        'pip_value': 1.0,
        'name': 'US100 (Nasdaq)'
    },
    'US500': {
        'decimals': 2,
        'pip_value': 0.1,
        'name': 'US500 (S&P 500)'
    },
    'GER40': {
        'decimals': 1,
        'pip_value': 1.0,
        'name': 'GER40 (DAX)'
    },
    'BTCUSD': {
        'decimals': 1,
        'pip_value': 10,
        'name': 'Bitcoin (BTC/USD)'
    },
    'GBPCHF': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'GBP/CHF'
    },
    'USDCHF': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'USD/CHF'
    },
    'CADCHF': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'CAD/CHF'
    },
    'AUDCHF': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'AUD/CHF'
    },
    'CHFJPY': {
        'decimals': 3,
        'pip_value': 0.01,
        'name': 'CHF/JPY'
    },
    'CADJPY': {
        'decimals': 3,
        'pip_value': 0.01,
        'name': 'CAD/JPY'
    },
    'AUDJPY': {
        'decimals': 3,
        'pip_value': 0.01,
        'name': 'AUD/JPY'
    },
    'USDCAD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'USD/CAD'
    },
    'GBPCAD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'GBP/CAD'
    },
    'EURCAD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'EUR/CAD'
    },
    'AUDCAD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'AUD/CAD'
    },
    'AUDNZD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'AUD/NZD'
    },
    'USDJPY': {
        'decimals': 3,
        'pip_value': 0.01,
        'name': 'USD/JPY'
    },
    'EURJPY': {
        'decimals': 3,
        'pip_value': 0.01,
        'name': 'EUR/JPY'
    },
    'NZDJPY': {
        'decimals': 3,
        'pip_value': 0.01,
        'name': 'NZD/JPY'
    },
    'EURGBP': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'EUR/GBP'
    },
    'EURAUD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'EUR/AUD'
    },
    'EURCHF': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'EUR/CHF'
    },
    'EURNZD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'EUR/NZD'
    },
    'GBPAUD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'GBP/AUD'
    },
    'GBPNZD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'GBP/NZD'
    },
    'NZDCAD': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'NZD/CAD'
    },
    'NZDCHF': {
        'decimals': 4,
        'pip_value': 0.0001,
        'name': 'NZD/CHF'
    },
}

EXCLUDED_FROM_TRACKING = ['XAUUSD', 'BTCUSD', 'GER40', 'US100']

AUTO_ROLE_CONFIG = {
    "enabled": True,
    "duration_hours": 72,
    "active_members": {},
    "role_history": {},
    "dm_schedule": {},
    "weekend_pending": {}
}

PRICE_TRACKING_CONFIG = {
    "enabled":
    True,
    "active_trades": {},
    "api_keys": {
        "currencybeacon_key": os.getenv("CURRENCYBEACON_KEY", ""),
        "exchangerate_api_key": os.getenv("EXCHANGERATE_API_KEY", ""),
        "currencylayer_key": os.getenv("CURRENCYLAYER_KEY", ""),
        "abstractapi_key": os.getenv("ABSTRACTAPI_KEY", "")
    },
    "api_endpoints": {
        "currencybeacon": "https://api.currencybeacon.com/v1/latest",
        "exchangerate_api": "https://v6.exchangerate-api.com/v6",
        "currencylayer": "https://api.currencylayer.com/live",
        "abstractapi": "https://exchange-rates.abstractapi.com/v1/live"
    },
    "api_priority_order":
    ["currencybeacon", "exchangerate_api", "currencylayer", "abstractapi"],
    "check_interval":
    120,
}

PENDING_ENTRIES = {}


class TelegramTradingBot:

    def __init__(self):
        if not TELEGRAM_BOT_TOKEN:
            raise ValueError(
                "TELEGRAM_BOT_TOKEN environment variable is required")
        if not TELEGRAM_API_ID:
            raise ValueError(
                "TELEGRAM_API_ID environment variable is required")
        if not TELEGRAM_API_HASH:
            raise ValueError(
                "TELEGRAM_API_HASH environment variable is required")

        self.app = Client("trading_bot",
                          api_id=TELEGRAM_API_ID,
                          api_hash=TELEGRAM_API_HASH,
                          bot_token=TELEGRAM_BOT_TOKEN)
        self.db_pool = None
        self.client_session = None
        self.last_online_time = None
        self.running = True
        self.awaiting_price_input = {}
        self.awaiting_custom_pair = {}
        self.override_trade_mappings = {}  # menu_id -> {idx: message_id}
        self.trial_pending_approvals = set(
        )  # Track user IDs approved for trial

        self._register_handlers()

    def _register_handlers(self):

        @self.app.on_message(filters.command("entry"))
        async def entry_command(client, message: Message):
            await self.handle_entry(client, message)

        @self.app.on_message(filters.command("activetrades"))
        async def active_trades_command(client, message: Message):
            await self.handle_active_trades(client, message)

        @self.app.on_message(filters.command("tradeoverride"))
        async def trade_override_command(client, message: Message):
            await self.handle_trade_override(client, message)

        @self.app.on_message(filters.command("pricetest"))
        async def price_test_command(client, message: Message):
            await self.handle_price_test(client, message)

        @self.app.on_message(filters.command("dbstatus"))
        async def db_status_command(client, message: Message):
            await self.handle_db_status(client, message)

        @self.app.on_message(filters.command("dmstatus"))
        async def dm_status_command(client, message: Message):
            await self.handle_dm_status(client, message)

        @self.app.on_message(filters.command("freetrialusers"))
        async def timed_auto_role_command(client, message: Message):
            await self.handle_timed_auto_role(client, message)

        @self.app.on_message(filters.command("retracttrial"))
        async def retract_trial_command(client, message: Message):
            await self.handle_retract_trial(client, message)

        @self.app.on_message(filters.command("clearmember"))
        async def clear_member_command(client, message: Message):
            await self.handle_clear_member(client, message)

        @self.app.on_chat_join_request()
        async def handle_join_request(client, join_request: ChatJoinRequest):
            await self.process_join_request(client, join_request)

        @self.app.on_chat_member_updated()
        async def handle_member_update(client,
                                       member_update: ChatMemberUpdated):
            await self.process_member_update(client, member_update)

        @self.app.on_callback_query(filters.regex("^entry_"))
        async def entry_callback(client, callback_query: CallbackQuery):
            await self.handle_entry_callback(client, callback_query)

        @self.app.on_callback_query(filters.regex("^ovr_"))
        async def override_callback(client, callback_query: CallbackQuery):
            await self.handle_override_callback(client, callback_query)

        @self.app.on_callback_query(filters.regex("^tar_"))
        async def timedautorole_callback(client,
                                         callback_query: CallbackQuery):
            await self.handle_timedautorole_callback(client, callback_query)

        @self.app.on_callback_query(filters.regex("^pricetest_"))
        async def pricetest_callback(client, callback_query: CallbackQuery):
            await self.handle_pricetest_callback(client, callback_query)

        @self.app.on_callback_query(filters.regex("^retrt_"))
        async def retracttrial_callback(client, callback_query: CallbackQuery):
            await self.handle_retracttrial_callback(client, callback_query)

        @self.app.on_message(
            filters.private & filters.text & ~filters.command([
                "entry", "activetrades", "tradeoverride", "pricetest",
                "dbstatus", "dmstatus", "freetrialusers", "retracttrial"
            ]))
        async def text_input_handler(client, message: Message):
            await self.handle_text_input(client, message)

        @self.app.on_message(filters.group & filters.text & ~filters.command([
            "entry", "activetrades", "tradeoverride", "pricetest", "dbstatus",
            "dmstatus", "freetrialusers", "retracttrial"
        ]))
        async def group_message_handler(client, message: Message):
            await self.handle_group_message(client, message)

    async def is_owner(self, user_id: int) -> bool:
        """Check if user is the bot owner"""
        if BOT_OWNER_USER_ID == 0:
            # If owner ID not set, no one is owner
            return False
        is_owner = user_id == BOT_OWNER_USER_ID
        if not is_owner and BOT_OWNER_USER_ID > 0:
            # Log failed owner checks for debugging
            logger.debug(
                f"Owner check failed: user {user_id} != owner {BOT_OWNER_USER_ID}"
            )
        return is_owner

    async def handle_group_message(self, client: Client, message: Message):
        """Handle messages in groups - detect manual signals from owner"""
        if not message.from_user:
            return

        if not await self.is_owner(message.from_user.id):
            return

        if message.chat.id == DEBUG_GROUP_ID:
            return

        if message.chat.id not in [VIP_GROUP_ID, FREE_GROUP_ID]:
            return

        if "Trade Signal For:" not in message.text:
            return

        await self.log_to_debug(
            f"Manual signal detected from owner in {'VIP' if message.chat.id == VIP_GROUP_ID else 'Free'} group"
        )

        try:
            trade_data = self.parse_signal_message(message.text)

            if not trade_data:
                await self.log_to_debug(
                    "Failed to parse manual signal - invalid format or missing data"
                )
                return

            pair = trade_data.get('pair')
            action = trade_data.get('action')
            entry_price = trade_data.get('entry')

            if not pair or not action or not entry_price:
                await self.log_to_debug(
                    f"Missing required fields - pair: {pair}, action: {action}, entry: {entry_price}"
                )
                return

            await self.log_to_debug(
                f"Parsed manual signal: {pair} {action} @ {entry_price}")

            if pair in EXCLUDED_FROM_TRACKING:
                await self.log_to_debug(
                    f"Skipping tracking for {pair} (excluded from auto-tracking)"
                )
                return

            assigned_api = await self.get_working_api_for_pair(pair)

            live_price = await self.get_live_price(pair)
            if live_price:
                live_tracking_levels = self.calculate_tp_sl_levels(
                    live_price, pair, action)
                await self.log_to_debug(
                    f"Manual signal tracking setup for {pair}:\n"
                    f"- User entered price: {entry_price}\n"
                    f"- Live price at signal: {live_price}\n"
                    f"- Tracking TP1: {live_tracking_levels['tp1']:.5f}\n"
                    f"- Tracking TP2: {live_tracking_levels['tp2']:.5f}\n"
                    f"- Tracking TP3: {live_tracking_levels['tp3']:.5f}\n"
                    f"- Tracking SL: {live_tracking_levels['sl']:.5f}")
            else:
                live_tracking_levels = self.calculate_tp_sl_levels(
                    entry_price, pair, action)
                live_price = entry_price
                await self.log_to_debug(
                    f"Could not get live price for {pair}, using entered price for tracking"
                )

            group_name = "VIP" if message.chat.id == VIP_GROUP_ID else "Free"
            trade_key = f"{message.chat.id}_{message.id}"

            parsed_entry_type = trade_data.get('entry_type')
            if not parsed_entry_type:
                parsed_entry_type = f"{action.lower()} execution"

            full_trade_data = {
                'message_id':
                str(message.id),
                'trade_key':
                trade_key,
                'chat_id':
                message.chat.id,
                'pair':
                pair,
                'action':
                action,
                'entry_type':
                parsed_entry_type,
                'entry_price':
                float(entry_price),
                'tp1_price':
                float(live_tracking_levels['tp1']),
                'tp2_price':
                float(live_tracking_levels['tp2']),
                'tp3_price':
                float(live_tracking_levels['tp3']),
                'sl_price':
                float(live_tracking_levels['sl']),
                'entry':
                float(live_price),
                'tp1':
                float(live_tracking_levels['tp1']),
                'tp2':
                float(live_tracking_levels['tp2']),
                'tp3':
                float(live_tracking_levels['tp3']),
                'sl':
                float(live_tracking_levels['sl']),
                'telegram_entry':
                float(entry_price),
                'telegram_tp1':
                float(trade_data.get('tp1') or live_tracking_levels['tp1']),
                'telegram_tp2':
                float(trade_data.get('tp2') or live_tracking_levels['tp2']),
                'telegram_tp3':
                float(trade_data.get('tp3') or live_tracking_levels['tp3']),
                'telegram_sl':
                float(trade_data.get('sl') or live_tracking_levels['sl']),
                'live_entry':
                float(live_price),
                'assigned_api':
                assigned_api,
                'status':
                trade_data.get('status', 'active'),
                'tp_hits': [],
                'manual_overrides': [],
                'breakeven_active':
                False,
                'created_at':
                datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ).isoformat(),
                'group_name':
                group_name,
                'channel_id':
                message.chat.id
            }

            PRICE_TRACKING_CONFIG['active_trades'][trade_key] = full_trade_data
            await self.save_trade_to_db(trade_key, full_trade_data)
            asyncio.create_task(
                self.check_single_trade_immediately(trade_key,
                                                    full_trade_data))

            await self.log_to_debug(
                f"Manual signal tracking activated: {pair} {action} @ {live_price} in {group_name} group"
            )

            logger.info(
                f"NEW MANUAL SIGNAL DETECTED: {pair} {action} @ {live_price}")

        except Exception as e:
            import traceback
            full_traceback = traceback.format_exc()
            await self.log_to_debug(
                f"Error processing manual signal: {str(e)}\n{full_traceback[:1500]}"
            )
            logger.error(f"Error processing manual signal: {e}")

    def parse_signal_message(self, content: str) -> Optional[Dict]:
        """Parse a trading signal message to extract trade data"""
        try:
            trade_data = {
                "pair": None,
                "action": None,
                "entry": None,
                "tp1": None,
                "tp2": None,
                "tp3": None,
                "sl": None,
                "status": "active",
                "tp_hits": [],
                "breakeven_active": False,
                "entry_type": None
            }

            pair_match = re.search(
                r'Trade Signal For:\s*\*?\*?([A-Z0-9/]+)\*?\*?', content,
                re.IGNORECASE)
            if pair_match:
                raw_pair = pair_match.group(1).strip()
                trade_data["pair"] = raw_pair.upper().replace("/", "").replace(
                    "-", "").replace("_", "")

            entry_type_match = re.search(
                r'Entry Type:\s*\*?\*?(Buy|Sell)\s+(execution|limit)\*?\*?',
                content, re.IGNORECASE)
            if entry_type_match:
                action = entry_type_match.group(1).upper()
                order_type = entry_type_match.group(2).lower()
                trade_data["action"] = action
                trade_data["entry_type"] = f"{action.lower()} {order_type}"
                if order_type == "limit":
                    trade_data["status"] = "pending_entry"
                else:
                    trade_data["status"] = "active"
            else:
                if "BUY" in content.upper():
                    trade_data["action"] = "BUY"
                elif "SELL" in content.upper():
                    trade_data["action"] = "SELL"

            entry_match = re.search(r'Entry Price:\s*\$?([0-9]+(?:\.[0-9]+)?)',
                                    content, re.IGNORECASE)
            if entry_match:
                trade_data["entry"] = float(entry_match.group(1))
            else:
                entry_match = re.search(r'Entry[:\s]*\$?([0-9]+(?:\.[0-9]+)?)',
                                        content, re.IGNORECASE)
                if entry_match:
                    trade_data["entry"] = float(entry_match.group(1))

            tp1_match = re.search(r'Take Profit 1:\s*\$?([0-9]+(?:\.[0-9]+)?)',
                                  content, re.IGNORECASE)
            if tp1_match:
                trade_data["tp1"] = float(tp1_match.group(1))

            tp2_match = re.search(r'Take Profit 2:\s*\$?([0-9]+(?:\.[0-9]+)?)',
                                  content, re.IGNORECASE)
            if tp2_match:
                trade_data["tp2"] = float(tp2_match.group(1))

            tp3_match = re.search(r'Take Profit 3:\s*\$?([0-9]+(?:\.[0-9]+)?)',
                                  content, re.IGNORECASE)
            if tp3_match:
                trade_data["tp3"] = float(tp3_match.group(1))

            sl_match = re.search(r'Stop Loss:\s*\$?([0-9]+(?:\.[0-9]+)?)',
                                 content, re.IGNORECASE)
            if sl_match:
                trade_data["sl"] = float(sl_match.group(1))

            if trade_data["pair"] and trade_data["action"] and trade_data[
                    "entry"]:
                return trade_data
            else:
                return None

        except Exception as e:
            logger.error(f"Error parsing signal message: {e}")
            return None

    async def log_to_debug(self, message: str):
        if DEBUG_GROUP_ID:
            try:
                await self.app.send_message(DEBUG_GROUP_ID,
                                            f"**Bot Log:** {message}")
            except Exception as e:
                logger.error(f"Failed to send debug log: {e}")
        logger.info(message)

    def is_weekend_time(self, check_time: datetime) -> bool:
        if PYTZ_AVAILABLE:
            if check_time.tzinfo is None:
                check_time = AMSTERDAM_TZ.localize(check_time)
            else:
                check_time = check_time.astimezone(AMSTERDAM_TZ)
        weekday = check_time.weekday()
        hour = check_time.hour
        if weekday == 4 and hour >= 12:
            return True
        if weekday == 5:
            return True
        if weekday == 6:
            return True
        return False

    def calculate_trial_expiry_time(self, join_time: datetime) -> datetime:
        """
        Calculate trial expiry time to ensure exactly 3 trading days.
        Trading days: Monday-Friday only
        
        Rules:
        - Saturday/Sunday joiners: Always expire Wednesday 22:59 (regardless of join time)
        - All other days: Expire exactly 3 trading days later at the same time they joined
        
        Examples:
        - Saturday 13:37 join → expires Wednesday 22:59
        - Sunday 03:00 join → expires Wednesday 22:59
        - Friday 13:37 join → expires Tuesday 13:37 (5 calendar days: Fri→Sat→Sun→Mon→Tue)
        - Wednesday 14:00 join → expires Friday 14:00 (Wed, Thu, Fri = 3 trading days)
        """
        if join_time.tzinfo is None:
            join_time = AMSTERDAM_TZ.localize(join_time)
        else:
            join_time = join_time.astimezone(AMSTERDAM_TZ)

        weekday = join_time.weekday(
        )  # 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun

        # If joined on Saturday or Sunday, always expire Wednesday 22:59
        if weekday >= 5:  # 5=Saturday, 6=Sunday
            current_date = join_time.date()
            # Find the next Wednesday
            while current_date.weekday() != 2:  # 2 = Wednesday
                current_date += timedelta(days=1)

            expiry_time = AMSTERDAM_TZ.localize(
                datetime(current_date.year, current_date.month,
                         current_date.day, 22, 59, 0))
            return expiry_time

        # For other weekdays, count exactly 3 trading days from join time
        trading_days_counted = 0
        current_date = join_time.date()

        while True:
            day_weekday = current_date.weekday()

            # Only count weekdays (Mon-Fri)
            if day_weekday < 5:
                trading_days_counted += 1

                # If we've counted 3 trading days, set expiry at the same time as join
                if trading_days_counted == 3:
                    expiry_datetime = datetime(current_date.year,
                                               current_date.month,
                                               current_date.day,
                                               join_time.hour,
                                               join_time.minute,
                                               join_time.second)
                    return AMSTERDAM_TZ.localize(expiry_datetime)

            current_date += timedelta(days=1)

    def calculate_tp_sl_levels(self, entry_price: float, pair: str,
                               action: str) -> dict:
        if pair.upper() in PAIR_CONFIG:
            pip_value = PAIR_CONFIG[pair.upper()]['pip_value']
        else:
            pip_value = 0.0001

        tp1_pips = 20 * pip_value
        tp2_pips = 40 * pip_value
        tp3_pips = 70 * pip_value
        sl_pips = 50 * pip_value

        is_buy = action.upper() == "BUY"

        if is_buy:
            tp1 = entry_price + tp1_pips
            tp2 = entry_price + tp2_pips
            tp3 = entry_price + tp3_pips
            sl = entry_price - sl_pips
        else:
            tp1 = entry_price - tp1_pips
            tp2 = entry_price - tp2_pips
            tp3 = entry_price - tp3_pips
            sl = entry_price + sl_pips

        return {
            'entry': entry_price,
            'tp1': tp1,
            'tp2': tp2,
            'tp3': tp3,
            'sl': sl
        }

    async def handle_entry(self, client: Client, message: Message):
        if not await self.is_owner(message.from_user.id):
            return

        user_id = message.from_user.id
        PENDING_ENTRIES[user_id] = {
            'action': None,
            'entry_type': None,
            'pair': None,
            'price': None,
            'groups': [],
            'track_price': True
        }

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("BUY", callback_data="entry_action_buy"),
            InlineKeyboardButton("SELL", callback_data="entry_action_sell")
        ], [InlineKeyboardButton("Cancel", callback_data="entry_cancel")]])

        await message.reply(
            "**Create Trading Signal**\n\nStep 1: Select action:",
            reply_markup=keyboard)

    async def handle_entry_callback(self, client: Client,
                                    callback_query: CallbackQuery):
        user_id = callback_query.from_user.id

        if not await self.is_owner(user_id):
            await callback_query.answer("This is restricted to the bot owner.",
                                        show_alert=True)
            return

        data = callback_query.data

        if data == "entry_cancel":
            PENDING_ENTRIES.pop(user_id, None)
            self.awaiting_price_input.pop(user_id, None)
            self.awaiting_custom_pair.pop(user_id, None)
            await callback_query.message.edit_text("Signal creation cancelled."
                                                   )
            return

        if user_id not in PENDING_ENTRIES:
            await callback_query.answer(
                "Session expired. Please use /entry again.", show_alert=True)
            return

        entry_data = PENDING_ENTRIES[user_id]

        if data.startswith("entry_action_"):
            action = data.replace("entry_action_", "").upper()
            entry_data['action'] = action

            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("Execution (Market)",
                                     callback_data="entry_type_execution"),
                InlineKeyboardButton("Limit Order",
                                     callback_data="entry_type_limit")
            ], [InlineKeyboardButton("Cancel", callback_data="entry_cancel")]])

            await callback_query.message.edit_text(
                f"**Create Trading Signal**\n\n"
                f"Action: **{action}**\n\n"
                f"Step 2: Select order type:",
                reply_markup=keyboard)

        elif data.startswith("entry_type_"):
            entry_type = data.replace("entry_type_", "")
            entry_data['entry_type'] = entry_type

            self.awaiting_custom_pair[user_id] = callback_query.message.id
            await callback_query.message.edit_text(
                f"**Create Trading Signal**\n\n"
                f"Action: **{entry_data['action']}**\n"
                f"Type: **{entry_type.upper()}**\n\n"
                f"Step 3: Type the trading pair (e.g., EURUSD, GBPJPY, XAUUSD):"
            )

        elif data.startswith("entry_group_"):
            group_choice = data.replace("entry_group_", "")

            if group_choice == "vip":
                entry_data['groups'] = [VIP_GROUP_ID] if VIP_GROUP_ID else []
                entry_data['track_price'] = True
            elif group_choice == "free":
                entry_data['groups'] = [FREE_GROUP_ID] if FREE_GROUP_ID else []
                entry_data['track_price'] = True
            elif group_choice == "both":
                entry_data['groups'] = []
                if VIP_GROUP_ID:
                    entry_data['groups'].append(VIP_GROUP_ID)
                if FREE_GROUP_ID:
                    entry_data['groups'].append(FREE_GROUP_ID)
                entry_data['track_price'] = True
            elif group_choice == "manual":
                entry_data['groups'] = [callback_query.message.chat.id]
                entry_data['track_price'] = False
                entry_data['manual_signal'] = True

            await self.show_confirmation(callback_query, entry_data)

        elif data == "entry_confirm":
            await self.execute_entry_signal(client, callback_query, entry_data)

        elif data == "entry_back_groups":
            await self.show_group_selection(callback_query, entry_data)

        await callback_query.answer()

    async def show_group_selection(self, callback_query: CallbackQuery,
                                   entry_data: dict):
        keyboard = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("VIP Group Only",
                                     callback_data="entry_group_vip")
            ],
             [
                 InlineKeyboardButton("Free Group Only",
                                      callback_data="entry_group_free")
             ],
             [
                 InlineKeyboardButton("Both Groups",
                                      callback_data="entry_group_both")
             ],
             [
                 InlineKeyboardButton("Manual Signal (untracked)",
                                      callback_data="entry_group_manual")
             ], [InlineKeyboardButton("Cancel",
                                      callback_data="entry_cancel")]])

        price_text = f"\nPrice: **{entry_data['price']}**" if entry_data[
            'price'] else ""

        await callback_query.message.edit_text(
            f"**Create Trading Signal**\n\n"
            f"Action: **{entry_data['action']}**\n"
            f"Type: **{entry_data['entry_type'].upper()}**\n"
            f"Pair: **{entry_data['pair']}**{price_text}\n\n"
            f"Step 4: Select where to send:",
            reply_markup=keyboard)

    async def show_confirmation(self, callback_query: CallbackQuery,
                                entry_data: dict):
        group_names = []
        if VIP_GROUP_ID in entry_data['groups']:
            group_names.append("VIP Group")
        if FREE_GROUP_ID in entry_data['groups']:
            group_names.append("Free Group")
        if entry_data.get('manual_signal'):
            group_names.append("Manual Signal (Current Chat)")
        groups_text = ", ".join(group_names) if group_names else "None"

        pair = entry_data['pair']
        decimals = PAIR_CONFIG.get(pair, {}).get('decimals', 5)
        if entry_data['price']:
            price_text = f"Price: **${entry_data['price']:.{decimals}f}**\n"
        else:
            price_text = "Price: **Live (auto-fetch)**\n"
        tracking_text = "Price Tracking: **Enabled**" if entry_data.get(
            'track_price',
            True) else "Price Tracking: **Disabled** (debug mode)"

        combined_entry_type = f"{entry_data['action'].capitalize()} {entry_data['entry_type']}"

        keyboard = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("Confirm & Send Signal",
                                     callback_data="entry_confirm")
            ],
             [
                 InlineKeyboardButton("Change Groups",
                                      callback_data="entry_back_groups")
             ], [InlineKeyboardButton("Cancel",
                                      callback_data="entry_cancel")]])

        await callback_query.message.edit_text(
            f"**Confirm Trading Signal**\n\n"
            f"Entry Type: **{combined_entry_type}**\n"
            f"Pair: **{pair}**\n"
            f"{price_text}"
            f"Send to: **{groups_text}**\n"
            f"{tracking_text}\n\n"
            f"Ready to send?",
            reply_markup=keyboard)

    async def execute_entry_signal(self, client: Client,
                                   callback_query: CallbackQuery,
                                   entry_data: dict):
        user_id = callback_query.from_user.id

        pair = entry_data['pair']
        action = entry_data['action']
        entry_type = entry_data['entry_type']
        entry_price = entry_data['price']
        signal_channels = entry_data['groups']

        if not entry_price:
            live_price = await self.get_live_price(pair)
            if live_price:
                entry_price = live_price
            else:
                await callback_query.message.edit_text(
                    f"Could not get live price for {pair}. Please try again with a limit order and specify the price."
                )
                PENDING_ENTRIES.pop(user_id, None)
                return

        levels = self.calculate_tp_sl_levels(entry_price, pair, action)
        decimals = PAIR_CONFIG.get(pair, {}).get('decimals', 5)

        def fmt(price):
            return f"${price:.{decimals}f}"

        combined_entry_type = f"{action.capitalize()} {entry_type}"

        signal_text = (f"**Trade Signal For: {pair}**\n"
                       f"Entry Type: {combined_entry_type}\n"
                       f"Entry Price: {fmt(entry_price)}\n\n"
                       f"**Take Profit Levels:**\n"
                       f"Take Profit 1: {fmt(levels['tp1'])}\n"
                       f"Take Profit 2: {fmt(levels['tp2'])}\n"
                       f"Take Profit 3: {fmt(levels['tp3'])}\n\n"
                       f"Stop Loss: {fmt(levels['sl'])}")

        if pair.upper() in ['US100', 'GER40']:
            signal_text += "\n\n**Please note that prices on US100 & GER40 vary a lot from broker to broker, so it is possible that the current price in our signal is different than the current price with your broker. Execute this signal within a 5 minute window of this trade being sent and please manually recalculate the pip value for TP1/2/3 & SL depending on your broker's current price.**"

        if not signal_channels:
            signal_channels = [callback_query.message.chat.id]

        sent_count = 0
        sent_messages = []

        for channel_id in signal_channels:
            try:
                sent_msg = await self.app.send_message(channel_id, signal_text)
                sent_count += 1
                if channel_id == VIP_GROUP_ID:
                    group_name = "VIP"
                elif channel_id == FREE_GROUP_ID:
                    group_name = "Free"
                else:
                    group_name = "Manual Signal"
                sent_messages.append({
                    'message': sent_msg,
                    'channel_id': channel_id,
                    'group_name': group_name
                })
            except Exception as e:
                logger.error(
                    f"Failed to send signal to channel {channel_id}: {e}")

        track_price = entry_data.get('track_price', True)

        if pair in EXCLUDED_FROM_TRACKING:
            track_price = False

        if sent_messages and track_price:
            assigned_api = await self.get_working_api_for_pair(pair)

            live_price = await self.get_live_price(pair)
            if live_price:
                live_tracking_levels = self.calculate_tp_sl_levels(
                    live_price, pair, action)
                await self.log_to_debug(
                    f"Price tracking setup for {pair}:\n"
                    f"- User entered price: {entry_price}\n"
                    f"- Live price at signal: {live_price}\n"
                    f"- Tracking TP1: {live_tracking_levels['tp1']:.5f}\n"
                    f"- Tracking TP2: {live_tracking_levels['tp2']:.5f}\n"
                    f"- Tracking TP3: {live_tracking_levels['tp3']:.5f}\n"
                    f"- Tracking SL: {live_tracking_levels['sl']:.5f}")
            else:
                live_tracking_levels = levels
                live_price = entry_price
                await self.log_to_debug(
                    f"Could not get live price for {pair}, using user-entered price for tracking"
                )

            for msg_info in sent_messages:
                sent_msg = msg_info['message']
                channel_id = msg_info['channel_id']
                group_name = msg_info['group_name']

                trade_key = f"{channel_id}_{sent_msg.id}"

                trade_data = {
                    'message_id': str(sent_msg.id),
                    'trade_key': trade_key,
                    'chat_id': sent_msg.chat.id,
                    'pair': pair,
                    'action': action,
                    'entry_type': entry_type,
                    'entry_price': float(entry_price),
                    'tp1_price': float(live_tracking_levels['tp1']),
                    'tp2_price': float(live_tracking_levels['tp2']),
                    'tp3_price': float(live_tracking_levels['tp3']),
                    'sl_price': float(live_tracking_levels['sl']),
                    'entry': float(live_price),
                    'tp1': float(live_tracking_levels['tp1']),
                    'tp2': float(live_tracking_levels['tp2']),
                    'tp3': float(live_tracking_levels['tp3']),
                    'sl': float(live_tracking_levels['sl']),
                    'telegram_entry': float(entry_price),
                    'telegram_tp1': float(levels['tp1']),
                    'telegram_tp2': float(levels['tp2']),
                    'telegram_tp3': float(levels['tp3']),
                    'telegram_sl': float(levels['sl']),
                    'live_entry': float(live_price),
                    'assigned_api': assigned_api,
                    'status': 'active',
                    'tp_hits': [],
                    'manual_overrides': [],
                    'breakeven_active': False,
                    'created_at': datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ).isoformat(),
                    'group_name': group_name,
                    'channel_id': channel_id
                }

                PRICE_TRACKING_CONFIG['active_trades'][trade_key] = trade_data
                await self.save_trade_to_db(trade_key, trade_data)
                asyncio.create_task(
                    self.check_single_trade_immediately(trade_key, trade_data))

        if pair in EXCLUDED_FROM_TRACKING:
            tracking_status = f"No price tracking ({pair} is manually tracked)."
        elif track_price:
            tracking_status = "Live price tracking is now active."
        else:
            tracking_status = "No price tracking (debug mode)."

        await callback_query.message.edit_text(
            f"**Signal Created Successfully!**\n\n"
            f"Pair: **{pair}**\n"
            f"Action: **{action}**\n"
            f"Entry: **{fmt(entry_price)}**\n\n"
            f"Sent to **{sent_count}** group(s).\n"
            f"{tracking_status}")

        PENDING_ENTRIES.pop(user_id, None)
        await self.log_to_debug(
            f"New {entry_type} signal created: {pair} {action} @ {entry_price} - sent to {sent_count} channels"
        )

    async def handle_text_input(self, client: Client, message: Message):
        user_id = message.from_user.id

        if not await self.is_owner(user_id):
            return

        # Handle retracttrial custom input
        if not hasattr(self, 'awaiting_retracttrial_input'):
            self.awaiting_retracttrial_input = {}
        
        if user_id in self.awaiting_retracttrial_input:
            try:
                context = self.awaiting_retracttrial_input.pop(user_id)
                menu_id = context['menu_id']
                idx = context['idx']
                user_id_str = context['user_id_str']
                
                text = message.text.strip().lower()
                total_minutes = 0
                
                # Parse input like "5h", "30m", "2h 15m", etc.
                parts = text.split()
                for part in parts:
                    if 'h' in part:
                        try:
                            h = int(part.replace('h', ''))
                            total_minutes += h * 60
                        except ValueError:
                            pass
                    elif 'm' in part:
                        try:
                            m = int(part.replace('m', ''))
                            total_minutes += m
                        except ValueError:
                            pass
                
                if total_minutes <= 0:
                    await message.reply("Please enter a valid time (e.g., '5h' or '30m' or '2h 15m')")
                    return
                
                if user_id_str not in AUTO_ROLE_CONFIG['active_members']:
                    await message.reply("User not found in active trials.")
                    return
                
                member_data = AUTO_ROLE_CONFIG['active_members'][user_id_str]
                old_expiry = datetime.fromisoformat(member_data['expiry_time'])
                if old_expiry.tzinfo is None:
                    old_expiry = AMSTERDAM_TZ.localize(old_expiry)
                
                new_expiry = old_expiry - timedelta(minutes=total_minutes)
                member_data['expiry_time'] = new_expiry.isoformat()
                
                # Update database
                if self.db_pool:
                    async with self.db_pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE active_members SET expiry_time = $1 WHERE member_id = $2",
                            new_expiry, int(user_id_str))
                
                await self.save_auto_role_config()
                self.retracttrial_mappings.pop(menu_id, None)
                
                hours = total_minutes // 60
                mins = total_minutes % 60
                if hours > 0 and mins > 0:
                    label = f"{hours}h {mins}m"
                elif hours > 0:
                    label = f"{hours}h"
                else:
                    label = f"{mins}m"
                
                await message.reply(
                    f"✅ Subtracted {label} from user {user_id_str}\n"
                    f"New expiry: {new_expiry.strftime('%Y-%m-%d %H:%M:%S')}")
                return
            except Exception as e:
                await message.reply(f"Error: {str(e)}")
                return

        if user_id in self.awaiting_custom_pair:
            pair = message.text.upper().strip()
            awaiting_data = self.awaiting_custom_pair.pop(user_id)

            if isinstance(awaiting_data,
                          dict) and awaiting_data.get('type') == 'pricetest':
                await message.reply(f"Fetching live price for **{pair}**...")

                price = await self.get_live_price(pair)
                if price:
                    pair_name = PAIR_CONFIG.get(pair, {}).get('name', pair)
                    decimals = PAIR_CONFIG.get(pair, {}).get('decimals', 5)
                    await message.reply(
                        f"**Price Test: {pair_name}**\n\nLive Price: **{price:.{decimals}f}**"
                    )
                else:
                    await message.reply(
                        f"Could not retrieve price for **{pair}**. The pair may not be supported or APIs are unavailable."
                    )
                return

            if user_id in PENDING_ENTRIES:
                entry_data = PENDING_ENTRIES[user_id]
                entry_data['pair'] = pair

                self.awaiting_price_input[user_id] = True
                await message.reply(
                    f"Pair set to **{pair}**.\n\nStep 4: Type the entry price (the price on your chart right now):"
                )
            return

        if user_id in self.awaiting_price_input:
            try:
                price = float(message.text.strip())
                if user_id in PENDING_ENTRIES:
                    entry_data = PENDING_ENTRIES[user_id]
                    entry_data['price'] = price

                    self.awaiting_price_input.pop(user_id, None)

                    keyboard = InlineKeyboardMarkup(
                        [[
                            InlineKeyboardButton(
                                "VIP Group Only",
                                callback_data="entry_group_vip")
                        ],
                         [
                             InlineKeyboardButton(
                                 "Free Group Only",
                                 callback_data="entry_group_free")
                         ],
                         [
                             InlineKeyboardButton(
                                 "Both Groups",
                                 callback_data="entry_group_both")
                         ],
                         [
                             InlineKeyboardButton(
                                 "Manual Signal (untracked)",
                                 callback_data="entry_group_manual")
                         ],
                         [
                             InlineKeyboardButton("Cancel",
                                                  callback_data="entry_cancel")
                         ]])
                    await message.reply(
                        f"**Create Trading Signal**\n\n"
                        f"Action: **{entry_data['action']}**\n"
                        f"Type: **{entry_data['entry_type'].upper()}**\n"
                        f"Pair: **{entry_data['pair']}**\n"
                        f"Price: **{price}**\n\n"
                        f"Step 4: Select where to send:",
                        reply_markup=keyboard)
            except ValueError:
                await message.reply(
                    "Invalid price. Please enter a valid number (e.g., 2650.50):"
                )
            return

    async def handle_active_trades(self, client: Client, message: Message):
        if not await self.is_owner(message.from_user.id):
            return

        trades = PRICE_TRACKING_CONFIG['active_trades']

        if not trades:
            await message.reply(
                "**No Active Trades**\n\nThere are currently no active trading signals being tracked."
            )
            return

        response = f"**Active Trades ({len(trades)})**\n"
        response += f"Next refresh in: {PRICE_TRACKING_CONFIG['check_interval']}s\n\n"

        for msg_id, trade in list(trades.items())[:10]:
            pair = trade.get('pair', 'Unknown')
            action = trade.get('action', 'Unknown')
            entry = trade.get('entry_price', trade.get('entry', 0))
            tp1 = trade.get('tp1_price', trade.get('tp1', 0))
            tp2 = trade.get('tp2_price', trade.get('tp2', 0))
            tp3 = trade.get('tp3_price', trade.get('tp3', 0))
            sl = trade.get('sl_price', trade.get('sl', 0))
            status = trade.get('status', 'active')
            tp_hits = trade.get('tp_hits', [])
            breakeven = trade.get('breakeven_active', False)
            assigned_api = trade.get('assigned_api', 'currencybeacon')
            manual_overrides = trade.get('manual_overrides', [])

            live_price = await self.get_live_price(pair)

            if live_price:
                position_info = self.analyze_trade_position(
                    action, entry, tp1, tp2, tp3, sl, live_price)
                price_line = f"**Current: {live_price:.5f}** {position_info['emoji']}"
                position_text = f"_{position_info['position']}_"
            else:
                price_line = "**Current: N/A**"
                position_text = "_Unable to get price_"

            tp1_mark = "✅" if 'tp1' in tp_hits else "⭕"
            tp2_mark = "✅" if 'tp2' in tp_hits else "⭕"
            tp3_mark = "✅" if 'tp3' in tp_hits else "⭕"
            sl_mark = "🔴" if 'sl' in status.lower() else "⭕"

            levels_display = (f"{sl_mark} SL: {sl:.5f}\n"
                              f"⭕ Entry: {entry:.5f}\n"
                              f"{tp1_mark} TP1: {tp1:.5f}\n"
                              f"{tp2_mark} TP2: {tp2:.5f}\n"
                              f"{tp3_mark} TP3: {tp3:.5f}")

            status_indicators = []
            if breakeven:
                status_indicators.append("🔄 Breakeven Active")
            if 'active' in status.lower():
                status_indicators.append("🟢 Active")
            elif 'closed' in status.lower():
                status_indicators.append("⚪ Closed")
            if manual_overrides:
                status_indicators.append(
                    f"✋ Overrides: {', '.join(manual_overrides)}")

            response += f"**{pair}** - {action}\n"
            response += f"{price_line}\n{position_text}\n\n"
            response += f"{levels_display}\n"
            response += f"API: {assigned_api}\n"
            if status_indicators:
                response += f"{' | '.join(status_indicators)}\n"
            response += f"ID: {msg_id[:8]}...\n"
            response += "─────────────\n\n"

        if len(trades) > 10:
            response += f"_...and {len(trades) - 10} more trades_"

        await message.reply(response)

    def analyze_trade_position(self, action: str, entry: float, tp1: float,
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
            elif current_price >= tp2:
                return {"emoji": "🟢", "position": "Between TP1 and TP2"}
            elif current_price >= tp3:
                return {"emoji": "💚", "position": "Between TP2 and TP3"}
            else:
                return {"emoji": "🚀", "position": "Below TP3 - Max Profit"}

    async def handle_trade_override(self, client: Client, message: Message):
        if not await self.is_owner(message.from_user.id):
            return

        active_trades = PRICE_TRACKING_CONFIG['active_trades']

        if not active_trades:
            await message.reply("**Trade Override System**\n\n"
                                "No active trades found.\n\n"
                                "Use /activetrades to confirm.")
            return

        menu_id = str(message.id)[-8:]
        trade_mapping = {}
        self.pending_multi_select = self.pending_multi_select if hasattr(
            self, 'pending_multi_select') else {}
        self.pending_multi_select[menu_id] = []
        buttons = []

        for idx, (msg_id,
                  trade_data) in enumerate(list(active_trades.items())[:25]):
            pair = trade_data.get('pair', 'Unknown')
            action = trade_data.get('action', 'Unknown')
            tp_hits = trade_data.get('tp_hits', [])
            tp_status = f" ({', '.join(tp_hits)})" if tp_hits else ""
            group_name = trade_data.get('group_name', '')
            group_label = f" [{group_name}]" if group_name else ""

            trade_mapping[str(idx)] = msg_id

            buttons.append([
                InlineKeyboardButton(
                    f"☐ {pair} - {action}{tp_status}{group_label}",
                    callback_data=f"ovr_{menu_id}_sel_{idx}")
            ])

        self.override_trade_mappings[menu_id] = trade_mapping
        buttons.append([
            InlineKeyboardButton("✅ Confirm Selection",
                                 callback_data=f"ovr_{menu_id}_confirm")
        ])
        buttons.append([
            InlineKeyboardButton("Cancel",
                                 callback_data=f"ovr_{menu_id}_cancel")
        ])

        keyboard = InlineKeyboardMarkup(buttons)

        await message.reply(
            f"**Trade Override System**\n\n"
            f"Found **{len(active_trades)}** active trades.\n\n"
            f"Select up to 5 trades to modify (tap to toggle), then press Confirm:",
            reply_markup=keyboard)

    async def handle_override_callback(self, client: Client,
                                       callback_query: CallbackQuery):
        user_id = callback_query.from_user.id

        if not await self.is_owner(user_id):
            await callback_query.answer("This is restricted to the bot owner.",
                                        show_alert=True)
            return

        data = callback_query.data

        if not data.startswith("ovr_"):
            return

        parts = data.split("_")
        if len(parts) < 3:
            await callback_query.answer("Invalid callback data.",
                                        show_alert=True)
            return

        menu_id = parts[1]
        action_type = parts[2]

        if action_type == "cancel":
            self.override_trade_mappings.pop(menu_id, None)
            if hasattr(self, 'pending_multi_select'):
                self.pending_multi_select.pop(menu_id, None)
            await callback_query.message.edit_text("Trade override cancelled.")
            return

        trade_mapping = self.override_trade_mappings.get(menu_id, {})
        if not hasattr(self, 'pending_multi_select'):
            self.pending_multi_select = {}

        if action_type == "sel":
            idx = parts[3] if len(parts) > 3 else None
            if not idx:
                await callback_query.answer("Invalid selection.",
                                            show_alert=True)
                return

            selected = self.pending_multi_select.get(menu_id, [])
            if idx in selected:
                selected.remove(idx)
            else:
                if len(selected) >= 5:
                    await callback_query.answer(
                        "Maximum 5 trades can be selected.", show_alert=True)
                    return
                selected.append(idx)
            self.pending_multi_select[menu_id] = selected

            active_trades = PRICE_TRACKING_CONFIG['active_trades']
            buttons = []
            for i, (msg_id,
                    trade_data) in enumerate(list(active_trades.items())[:25]):
                pair = trade_data.get('pair', 'Unknown')
                action = trade_data.get('action', 'Unknown')
                tp_hits = trade_data.get('tp_hits', [])
                tp_status = f" ({', '.join(tp_hits)})" if tp_hits else ""
                group_name = trade_data.get('group_name', '')
                group_label = f" [{group_name}]" if group_name else ""
                checkbox = "☑" if str(i) in selected else "☐"
                buttons.append([
                    InlineKeyboardButton(
                        f"{checkbox} {pair} - {action}{tp_status}{group_label}",
                        callback_data=f"ovr_{menu_id}_sel_{i}")
                ])
            buttons.append([
                InlineKeyboardButton("✅ Confirm Selection",
                                     callback_data=f"ovr_{menu_id}_confirm")
            ])
            buttons.append([
                InlineKeyboardButton("Cancel",
                                     callback_data=f"ovr_{menu_id}_cancel")
            ])
            keyboard = InlineKeyboardMarkup(buttons)
            selected_count = len(selected)
            await callback_query.message.edit_text(
                f"**Trade Override System**\n\n"
                f"**Selected: {selected_count}/5** trades\n\n"
                f"Tap to toggle, then press Confirm:",
                reply_markup=keyboard)
            await callback_query.answer()
            return

        if action_type == "confirm":
            selected = self.pending_multi_select.get(menu_id, [])
            if not selected:
                await callback_query.answer(
                    "Please select at least one trade.", show_alert=True)
                return

            trade_list = []
            for idx in selected:
                full_msg_id = trade_mapping.get(idx)
                if full_msg_id and full_msg_id in PRICE_TRACKING_CONFIG[
                        'active_trades']:
                    trade = PRICE_TRACKING_CONFIG['active_trades'][full_msg_id]
                    pair = trade.get('pair', 'Unknown')
                    action = trade.get('action', 'Unknown')
                    group_name = trade.get('group_name', '')
                    group_label = f" [{group_name}]" if group_name else ""
                    trade_list.append(f"• {pair} - {action}{group_label}")

            if len(selected) == 1:
                description = f"**Selected Trade:**\n{trade_list[0]}"
            else:
                description = f"**Selected {len(selected)} Trades:**\n" + "\n".join(
                    trade_list)

            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🔴 SL Hit",
                                         callback_data=f"ovr_{menu_id}_slhit")
                ],
                [
                    InlineKeyboardButton("🟢 TP1 Hit",
                                         callback_data=f"ovr_{menu_id}_tp1hit")
                ],
                [
                    InlineKeyboardButton("🟢 TP2 Hit",
                                         callback_data=f"ovr_{menu_id}_tp2hit")
                ],
                [
                    InlineKeyboardButton("🚀 TP3 Hit",
                                         callback_data=f"ovr_{menu_id}_tp3hit")
                ],
                [
                    InlineKeyboardButton("🟡 Breakeven Hit After TP2",
                                         callback_data=f"ovr_{menu_id}_behit")
                ],
                [
                    InlineKeyboardButton("⏹️ End Tracking",
                                         callback_data=f"ovr_{menu_id}_endhit")
                ],
                [
                    InlineKeyboardButton("Cancel",
                                         callback_data=f"ovr_{menu_id}_cancel")
                ]
            ])

            await callback_query.message.edit_text(
                f"**Trade Override**\n\n"
                f"{description}\n\n"
                f"Select the status to apply:",
                reply_markup=keyboard)
            await callback_query.answer()
            return

        if action_type in [
                'slhit', 'tp1hit', 'tp2hit', 'tp3hit', 'behit', 'endhit'
        ]:
            selected = self.pending_multi_select.get(menu_id, [])
            if not selected:
                await callback_query.answer("No trades selected.",
                                            show_alert=True)
                return

            successful_trades = []
            failed_trades = []

            for idx in selected:
                full_msg_id = trade_mapping.get(idx)
                if not full_msg_id or full_msg_id not in PRICE_TRACKING_CONFIG[
                        'active_trades']:
                    failed_trades.append(f"ID {idx}: Not found")
                    continue

                trade = PRICE_TRACKING_CONFIG['active_trades'][full_msg_id]
                pair = trade.get('pair', 'Unknown')
                action = trade.get('action', 'Unknown')
                group_name = trade.get('group_name', '')
                group_label = f" [{group_name}]" if group_name else ""

                try:
                    if action_type == 'slhit':
                        trade['status'] = 'closed (sl hit - manual override)'
                        await self.send_sl_notification(
                            full_msg_id, trade, trade.get('sl_price', 0))
                        del PRICE_TRACKING_CONFIG['active_trades'][full_msg_id]
                        await self.remove_trade_from_db(
                            full_msg_id, 'manual_sl_hit')
                        successful_trades.append(
                            f"{pair} {action}{group_label} - SL Hit")

                    elif action_type == 'tp1hit':
                        if 'TP1' not in trade.get('tp_hits', []):
                            trade['tp_hits'] = trade.get('tp_hits',
                                                         []) + ['TP1']
                        trade['status'] = 'active (tp1 hit - manual override)'
                        await self.send_tp_notification(
                            full_msg_id, trade, 'TP1',
                            trade.get('tp1_price', 0))
                        await self.update_trade_in_db(full_msg_id, trade)
                        successful_trades.append(
                            f"{pair} {action}{group_label} - TP1 Hit")

                    elif action_type == 'tp2hit':
                        current_tp_hits = trade.get('tp_hits', [])
                        if 'TP1' not in current_tp_hits:
                            trade['tp_hits'] = current_tp_hits + ['TP1']
                            await self.send_tp_notification(
                                full_msg_id, trade, 'TP1',
                                trade.get('tp1_price', 0))
                            await asyncio.sleep(1)
                        if 'TP2' not in trade.get('tp_hits', []):
                            trade['tp_hits'] = trade.get('tp_hits',
                                                         []) + ['TP2']
                        trade['breakeven_active'] = True
                        trade[
                            'status'] = 'active (tp2 hit - manual override - breakeven active)'
                        await self.send_tp_notification(
                            full_msg_id, trade, 'TP2',
                            trade.get('tp2_price', 0))
                        await self.update_trade_in_db(full_msg_id, trade)
                        successful_trades.append(
                            f"{pair} {action}{group_label} - TP2 Hit")

                    elif action_type == 'tp3hit':
                        current_tp_hits = trade.get('tp_hits', [])
                        if 'TP1' not in current_tp_hits:
                            trade['tp_hits'] = current_tp_hits + ['TP1']
                            await self.send_tp_notification(
                                full_msg_id, trade, 'TP1',
                                trade.get('tp1_price', 0))
                            await asyncio.sleep(1)
                        if 'TP2' not in trade.get('tp_hits', []):
                            trade['tp_hits'] = trade.get('tp_hits',
                                                         []) + ['TP2']
                            trade['breakeven_active'] = True
                            await self.send_tp_notification(
                                full_msg_id, trade, 'TP2',
                                trade.get('tp2_price', 0))
                            await asyncio.sleep(1)
                        if 'TP3' not in trade.get('tp_hits', []):
                            trade['tp_hits'] = trade.get('tp_hits',
                                                         []) + ['TP3']
                        trade[
                            'status'] = 'completed (tp3 hit - manual override)'
                        await self.send_tp_notification(
                            full_msg_id, trade, 'TP3',
                            trade.get('tp3_price', 0))
                        del PRICE_TRACKING_CONFIG['active_trades'][full_msg_id]
                        await self.remove_trade_from_db(
                            full_msg_id, 'manual_tp3_hit')
                        successful_trades.append(
                            f"{pair} {action}{group_label} - TP3 Hit")

                    elif action_type == 'behit':
                        trade[
                            'status'] = 'closed (breakeven after tp2 - manual override)'
                        await self.send_breakeven_notification(
                            full_msg_id, trade)
                        del PRICE_TRACKING_CONFIG['active_trades'][full_msg_id]
                        await self.remove_trade_from_db(
                            full_msg_id, 'manual_breakeven_hit')
                        successful_trades.append(
                            f"{pair} {action}{group_label} - Breakeven After TP2"
                        )

                    elif action_type == 'endhit':
                        trade['status'] = 'closed (ended by manual override)'
                        del PRICE_TRACKING_CONFIG['active_trades'][full_msg_id]
                        await self.remove_trade_from_db(
                            full_msg_id, 'manual_end_tracking')
                        successful_trades.append(
                            f"{pair} {action}{group_label} - Tracking Ended")

                except Exception as e:
                    failed_trades.append(f"{pair}: {str(e)[:30]}")

            self.override_trade_mappings.pop(menu_id, None)
            self.pending_multi_select.pop(menu_id, None)

            total = len(successful_trades) + len(failed_trades)
            if len(successful_trades) == total:
                title = "✅ All Trades Updated Successfully"
            elif len(successful_trades) > 0:
                title = "⚠️ Partial Success"
            else:
                title = "❌ All Trades Failed"

            description = f"**Processed {total} trade(s)**\n\n"
            if successful_trades:
                description += "**✅ Successful:**\n" + "\n".join(
                    f"• {t}" for t in successful_trades) + "\n\n"
            if failed_trades:
                description += "**❌ Failed:**\n" + "\n".join(
                    f"• {t}" for t in failed_trades)

            await callback_query.message.edit_text(
                f"**{title}**\n\n{description}")
            await self.log_to_debug(
                f"Manual override completed: {len(successful_trades)} success, {len(failed_trades)} failed"
            )

        await callback_query.answer()

    async def handle_price_test(self, client: Client, message: Message):
        if not await self.is_owner(message.from_user.id):
            return

        args = message.text.split()[1:] if len(
            message.text.split()) > 1 else []

        if not args:
            popular_pairs = [['EURUSD', 'GBPUSD', 'USDJPY'],
                             ['XAUUSD', 'GBPJPY', 'AUDUSD'],
                             ['BTCUSD', 'US100', 'GER40'],
                             ['EURGBP', 'USDCAD', 'NZDUSD']]

            buttons = []
            for row in popular_pairs:
                button_row = [
                    InlineKeyboardButton(pair,
                                         callback_data=f"pricetest_{pair}")
                    for pair in row
                ]
                buttons.append(button_row)

            buttons.append([
                InlineKeyboardButton("Custom Pair",
                                     callback_data="pricetest_custom")
            ])
            buttons.append([
                InlineKeyboardButton("Cancel",
                                     callback_data="pricetest_cancel")
            ])

            keyboard = InlineKeyboardMarkup(buttons)

            await message.reply(
                "**Price Test Menu**\n\n"
                "Select a trading pair to check its live price:",
                reply_markup=keyboard)
            return

        pair = args[0].upper()
        await self._execute_price_test(message, pair)

    async def _execute_price_test(self, message: Message, pair: str):
        pair_name = PAIR_CONFIG.get(pair, {}).get('name', pair)

        price = await self.get_live_price(pair)

        if price:
            decimals = PAIR_CONFIG.get(pair, {}).get('decimals', 5)
            await message.reply(
                f"**Price Test: {pair_name}**\n\nLive Price: **{price:.{decimals}f}**"
            )
        else:
            await message.reply(
                f"Could not retrieve price for **{pair}**. The pair may not be supported or APIs are unavailable."
            )

    async def handle_pricetest_callback(self, client: Client,
                                        callback_query: CallbackQuery):
        data = callback_query.data

        if data == "pricetest_cancel":
            await callback_query.message.edit_text("Price test cancelled.")
            await callback_query.answer()
            return

        if data == "pricetest_custom":
            self.awaiting_custom_pair[callback_query.from_user.id] = {
                'type': 'pricetest',
                'message_id': callback_query.message.id
            }
            await callback_query.message.edit_text(
                "**Price Test - Custom Pair**\n\n"
                "Type the trading pair you want to check (e.g., EURUSD, GBPJPY):"
            )
            await callback_query.answer()
            return

        if data.startswith("pricetest_"):
            pair = data.replace("pricetest_", "").upper()
            await callback_query.message.edit_text(
                f"Fetching live price for **{pair}**...")

            price = await self.get_live_price(pair)

            if price:
                pair_name = PAIR_CONFIG.get(pair, {}).get('name', pair)
                decimals = PAIR_CONFIG.get(pair, {}).get('decimals', 5)
                await callback_query.message.edit_text(
                    f"**Price Test: {pair_name}**\n\nLive Price: **{price:.{decimals}f}**"
                )
            else:
                await callback_query.message.edit_text(
                    f"Could not retrieve price for **{pair}**. The pair may not be supported or APIs are unavailable."
                )

        await callback_query.answer()

    async def handle_db_status(self, client: Client, message: Message):
        if not await self.is_owner(message.from_user.id):
            return

        status = "**Database Status**\n\n"

        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    result = await conn.fetchval("SELECT 1")
                    active_trades = await conn.fetchval(
                        "SELECT COUNT(*) FROM active_trades")
                    active_members = await conn.fetchval(
                        "SELECT COUNT(*) FROM active_members")
                    dm_scheduled = await conn.fetchval(
                        "SELECT COUNT(*) FROM dm_schedule")
                    role_history = await conn.fetchval(
                        "SELECT COUNT(*) FROM role_history")

                status += (f"**Connection:** Connected\n"
                           f"**Active Trades:** {active_trades}\n"
                           f"**Trial Members:** {active_members}\n"
                           f"**DM Scheduled:** {dm_scheduled}\n"
                           f"**Anti-abuse Records:** {role_history}\n")
            except Exception as e:
                status += f"**Connection:** Error - {str(e)}\n"
        else:
            status += "**Connection:** Not connected\n"

        status += f"\n**In-Memory Trades:** {len(PRICE_TRACKING_CONFIG['active_trades'])}"

        await message.reply(status)

    async def handle_dm_status(self, client: Client, message: Message):
        if not await self.is_owner(message.from_user.id):
            return

        if not self.db_pool:
            await message.reply("Database not connected.")
            return

        try:
            async with self.db_pool.acquire() as conn:
                total = await conn.fetchval("SELECT COUNT(*) FROM dm_schedule")
                dm_3_sent = await conn.fetchval(
                    "SELECT COUNT(*) FROM dm_schedule WHERE dm_3_sent = TRUE")
                dm_7_sent = await conn.fetchval(
                    "SELECT COUNT(*) FROM dm_schedule WHERE dm_7_sent = TRUE")
                dm_14_sent = await conn.fetchval(
                    "SELECT COUNT(*) FROM dm_schedule WHERE dm_14_sent = TRUE")
                pending_3 = await conn.fetchval(
                    "SELECT COUNT(*) FROM dm_schedule WHERE dm_3_sent = FALSE")
                pending_7 = await conn.fetchval(
                    "SELECT COUNT(*) FROM dm_schedule WHERE dm_7_sent = FALSE AND dm_3_sent = TRUE"
                )
                pending_14 = await conn.fetchval(
                    "SELECT COUNT(*) FROM dm_schedule WHERE dm_14_sent = FALSE AND dm_7_sent = TRUE"
                )

            status = (f"**DM Status Report**\n\n"
                      f"**Total Tracked Users:** {total}\n\n"
                      f"**3-Day Follow-up:**\n"
                      f"  Sent: {dm_3_sent} | Pending: {pending_3}\n\n"
                      f"**7-Day Follow-up:**\n"
                      f"  Sent: {dm_7_sent} | Pending: {pending_7}\n\n"
                      f"**14-Day Follow-up:**\n"
                      f"  Sent: {dm_14_sent} | Pending: {pending_14}\n")

            await message.reply(status)
        except Exception as e:
            await message.reply(f"Error fetching DM status: {str(e)}")

    async def handle_timed_auto_role(self, client: Client, message: Message):
        if not await self.is_owner(message.from_user.id):
            return

        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("View Stats", callback_data="tar_status")],
             [
                 InlineKeyboardButton("List Active Users",
                                      callback_data="tar_list")
             ], [InlineKeyboardButton("Cancel", callback_data="tar_cancel")]])

        await message.reply("**Trial System Menu**\n\n"
                            "Select an option:",
                            reply_markup=keyboard)

    async def handle_timedautorole_callback(self, client: Client,
                                            callback_query: CallbackQuery):
        user_id = callback_query.from_user.id

        if not await self.is_owner(user_id):
            await callback_query.answer("This is restricted to the bot owner.",
                                        show_alert=True)
            return

        data = callback_query.data

        if data == "tar_cancel":
            await callback_query.message.edit_text("Menu closed.")
            await callback_query.answer()
            return

        if data == "tar_status":
            active_count = len(AUTO_ROLE_CONFIG['active_members'])
            history_count = len(AUTO_ROLE_CONFIG['role_history'])

            status = (
                f"**Trial System Status**\n\n"
                f"**Status:** {'Enabled' if AUTO_ROLE_CONFIG['enabled'] else 'Disabled'}\n"
                f"**Duration:** Exactly 3 trading days (Mon-Fri only)\n"
                f"**Expiry Time:** 22:59 on the 3rd trading day\n\n"
                f"**Active Trials:** {active_count}\n"
                f"**Anti-abuse Records:** {history_count}\n")
            await callback_query.message.edit_text(status)

        elif data == "tar_list":
            if not AUTO_ROLE_CONFIG['active_members']:
                await callback_query.message.edit_text(
                    "No active trial members.")
                await callback_query.answer()
                return

            response = "**Active Trial Members**\n\n"
            current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)

            for member_id, data_item in list(
                    AUTO_ROLE_CONFIG['active_members'].items())[:20]:
                expiry = datetime.fromisoformat(
                    data_item.get('expiry_time', current_time.isoformat()))
                if expiry.tzinfo is None:
                    expiry = AMSTERDAM_TZ.localize(expiry)

                time_left = expiry - current_time
                total_seconds = max(0, time_left.total_seconds())
                hours = int(total_seconds // 3600)
                minutes = int((total_seconds % 3600) // 60)

                weekend = " (weekend)" if data_item.get(
                    'weekend_delayed') else ""
                response += f"User {member_id}: {hours}h {minutes}m left{weekend}\n"

            if len(AUTO_ROLE_CONFIG['active_members']) > 20:
                response += f"\n_...and {len(AUTO_ROLE_CONFIG['active_members']) - 20} more_"

            await callback_query.message.edit_text(response)

        await callback_query.answer()

    async def handle_retract_trial(self, client: Client, message: Message):
        if not await self.is_owner(message.from_user.id):
            await message.reply(
                "❌ This command can only be used by the bot owner.")
            return

        if not AUTO_ROLE_CONFIG['active_members']:
            await message.reply("No active trial members found.")
            return

        # Initialize mappings if needed
        if not hasattr(self, 'retracttrial_mappings'):
            self.retracttrial_mappings = {}

        menu_id = str(message.id)[-8:]
        user_mapping = {}
        buttons = []

        # Show first 20 active trial members
        for idx, (user_id_str, member_data) in enumerate(list(
                AUTO_ROLE_CONFIG['active_members'].items())[:20]):
            user_mapping[str(idx)] = user_id_str
            expiry = datetime.fromisoformat(member_data.get('expiry_time', ''))
            if expiry.tzinfo is None:
                expiry = AMSTERDAM_TZ.localize(expiry)

            total_seconds = max(0, (expiry - datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)).total_seconds())
            hours = int(total_seconds // 3600)
            minutes = int((total_seconds % 3600) // 60)

            buttons.append([
                InlineKeyboardButton(
                    f"User {user_id_str} ({hours}h {minutes}m left)",
                    callback_data=f"retrt_{menu_id}_select_{idx}")
            ])

        self.retracttrial_mappings[menu_id] = user_mapping
        buttons.append([
            InlineKeyboardButton("Cancel",
                                 callback_data=f"retrt_{menu_id}_cancel")
        ])

        keyboard = InlineKeyboardMarkup(buttons)
        await message.reply(
            "**Retract Trial Time**\n\n"
            "Select a user to adjust their trial expiry time:",
            reply_markup=keyboard)

    async def handle_retracttrial_callback(self, client: Client,
                                           callback_query: CallbackQuery):
        user_id = callback_query.from_user.id

        if not await self.is_owner(user_id):
            await callback_query.answer("This is restricted to the bot owner.",
                                        show_alert=True)
            return

        data = callback_query.data

        if not data.startswith("retrt_"):
            return

        parts = data.split("_")
        if len(parts) < 3:
            await callback_query.answer("Invalid callback data.",
                                        show_alert=True)
            return

        menu_id = parts[1]
        action_type = parts[2]

        if not hasattr(self, 'retracttrial_mappings'):
            self.retracttrial_mappings = {}

        if action_type == "cancel":
            self.retracttrial_mappings.pop(menu_id, None)
            await callback_query.message.edit_text("Cancelled.")
            await callback_query.answer()
            return

        if action_type == "select":
            idx = parts[3] if len(parts) > 3 else None
            if not idx:
                await callback_query.answer("Invalid selection.",
                                            show_alert=True)
                return

            user_mapping = self.retracttrial_mappings.get(menu_id, {})
            selected_user_id_str = user_mapping.get(idx)

            if not selected_user_id_str:
                await callback_query.answer("User not found.",
                                            show_alert=True)
                return

            # Show time adjustment menu
            buttons = []
            for hours in [1, 2, 3, 6, 12, 24]:
                buttons.append([
                    InlineKeyboardButton(
                        f"Reduce {hours}h",
                        callback_data=f"retrt_{menu_id}_reduce_{idx}_h{hours}")
                ])

            buttons.append([
                InlineKeyboardButton("Custom Hours/Minutes",
                                     callback_data=f"retrt_{menu_id}_custom_{idx}")
            ])
            buttons.append([
                InlineKeyboardButton("Back",
                                     callback_data=f"retrt_{menu_id}_back")
            ])
            buttons.append([
                InlineKeyboardButton("Cancel",
                                     callback_data=f"retrt_{menu_id}_cancel")
            ])

            keyboard = InlineKeyboardMarkup(buttons)
            await callback_query.message.edit_text(
                f"**Reducing trial for User {selected_user_id_str}**\n\n"
                f"Select hours to subtract:",
                reply_markup=keyboard)
            await callback_query.answer()
            return

        if action_type == "reduce":
            idx = parts[3] if len(parts) > 3 else None
            hours_str = parts[4] if len(parts) > 4 else None

            if not idx or not hours_str or not hours_str.startswith("h"):
                await callback_query.answer("Invalid action.",
                                            show_alert=True)
                return

            try:
                hours = int(hours_str[1:])
                minutes = hours * 60

                user_mapping = self.retracttrial_mappings.get(menu_id, {})
                user_id_str = user_mapping.get(idx)

                if not user_id_str or user_id_str not in AUTO_ROLE_CONFIG[
                        'active_members']:
                    await callback_query.answer("User not found.",
                                                show_alert=True)
                    return

                member_data = AUTO_ROLE_CONFIG['active_members'][user_id_str]
                old_expiry = datetime.fromisoformat(member_data['expiry_time'])
                if old_expiry.tzinfo is None:
                    old_expiry = AMSTERDAM_TZ.localize(old_expiry)

                new_expiry = old_expiry - timedelta(minutes=minutes)
                member_data['expiry_time'] = new_expiry.isoformat()

                # Update database with integer user_id
                if self.db_pool:
                    async with self.db_pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE active_members SET expiry_time = $1 WHERE member_id = $2",
                            new_expiry, int(user_id_str))

                await self.save_auto_role_config()

                self.retracttrial_mappings.pop(menu_id, None)
                await callback_query.message.edit_text(
                    f"✅ Subtracted {hours} hours from user {user_id_str}\n"
                    f"New expiry: {new_expiry.strftime('%Y-%m-%d %H:%M:%S')}")
                await callback_query.answer()

            except (ValueError, IndexError):
                await callback_query.answer("Error processing request.",
                                            show_alert=True)
            except Exception as e:
                await callback_query.message.edit_text(
                    f"Error updating trial: {str(e)}")
                await callback_query.answer()
            return

        if action_type == "custom":
            # Handle custom hours/minutes input
            idx = parts[3] if len(parts) > 3 else None
            if not idx:
                await callback_query.answer("Invalid selection.",
                                            show_alert=True)
                return

            user_mapping = self.retracttrial_mappings.get(menu_id, {})
            selected_user_id_str = user_mapping.get(idx)

            if not selected_user_id_str:
                await callback_query.answer("User not found.",
                                            show_alert=True)
                return

            # Ask for input - store the context
            if not hasattr(self, 'awaiting_retracttrial_input'):
                self.awaiting_retracttrial_input = {}

            self.awaiting_retracttrial_input[callback_query.from_user.id] = {
                'menu_id': menu_id,
                'idx': idx,
                'user_id_str': selected_user_id_str
            }

            buttons = [[
                InlineKeyboardButton("Cancel",
                                     callback_data=f"retrt_{menu_id}_cancel")
            ]]
            keyboard = InlineKeyboardMarkup(buttons)

            await callback_query.message.edit_text(
                f"**Custom Time for User {selected_user_id_str}**\n\n"
                f"Send me the time to subtract.\n\n"
                f"Examples:\n"
                f"• `5h` (5 hours)\n"
                f"• `30m` (30 minutes)\n"
                f"• `2h 15m` (2 hours 15 minutes)\n\n"
                f"Just type the number and unit (h or m).",
                reply_markup=keyboard)
            await callback_query.answer()
            return

        if action_type == "applycustom":
            # Apply custom time reduction
            idx = parts[3] if len(parts) > 3 else None
            time_str = parts[4] if len(parts) > 4 else None

            if not idx or not time_str:
                await callback_query.answer("Invalid action.",
                                            show_alert=True)
                return

            user_mapping = self.retracttrial_mappings.get(menu_id, {})
            user_id_str = user_mapping.get(idx)

            if not user_id_str or user_id_str not in AUTO_ROLE_CONFIG[
                    'active_members']:
                await callback_query.answer("User not found.",
                                            show_alert=True)
                return

            try:
                if time_str.startswith("h"):
                    minutes = int(time_str[1:]) * 60
                    label = f"{int(time_str[1:])}h"
                elif time_str.startswith("m"):
                    minutes = int(time_str[1:])
                    label = f"{minutes} min"
                else:
                    await callback_query.answer("Invalid time format.",
                                                show_alert=True)
                    return

                member_data = AUTO_ROLE_CONFIG['active_members'][user_id_str]
                old_expiry = datetime.fromisoformat(member_data['expiry_time'])
                if old_expiry.tzinfo is None:
                    old_expiry = AMSTERDAM_TZ.localize(old_expiry)

                new_expiry = old_expiry - timedelta(minutes=minutes)
                member_data['expiry_time'] = new_expiry.isoformat()

                # Update database with integer user_id
                if self.db_pool:
                    async with self.db_pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE active_members SET expiry_time = $1 WHERE member_id = $2",
                            new_expiry, int(user_id_str))

                await self.save_auto_role_config()
                self.retracttrial_mappings.pop(menu_id, None)

                await callback_query.message.edit_text(
                    f"✅ Subtracted {label} from user {user_id_str}\n"
                    f"New expiry: {new_expiry.strftime('%Y-%m-%d %H:%M:%S')}")
                await callback_query.answer()

            except (ValueError, IndexError):
                await callback_query.answer("Error processing request.",
                                            show_alert=True)
            except Exception as e:
                await callback_query.message.edit_text(
                    f"Error updating trial: {str(e)}")
                await callback_query.answer()
            return

        if action_type == "back":
            # Return to user selection menu
            if not hasattr(self, 'retracttrial_mappings'):
                self.retracttrial_mappings = {}

            buttons = []
            for idx, user_id_str in self.retracttrial_mappings.get(
                    menu_id, {}).items():
                if user_id_str in AUTO_ROLE_CONFIG['active_members']:
                    member_data = AUTO_ROLE_CONFIG['active_members'][
                        user_id_str]
                    expiry = datetime.fromisoformat(
                        member_data.get('expiry_time', ''))
                    if expiry.tzinfo is None:
                        expiry = AMSTERDAM_TZ.localize(expiry)

                    total_seconds = max(0, (expiry - datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)).
                                    total_seconds())
                    hours = int(total_seconds // 3600)
                    minutes = int((total_seconds % 3600) // 60)

                    buttons.append([
                        InlineKeyboardButton(
                            f"User {user_id_str} ({hours}h {minutes}m left)",
                            callback_data=f"retrt_{menu_id}_select_{idx}")
                    ])

            buttons.append([
                InlineKeyboardButton("Cancel",
                                     callback_data=f"retrt_{menu_id}_cancel")
            ])

            keyboard = InlineKeyboardMarkup(buttons)
            await callback_query.message.edit_text(
                "**Retract Trial Time**\n\n"
                "Select a user to adjust their trial expiry time:",
                reply_markup=keyboard)
            await callback_query.answer()
            return

        await callback_query.answer()

    async def handle_clear_member(self, client: Client, message: Message):
        """Remove a user from all trial tracking tables (active_members, role_history, dm_schedule, weekend_pending)"""
        if not await self.is_owner(message.from_user.id):
            await message.reply(
                "❌ This command can only be used by the bot owner.")
            return

        try:
            parts = message.text.split()
            if len(parts) < 2:
                await message.reply(
                    "Usage: /clearmember <user_id>\n\nExample: /clearmember 5115200383"
                )
                return

            user_id = parts[1]
            user_id_str = str(user_id)

            # Remove from memory
            AUTO_ROLE_CONFIG['active_members'].pop(user_id_str, None)
            AUTO_ROLE_CONFIG['role_history'].pop(user_id_str, None)
            AUTO_ROLE_CONFIG['dm_schedule'].pop(user_id_str, None)
            AUTO_ROLE_CONFIG['weekend_pending'].pop(user_id_str, None)

            # Remove from database
            if self.db_pool:
                async with self.db_pool.acquire() as conn:
                    await conn.execute(
                        "DELETE FROM active_members WHERE member_id = $1",
                        int(user_id))
                    await conn.execute(
                        "DELETE FROM role_history WHERE member_id = $1",
                        int(user_id))
                    await conn.execute(
                        "DELETE FROM dm_schedule WHERE member_id = $1",
                        int(user_id))
                    await conn.execute(
                        "DELETE FROM weekend_pending WHERE member_id = $1",
                        int(user_id))

            await self.save_auto_role_config()
            await message.reply(
                f"✅ User {user_id} removed from all trial tracking")

        except Exception as e:
            await message.reply(f"Error: {str(e)}")

    async def process_join_request(self, client: Client,
                                   join_request: ChatJoinRequest):
        # Debug: always log that we received a join request
        chat_name = getattr(join_request.chat, 'title', 'Unknown')
        logger.info(f"📨 Join request received from {join_request.from_user.first_name} (ID: {join_request.from_user.id}) in chat {chat_name} (ID: {join_request.chat.id})")
        
        # Validate VIP_GROUP_ID
        if VIP_GROUP_ID == 0:
            logger.error("❌ VIP_GROUP_ID not set! Join requests cannot be processed.")
            return
        
        if join_request.chat.id != VIP_GROUP_ID:
            logger.debug(f"Join request is for different group (got {join_request.chat.id}, expected {VIP_GROUP_ID})")
            return

        try:
            user_id = join_request.from_user.id
            user_id_str = str(user_id)
            user_name = join_request.from_user.first_name or str(user_id)
            
            # Check if user already used their trial BEFORE approving
            has_used_trial = user_id_str in AUTO_ROLE_CONFIG['role_history']
            
            if not has_used_trial and self.db_pool:
                try:
                    async with self.db_pool.acquire() as conn:
                        db_history = await conn.fetchrow(
                            "SELECT * FROM role_history WHERE member_id = $1",
                            user_id)
                        if db_history:
                            has_used_trial = True
                except Exception as e:
                    logger.error(f"Error checking role_history: {e}")
            
            # Reject if they already used trial
            if has_used_trial:
                await self.log_to_debug(
                    f"❌ Rejected join request from {user_name} (ID: {user_id}) - trial already used before")
                
                try:
                    await join_request.decline()
                    logger.info(f"✅ Declined join request for {user_name} (ID: {user_id})")
                except Exception as e:
                    logger.error(f"Error declining join request: {e}")
                
                # Send friendly DM about re-using trial
                try:
                    rejection_dm = (
                        f"Hey {user_name}!\n\n"
                        f"Unfortunately, our free trial can only be used once per person. Your trial has already ran out, so we can't give you another.\n\n"
                        f"We truly hope that you were able to profit with us during your free trial. If you were happy with the results you got, then feel free to rejoin our VIP group through this link: https://whop.com/gold-pioneer/gold-pioneer/"
                    )
                    await client.send_message(user_id, rejection_dm, disable_web_page_preview=True)
                    logger.info(f"Sent rejection DM to {user_name}")
                except Exception as e:
                    logger.error(f"Could not send rejection DM to {user_name}: {e}")
                return
            
            # Track this user as trial approval (they're joining via trial link with approval)
            self.trial_pending_approvals.add(user_id)
            logger.info(f"✅ Added {user_id} to trial_pending_approvals set")
            
            # Actually approve the request
            await join_request.approve()
            logger.info(f"✅ Successfully approved join request for {user_name} (ID: {user_id})")
            
            await self.log_to_debug(
                f"🎯 Auto-approved trial join request from {user_name} (ID: {user_id}) - waiting for member join event..."
            )
        except Exception as e:
            logger.error(f"❌ Error processing join request from {join_request.from_user.first_name}: {type(e).__name__}: {e}")
            await self.log_to_debug(
                f"❌ Failed to process join request from {join_request.from_user.first_name} (ID: {join_request.from_user.id}): {e}"
            )

    async def process_member_update(self, client: Client,
                                    member_update: ChatMemberUpdated):
        if not member_update.new_chat_member:
            return

        old_status = member_update.old_chat_member.status if member_update.old_chat_member else None
        new_status = member_update.new_chat_member.status

        is_new_join = (old_status in [
            None, ChatMemberStatus.LEFT, ChatMemberStatus.BANNED
        ] and new_status in [
            ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER
        ])

        if not is_new_join:
            return

        user = member_update.new_chat_member.user
        chat_id = member_update.chat.id

        if chat_id == FREE_GROUP_ID:
            await self.handle_free_group_join(client, user)

        elif chat_id == VIP_GROUP_ID:
            invite_link = member_update.invite_link
            await self.handle_vip_group_join(client, user, invite_link)

    async def handle_free_group_join(self, client: Client, user):
        await self.log_to_debug(
            f"New member joined FREE group: {user.first_name} (ID: {user.id})")

        welcome_dm = (
            f"**Hey {user.first_name}, Welcome to FX Pip Pioneers!**\n\n"
            f"**Want to try our VIP Group for FREE?**\n"
            f"We're offering a **3-day free trial** of our VIP Group where you'll receive "
            f"**6+ high-quality trade signals per day**.\n\n"
            f"**Activate your free trial here:** https://t.me/+uM_Ug2wTKFpiMDVk\n\n"
            f"Good luck trading!")

        await asyncio.sleep(3)

        try:
            await client.send_message(user.id, welcome_dm)
            await self.log_to_debug(
                f"Sent welcome DM to {user.first_name} about VIP trial")
        except Exception as e:
            logger.error(
                f"Could not send welcome DM to {user.first_name}: {e}")

    async def handle_vip_group_join(self, client: Client, user, invite_link):
        """
        Handle VIP group joins. Register trial users (those with approved join requests).
        Paid members joining via main link are not registered/tracked.
        """
        user_id_str = str(user.id)
        current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)

        # Determine if this is a trial user (they were approved for trial access)
        is_trial_user = user.id in self.trial_pending_approvals

        # Remove from pending so we don't register twice
        self.trial_pending_approvals.discard(user.id)

        # If not trial user, don't register - they're paying members
        if not is_trial_user:
            await self.log_to_debug(
                f"💳 {user.first_name} (ID: {user.id}) joined via paid link - no trial registration needed"
            )
            return

        await self.log_to_debug(
            f"🆓 Trial join detected: {user.first_name} (ID: {user.id})")

        # Check if this user has already used their trial
        has_used_trial = user_id_str in AUTO_ROLE_CONFIG['role_history']
        db_check_failed = False

        if not has_used_trial and self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    db_history = await conn.fetchrow(
                        "SELECT * FROM role_history WHERE member_id = $1",
                        user.id)
                    if db_history:
                        has_used_trial = True
                        AUTO_ROLE_CONFIG['role_history'][user_id_str] = {
                            'first_granted':
                            db_history['first_granted'].isoformat()
                            if db_history['first_granted'] else None,
                            'times_granted':
                            db_history['times_granted'] or 1,
                            'last_expired':
                            db_history['last_expired'].isoformat()
                            if db_history['last_expired'] else None
                        }
            except Exception as e:
                logger.error(f"Error checking role_history in database: {e}")
                db_check_failed = True

        if db_check_failed:
            await self.log_to_debug(
                f"⚠️ Database check failed for {user.first_name} - allowing access but logging warning"
            )

        # If user already used trial once, they shouldn't have gotten past join request approval
        # But as a safety check if they somehow made it here, send them a message and log it
        if has_used_trial:
            await self.log_to_debug(
                f"⚠️ Warning: User {user.first_name} ({user.id}) joined despite trial already used - should have been rejected at join request")

            try:
                rejection_dm = (
                    f"Hey {user.first_name}!\n\n"
                    f"Unfortunately, our free trial can only be used once per person. Your trial has already ran out, so we can't give you another.\n\n"
                    f"We truly hope that you were able to profit with us during your free trial. If you were happy with the results you got, then feel free to rejoin our VIP group through this link: https://whop.com/gold-pioneer/gold-pioneer/"
                )
                await client.send_message(user.id, rejection_dm, disable_web_page_preview=True)
            except Exception as e:
                logger.error(f"Could not send DM to {user.first_name}: {e}")

            # Don't kick - they should have been rejected at join request stage
            return

        # New trial user - register for 3-day trial
        await self.log_to_debug(
            f"✅ Registering {user.first_name} for 3-day trial")

        # Calculate expiry time to ensure exactly 3 trading days
        expiry_time = self.calculate_trial_expiry_time(current_time)

        # Determine if joined during weekend for tracking
        is_weekend = self.is_weekend_time(current_time)

        AUTO_ROLE_CONFIG['active_members'][user_id_str] = {
            'joined_at': current_time.isoformat(),
            'expiry_time': expiry_time.isoformat(),
            'weekend_delayed': is_weekend,
            'chat_id': VIP_GROUP_ID
        }

        AUTO_ROLE_CONFIG['role_history'][user_id_str] = {
            'first_granted': current_time.isoformat(),
            'times_granted': 1,
            'last_expired': None
        }

        await self.save_auto_role_config()

        welcome_msg = (
            f"**Welcome to FX Pip Pioneers!** As a welcome gift, we've given you "
            f"**access to the VIP Group for 3 trading days.** "
            f"Your access will expire on {expiry_time.strftime('%A at %H:%M')}. "
            f"Good luck trading!")

        try:
            await client.send_message(user.id, welcome_msg)
        except Exception as e:
            logger.error(
                f"Could not send welcome DM to {user.first_name}: {e}")

    async def get_working_api_for_pair(self, pair: str) -> str:
        pair_clean = pair.upper().replace("/",
                                          "").replace("-",
                                                      "").replace("_", "")

        for api_name in PRICE_TRACKING_CONFIG['api_priority_order']:
            try:
                api_key = PRICE_TRACKING_CONFIG['api_keys'].get(
                    f"{api_name}_key")
                if not api_key:
                    continue

                price = await self.get_price_from_api(api_name, pair_clean)
                if price is not None:
                    logger.info(
                        f"API assignment: {pair_clean} will use {api_name}")
                    return api_name
            except Exception as e:
                logger.warning(
                    f"{api_name} failed for {pair_clean}: {str(e)[:100]}")
                continue

        logger.warning(
            f"All APIs failed for {pair_clean}, defaulting to currencybeacon")
        return "currencybeacon"

    async def get_live_price(self, pair: str) -> Optional[float]:
        pair_clean = pair.upper().replace("/",
                                          "").replace("-",
                                                      "").replace("_", "")

        for api_name in PRICE_TRACKING_CONFIG['api_priority_order']:
            try:
                price = await self.get_price_from_api(api_name, pair_clean)
                if price:
                    return price
            except Exception as e:
                logger.error(f"Error getting price from {api_name}: {e}")
                continue

        return None

    async def get_price_from_api(self, api_name: str,
                                 pair: str) -> Optional[float]:
        api_keys = PRICE_TRACKING_CONFIG['api_keys']

        try:
            if api_name == "currencybeacon":
                key = api_keys.get('currencybeacon_key')
                if not key:
                    return None

                if pair.startswith("XAU"):
                    base, quote = "XAU", pair[3:]
                elif pair.startswith("XAG"):
                    base, quote = "XAG", pair[3:]
                elif len(pair) == 6:
                    base, quote = pair[:3], pair[3:]
                else:
                    return None

                url = f"https://api.currencybeacon.com/v1/latest?api_key={key}&base={base}&symbols={quote}"

                async with aiohttp.ClientSession() as session:
                    async with session.get(
                            url, timeout=ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if 'rates' in data and quote in data['rates']:
                                return float(data['rates'][quote])

            elif api_name == "exchangerate_api":
                key = api_keys.get('exchangerate_api_key')
                if not key:
                    return None

                if len(pair) == 6:
                    base, quote = pair[:3], pair[3:]
                else:
                    return None

                url = f"https://v6.exchangerate-api.com/v6/{key}/pair/{base}/{quote}"

                async with aiohttp.ClientSession() as session:
                    async with session.get(
                            url, timeout=ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if 'conversion_rate' in data:
                                return float(data['conversion_rate'])

            elif api_name == "currencylayer":
                key = api_keys.get('currencylayer_key')
                if not key:
                    return None

                if len(pair) == 6:
                    base, quote = pair[:3], pair[3:]
                else:
                    return None

                url = f"https://api.currencylayer.com/live?access_key={key}&currencies={quote}&source={base}"

                async with aiohttp.ClientSession() as session:
                    async with session.get(
                            url, timeout=ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data.get('success') and 'quotes' in data:
                                rate_key = f"{base}{quote}"
                                if rate_key in data['quotes']:
                                    return float(data['quotes'][rate_key])

            elif api_name == "abstractapi":
                key = api_keys.get('abstractapi_key')
                if not key:
                    return None

                if len(pair) == 6:
                    base, quote = pair[:3], pair[3:]
                else:
                    return None

                url = f"https://exchange-rates.abstractapi.com/v1/live?api_key={key}&base={base}&target={quote}"

                async with aiohttp.ClientSession() as session:
                    async with session.get(
                            url, timeout=ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if 'exchange_rates' in data and quote in data[
                                    'exchange_rates']:
                                return float(data['exchange_rates'][quote])

        except asyncio.TimeoutError:
            logger.warning(f"Timeout getting price from {api_name}")
        except Exception as e:
            logger.error(f"Error with {api_name}: {e}")

        return None

    async def check_single_trade_immediately(self, message_id: str,
                                             trade_data: dict):
        await asyncio.sleep(5)
        await self.check_price_levels(message_id, trade_data)

    async def check_price_levels(self, message_id: str, trade_data: dict):
        pair = trade_data.get('pair')
        action = trade_data.get('action')
        tp1 = trade_data.get('tp1_price')
        tp2 = trade_data.get('tp2_price')
        tp3 = trade_data.get('tp3_price')
        sl = trade_data.get('sl_price')
        tp_hits = trade_data.get('tp_hits', [])
        breakeven_active = trade_data.get('breakeven_active', False)
        live_entry = trade_data.get('live_entry') or trade_data.get('entry')

        current_price = await self.get_live_price(pair)
        if not current_price:
            return

        is_buy = action.upper() == "BUY"

        if is_buy:
            if breakeven_active and live_entry:
                if current_price <= live_entry:
                    await self.handle_breakeven_hit(message_id, trade_data)
                    return
            elif current_price <= sl:
                await self.handle_sl_hit(message_id, trade_data, current_price)
                return

            if 'TP1' not in tp_hits and current_price >= tp1:
                await self.handle_tp_hit(message_id, trade_data, 'TP1',
                                         current_price)
            if 'TP2' not in tp_hits and current_price >= tp2:
                await self.handle_tp_hit(message_id, trade_data, 'TP2',
                                         current_price)
            if 'TP3' not in tp_hits and current_price >= tp3:
                await self.handle_tp_hit(message_id, trade_data, 'TP3',
                                         current_price)
                return
        else:
            if breakeven_active and live_entry:
                if current_price >= live_entry:
                    await self.handle_breakeven_hit(message_id, trade_data)
                    return
            elif current_price >= sl:
                await self.handle_sl_hit(message_id, trade_data, current_price)
                return

            if 'TP1' not in tp_hits and current_price <= tp1:
                await self.handle_tp_hit(message_id, trade_data, 'TP1',
                                         current_price)
            if 'TP2' not in tp_hits and current_price <= tp2:
                await self.handle_tp_hit(message_id, trade_data, 'TP2',
                                         current_price)
            if 'TP3' not in tp_hits and current_price <= tp3:
                await self.handle_tp_hit(message_id, trade_data, 'TP3',
                                         current_price)
                return

    async def handle_tp_hit(self, message_id: str, trade_data: dict,
                            tp_level: str, hit_price: float):
        trade = PRICE_TRACKING_CONFIG['active_trades'].get(message_id)
        if not trade:
            return

        if tp_level in trade.get('tp_hits', []):
            return

        trade['tp_hits'] = trade.get('tp_hits', []) + [tp_level]

        await self.send_tp_notification(message_id, trade, tp_level, hit_price)

        if tp_level == 'TP2' and not trade.get('breakeven_active'):
            trade['breakeven_active'] = True
            await self.send_breakeven_notification(message_id, trade)

        if tp_level == 'TP3':
            trade['status'] = 'completed'
            del PRICE_TRACKING_CONFIG['active_trades'][message_id]
            await self.remove_trade_from_db(message_id, 'tp3_hit')
        else:
            await self.update_trade_in_db(message_id, trade)

        await self.log_to_debug(
            f"{trade['pair']} {trade['action']} hit {tp_level} @ {hit_price:.5f}"
        )

    async def handle_sl_hit(self, message_id: str, trade_data: dict,
                            hit_price: float):
        trade = PRICE_TRACKING_CONFIG['active_trades'].get(message_id)
        if not trade:
            return

        trade['status'] = 'sl_hit'

        await self.send_sl_notification(message_id, trade, hit_price)

        del PRICE_TRACKING_CONFIG['active_trades'][message_id]
        await self.remove_trade_from_db(message_id, 'sl_hit')

        await self.log_to_debug(
            f"{trade['pair']} {trade['action']} hit SL @ {hit_price:.5f}")

    async def handle_breakeven_hit(self, message_id: str, trade_data: dict):
        trade = PRICE_TRACKING_CONFIG['active_trades'].get(message_id)
        if not trade:
            return

        trade['status'] = 'breakeven'

        pair = trade.get('pair', 'Unknown')
        action = trade.get('action', 'Unknown')
        live_entry = trade.get('live_entry') or trade.get(
            'entry') or trade.get('entry_price', 0)
        tp_hits = trade.get('tp_hits', [])

        tp_status = f"TPs hit: {', '.join(tp_hits)}" if tp_hits else ""

        notification = (f"**BREAKEVEN HIT** {pair} {action}\n\n"
                        f"Price returned to entry ({live_entry:.5f})\n"
                        f"{tp_status}\n"
                        f"Trade closed at breakeven.")

        chat_id = trade.get('chat_id') or trade.get('channel_id')
        if chat_id:
            original_msg_id = trade.get('message_id', message_id)
            if '_' in str(original_msg_id):
                original_msg_id = str(original_msg_id).split('_', 1)[1]

            try:
                await self.app.send_message(
                    chat_id,
                    notification,
                    reply_to_message_id=int(original_msg_id))
            except Exception as e:
                logger.error(
                    f"Failed to send breakeven hit notification to {chat_id}: {e}"
                )
                try:
                    await self.app.send_message(chat_id, notification)
                except Exception as e2:
                    logger.error(
                        f"Failed to send breakeven hit notification without reply: {e2}"
                    )

        del PRICE_TRACKING_CONFIG['active_trades'][message_id]
        await self.remove_trade_from_db(message_id, 'breakeven_hit')

        await self.log_to_debug(
            f"{trade['pair']} {trade['action']} hit breakeven @ {entry_price:.5f}"
        )

    async def send_tp_notification(self, message_id: str, trade_data: dict,
                                   tp_level: str, hit_price: float):
        pair = trade_data.get('pair', 'Unknown')
        action = trade_data.get('action', 'Unknown')

        tp1_messages = [
            "TP1 has been hit. First target secured, let's keep it going. Next stop: TP2 📈🔥",
            "TP1 smashed. Secure some profits if you'd like and let's aim for TP2 🎯💪",
            "We've just hit TP1. Nice start. The current momentum is looking good for TP2 🚀📊",
            "TP1 has been hit! Keep your eyes on the next level. TP2 up next 👀💸",
            "First milestone hit. The trade is off to a clean start 📉➡️📈",
            "TP1 has been reached. Let's keep the discipline and push for TP2 💼🔁",
            "First TP level hit! TP1 is in. Stay focused as we aim for TP2 & TP3! 💹🚀",
            "TP1 locked in. Let's keep monitoring price action and go for TP2 💰📍",
            "TP1 has been reached. Trade is moving as planned. Next stop: TP2 🔄📊",
            "TP1 hit. Great entry. now let's trail it smart toward TP2 🧠📈"
        ]

        tp2_messages = [
            "TP1 & TP2 have both been hit 🚀🚀 move your SL to breakeven and lets get TP3 💸",
            "TP2 has been hit 🚀🚀 move your SL to breakeven and lets get TP3 💸",
            "TP2 has been hit 🚀🚀 move your sl to breakeven, partially close the trade and lets get tp3 🎯🎯🎯",
            "TP2 has been hit💸 please move your SL to breakeven, partially close the trade and lets go for TP3 🚀",
            "TP2 has been hit. Move your SL to breakeven and secure those profits. Let's push for TP3. we're not done yet 🚀💰",
            "TP2 has officially been smashed. Move SL to breakeven, partial close if you haven't already. TP3 is calling 📈🔥",
            "TP2 just got hit. Lock in those gains by moving your SL to breakeven. TP3 is the next target so let's stay sharp and ride this momentum 💪📊",
            "Another level cleared as TP2 has been hit. Shift SL to breakeven and lock it in. Eyes on TP3 now so let's finish strong 🧠🎯",
            "TP2 has been hit. Move your SL to breakeven immediately. This setup is moving clean and TP3 is well within reach 🚀🔒",
            "Great move traders, TP2 has been tagged. Time to shift SL to breakeven and secure the bag. TP3 is the final boss and we're coming for it 💼⚔️"
        ]

        tp3_messages = [
            "TP3 hit. Full target smashed, perfect execution 🔥🔥🔥",
            "Huge win, TP3 reached. Congrats to everyone who followed 📊🚀",
            "TP3 just got hit. Close it out and lock in profits 💸🎯",
            "TP3 tagged. That wraps up the full setup — solid trade 💪💼",
            "TP3 locked in. Flawless setup from entry to exit 🙌📈",
            "TP3 hit. This one went exactly as expected. Great job ✅💰",
            "TP3 has been reached. Hope you secured profits all the way through 🏁📊",
            "TP3 reached. Strategy and patience paid off big time 🔍🚀",
            "Final target hit. Huge win for FX Pip Pioneers 🔥💸",
            "TP3 secured. That's the result of following the plan 💼💎"
        ]

        if tp_level.lower() == "tp1":
            notification = random.choice(tp1_messages)
        elif tp_level.lower() == "tp2":
            notification = random.choice(tp2_messages)
        elif tp_level.lower() == "tp3":
            notification = random.choice(tp3_messages)
        else:
            notification = f"**{tp_level.upper()} HAS BEEN HIT!** 🎯"

        chat_id = trade_data.get('chat_id') or trade_data.get('channel_id')
        if not chat_id:
            logger.error(f"No chat_id found for trade {message_id}")
            return

        original_msg_id = trade_data.get('message_id', message_id)
        if '_' in str(original_msg_id):
            original_msg_id = str(original_msg_id).split('_', 1)[1]

        try:
            await self.app.send_message(
                chat_id,
                notification,
                reply_to_message_id=int(original_msg_id))
        except Exception as e:
            logger.error(f"Failed to send TP notification to {chat_id}: {e}")
            try:
                await self.app.send_message(chat_id, notification)
            except Exception as e2:
                logger.error(
                    f"Failed to send TP notification without reply: {e2}")

    async def send_sl_notification(self, message_id: str, trade_data: dict,
                                   hit_price: float):
        pair = trade_data.get('pair', 'Unknown')
        action = trade_data.get('action', 'Unknown')
        tp_hits = trade_data.get('tp_hits', [])

        sl_messages = [
            "This one hit SL. It happens. Let's stay focused and get the next one 🔄🧠",
            "SL has been hit. Risk was managed, we move on 💪📉",
            "This setup didn't go as planned and hit SL. On to the next 📊",
            "SL hit. It's all part of the process. Stay disciplined 💼📚",
            "SL hit. Losses are part of trading. We bounce back 📈⏭️",
            "SL hit. Trust the process and prepare for the next opportunity 🔄🧠",
            "SL was hit on this one. We took the loss, now let's stay sharp 🔁💪",
            "SL hit. It's part of the game. Let's stay focused on quality 📉🎯",
            "This trade hit SL. Discipline keeps us in the game. We'll get the loss back next trade💼🧘‍♂️",
            "SL triggered. Part of proper risk management. Next setup coming soon 💪⚡"
        ]

        notification = random.choice(sl_messages)

        chat_id = trade_data.get('chat_id') or trade_data.get('channel_id')
        if not chat_id:
            logger.error(f"No chat_id found for trade {message_id}")
            return

        original_msg_id = trade_data.get('message_id', message_id)
        if '_' in str(original_msg_id):
            original_msg_id = str(original_msg_id).split('_', 1)[1]

        try:
            await self.app.send_message(
                chat_id,
                notification,
                reply_to_message_id=int(original_msg_id))
        except Exception as e:
            logger.error(f"Failed to send SL notification to {chat_id}: {e}")
            try:
                await self.app.send_message(chat_id, notification)
            except Exception as e2:
                logger.error(
                    f"Failed to send SL notification without reply: {e2}")

    async def send_breakeven_notification(self, message_id: str,
                                          trade_data: dict):
        pair = trade_data.get('pair', 'Unknown')
        action = trade_data.get('action', 'Unknown')
        entry_price = trade_data.get('entry_price', 0)

        breakeven_messages = [
            "TP2 has been hit & price has reversed to breakeven, so as usual, we're out safe 🫡",
            "Price returned to breakeven after hitting TP2. Smart exit, we secured profits and protected capital 💼✅",
            "Breakeven reached after TP2 hit. Clean trade management - we're out with gains secured 🎯🔒",
            "TP2 was hit, now back to breakeven. Perfect trade execution, we exit safe and profitable 📊🛡️",
            "Price reversed to entry after TP2. Textbook risk management - we're out with profits locked in 💰🧠",
            "Breakeven hit after TP2. Smart trading discipline pays off. We're out safe and ahead 🚀⚖️",
            "Back to breakeven post-TP2. This is how we protect profits. Clean exit, clean conscience 💎🔐",
            "TP2 secured, now at breakeven. Professional trade management - we exit with gains protected 📈🛡️",
            "Price action brought us back to entry after TP2. Strategic exit with profits in the bag 🎯💼",
            "Breakeven reached after TP2 hit. This is disciplined trading - we're out safe with profits secured 🧘‍♂️💸"
        ]

        notification = random.choice(breakeven_messages)

        chat_id = trade_data.get('chat_id') or trade_data.get('channel_id')
        if not chat_id:
            logger.error(f"No chat_id found for trade {message_id}")
            return

        original_msg_id = trade_data.get('message_id', message_id)
        if '_' in str(original_msg_id):
            original_msg_id = str(original_msg_id).split('_', 1)[1]

        try:
            await self.app.send_message(
                chat_id,
                notification,
                reply_to_message_id=int(original_msg_id))
        except Exception as e:
            logger.error(
                f"Failed to send breakeven notification to {chat_id}: {e}")
            try:
                await self.app.send_message(chat_id, notification)
            except Exception as e2:
                logger.error(
                    f"Failed to send breakeven notification without reply: {e2}"
                )

    async def init_database(self):
        try:
            database_url = os.getenv('DATABASE_URL')

            if not database_url:
                logger.warning(
                    "No database URL found - continuing without persistent memory"
                )
                return

            self.db_pool = await asyncpg.create_pool(
                database_url,
                min_size=1,
                max_size=5,
                command_timeout=30,
                server_settings={'application_name': 'telegram-trading-bot'})
            logger.info("PostgreSQL connection pool created")

            async with self.db_pool.acquire() as conn:
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS role_history (
                        member_id BIGINT PRIMARY KEY,
                        first_granted TIMESTAMP WITH TIME ZONE NOT NULL,
                        times_granted INTEGER DEFAULT 1,
                        last_expired TIMESTAMP WITH TIME ZONE,
                        guild_id BIGINT NOT NULL
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS active_members (
                        member_id BIGINT PRIMARY KEY,
                        role_added_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        role_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        weekend_delayed BOOLEAN DEFAULT FALSE,
                        expiry_time TIMESTAMP WITH TIME ZONE,
                        custom_duration BOOLEAN DEFAULT FALSE
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS weekend_pending (
                        member_id BIGINT PRIMARY KEY,
                        join_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        guild_id BIGINT NOT NULL
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS dm_schedule (
                        member_id BIGINT PRIMARY KEY,
                        role_expired TIMESTAMP WITH TIME ZONE NOT NULL,
                        guild_id BIGINT NOT NULL,
                        dm_3_sent BOOLEAN DEFAULT FALSE,
                        dm_7_sent BOOLEAN DEFAULT FALSE,
                        dm_14_sent BOOLEAN DEFAULT FALSE
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS pending_welcome_dms (
                        member_id BIGINT PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        joined_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        scheduled_send_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        sent BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS bot_status (
                        id INTEGER PRIMARY KEY DEFAULT 1,
                        last_online TIMESTAMP WITH TIME ZONE,
                        heartbeat_time TIMESTAMP WITH TIME ZONE,
                        CONSTRAINT tg_single_row_constraint UNIQUE (id)
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS active_trades (
                        message_id VARCHAR(100) PRIMARY KEY,
                        channel_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        pair VARCHAR(20) NOT NULL,
                        action VARCHAR(10) NOT NULL,
                        entry_price DECIMAL(30,15) NOT NULL,
                        tp1_price DECIMAL(30,15) NOT NULL,
                        tp2_price DECIMAL(30,15) NOT NULL,
                        tp3_price DECIMAL(30,15) NOT NULL,
                        sl_price DECIMAL(30,15) NOT NULL,
                        telegram_entry DECIMAL(30,15),
                        telegram_tp1 DECIMAL(30,15),
                        telegram_tp2 DECIMAL(30,15),
                        telegram_tp3 DECIMAL(30,15),
                        telegram_sl DECIMAL(30,15),
                        live_entry DECIMAL(30,15),
                        assigned_api VARCHAR(30) DEFAULT 'currencybeacon',
                        status VARCHAR(50) DEFAULT 'active',
                        tp_hits TEXT DEFAULT '',
                        breakeven_active BOOLEAN DEFAULT FALSE,
                        entry_type VARCHAR(30),
                        manual_overrides TEXT DEFAULT '',
                        channel_message_map TEXT DEFAULT '',
                        all_channel_ids TEXT DEFAULT '',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

                try:
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS telegram_entry DECIMAL(30,15)'
                    )
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS telegram_tp1 DECIMAL(30,15)'
                    )
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS telegram_tp2 DECIMAL(30,15)'
                    )
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS telegram_tp3 DECIMAL(30,15)'
                    )
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS telegram_sl DECIMAL(30,15)'
                    )
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS live_entry DECIMAL(30,15)'
                    )
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS assigned_api VARCHAR(30) DEFAULT \'currencybeacon\''
                    )
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS manual_overrides TEXT DEFAULT \'\''
                    )
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS channel_message_map TEXT DEFAULT \'\''
                    )
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS all_channel_ids TEXT DEFAULT \'\''
                    )
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS group_name VARCHAR(30) DEFAULT \'\''
                    )
                    await conn.execute(
                        'ALTER TABLE active_trades ALTER COLUMN message_id TYPE VARCHAR(100)'
                    )
                    await conn.execute(
                        'ALTER TABLE completed_trades ALTER COLUMN message_id TYPE VARCHAR(100)'
                    )
                except Exception:
                    pass

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS completed_trades (
                        message_id VARCHAR(100) PRIMARY KEY,
                        channel_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        pair VARCHAR(20) NOT NULL,
                        action VARCHAR(10) NOT NULL,
                        entry_price DECIMAL(30,15) NOT NULL,
                        tp1_price DECIMAL(30,15) NOT NULL,
                        tp2_price DECIMAL(30,15) NOT NULL,
                        tp3_price DECIMAL(30,15) NOT NULL,
                        sl_price DECIMAL(30,15) NOT NULL,
                        telegram_entry DECIMAL(30,15),
                        telegram_tp1 DECIMAL(30,15),
                        telegram_tp2 DECIMAL(30,15),
                        telegram_tp3 DECIMAL(30,15),
                        telegram_sl DECIMAL(30,15),
                        live_entry DECIMAL(30,15),
                        assigned_api VARCHAR(30) DEFAULT 'currencybeacon',
                        final_status VARCHAR(100) NOT NULL,
                        tp_hits TEXT DEFAULT '',
                        breakeven_active BOOLEAN DEFAULT FALSE,
                        entry_type VARCHAR(30),
                        manual_overrides TEXT DEFAULT '',
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        completed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        completion_reason VARCHAR(50) NOT NULL
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS missed_hits (
                        id SERIAL PRIMARY KEY,
                        message_id VARCHAR(20) NOT NULL,
                        hit_type VARCHAR(10) NOT NULL,
                        hit_level VARCHAR(10) NOT NULL,
                        hit_price DECIMAL(12,8) NOT NULL,
                        hit_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        processed BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

            logger.info("Database tables initialized")

            await self.load_config_from_db()
            await self.load_active_trades_from_db()

        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            self.db_pool = None

    async def load_config_from_db(self):
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                active_rows = await conn.fetch('SELECT * FROM active_members')
                for row in active_rows:
                    AUTO_ROLE_CONFIG["active_members"][str(
                        row['member_id'])] = {
                            "joined_at":
                            row['role_added_time'].isoformat(),
                            "expiry_time":
                            row['expiry_time'].isoformat()
                            if row['expiry_time'] else None,
                            "weekend_delayed":
                            row['weekend_delayed'],
                            "chat_id":
                            row['guild_id']
                        }

                history_rows = await conn.fetch('SELECT * FROM role_history')
                for row in history_rows:
                    AUTO_ROLE_CONFIG["role_history"][str(row['member_id'])] = {
                        "first_granted":
                        row['first_granted'].isoformat(),
                        "times_granted":
                        row['times_granted'],
                        "last_expired":
                        row['last_expired'].isoformat()
                        if row['last_expired'] else None
                    }

                dm_rows = await conn.fetch('SELECT * FROM dm_schedule')
                for row in dm_rows:
                    AUTO_ROLE_CONFIG["dm_schedule"][str(row['member_id'])] = {
                        "role_expired": row['role_expired'].isoformat(),
                        "dm_3_sent": row['dm_3_sent'],
                        "dm_7_sent": row['dm_7_sent'],
                        "dm_14_sent": row['dm_14_sent']
                    }

                weekend_rows = await conn.fetch('SELECT * FROM weekend_pending'
                                                )
                for row in weekend_rows:
                    AUTO_ROLE_CONFIG["weekend_pending"][str(
                        row['member_id'])] = {
                            "join_time": row['join_time'].isoformat(),
                            "chat_id": row['guild_id']
                        }

                logger.info(
                    f"Loaded {len(AUTO_ROLE_CONFIG['active_members'])} active members from database"
                )
                logger.info(
                    f"Loaded {len(AUTO_ROLE_CONFIG['role_history'])} role history records"
                )
                logger.info(
                    f"Loaded {len(AUTO_ROLE_CONFIG['weekend_pending'])} weekend pending members"
                )
        except Exception as e:
            logger.error(f"Error loading config from database: {e}")

    async def save_auto_role_config(self):
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                for member_id, data in AUTO_ROLE_CONFIG[
                        'active_members'].items():
                    joined_at = datetime.fromisoformat(data['joined_at'])
                    expiry_time = datetime.fromisoformat(
                        data['expiry_time']) if data.get(
                            'expiry_time') else None

                    await conn.execute(
                        '''
                        INSERT INTO active_members (member_id, role_added_time, role_id, guild_id, weekend_delayed, expiry_time)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (member_id) DO UPDATE SET
                        role_added_time = $2, role_id = $3, guild_id = $4, weekend_delayed = $5, expiry_time = $6
                    ''', int(member_id), joined_at, 0,
                        data.get('chat_id', VIP_GROUP_ID),
                        data.get('weekend_delayed', False), expiry_time)

                for member_id, data in AUTO_ROLE_CONFIG['role_history'].items(
                ):
                    first_granted = datetime.fromisoformat(
                        data['first_granted'])
                    last_expired = datetime.fromisoformat(
                        data['last_expired']) if data.get(
                            'last_expired') else None

                    await conn.execute(
                        '''
                        INSERT INTO role_history (member_id, first_granted, times_granted, last_expired, guild_id)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (member_id) DO UPDATE SET
                        first_granted = $2, times_granted = $3, last_expired = $4, guild_id = $5
                    ''', int(member_id), first_granted,
                        data.get('times_granted', 1), last_expired,
                        VIP_GROUP_ID)

                for member_id, data in AUTO_ROLE_CONFIG[
                        'weekend_pending'].items():
                    join_time = datetime.fromisoformat(data['join_time'])
                    await conn.execute(
                        '''
                        INSERT INTO weekend_pending (member_id, join_time, guild_id)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (member_id) DO UPDATE SET
                        join_time = $2, guild_id = $3
                    ''', int(member_id), join_time,
                        data.get('chat_id', VIP_GROUP_ID))

                for member_id, data in AUTO_ROLE_CONFIG['dm_schedule'].items():
                    role_expired = datetime.fromisoformat(data['role_expired'])
                    await conn.execute(
                        '''
                        INSERT INTO dm_schedule (member_id, role_expired, guild_id, dm_3_sent, dm_7_sent, dm_14_sent)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (member_id) DO UPDATE SET
                        role_expired = $2, guild_id = $3, dm_3_sent = $4, dm_7_sent = $5, dm_14_sent = $6
                    ''', int(member_id), role_expired, VIP_GROUP_ID,
                        data.get('dm_3_sent', False),
                        data.get('dm_7_sent', False),
                        data.get('dm_14_sent', False))
        except Exception as e:
            logger.error(f"Error saving auto role config: {e}")

    async def load_active_trades_from_db(self):
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM active_trades WHERE status = 'active'")

                for row in rows:
                    trade_key = row['message_id']
                    chat_id = row['channel_id']

                    if '_' in trade_key:
                        original_message_id = trade_key.split('_', 1)[1]
                    else:
                        original_message_id = trade_key

                    tp_hits = row['tp_hits'].split(
                        ',') if row['tp_hits'] else []
                    tp_hits = [t for t in tp_hits if t]

                    channel_message_map = {}
                    try:
                        raw_map = row.get('channel_message_map', '')
                        if raw_map and raw_map.strip():
                            channel_message_map = json.loads(raw_map)
                    except (json.JSONDecodeError, TypeError, KeyError):
                        pass

                    all_channel_ids = []
                    try:
                        raw_ids = row.get('all_channel_ids', '')
                        if raw_ids and raw_ids.strip():
                            all_channel_ids = [
                                int(cid) for cid in raw_ids.split(',')
                                if cid.strip().isdigit()
                            ]
                    except (ValueError, TypeError, KeyError):
                        pass

                    if not channel_message_map and chat_id:
                        channel_message_map = {
                            str(chat_id):
                            int(original_message_id)
                            if original_message_id.isdigit() else 0
                        }
                    if not all_channel_ids and chat_id:
                        all_channel_ids = [chat_id]

                    manual_overrides = []
                    try:
                        raw_overrides = row.get('manual_overrides', '')
                        if raw_overrides and raw_overrides.strip():
                            manual_overrides = [
                                o for o in raw_overrides.split(',') if o
                            ]
                    except (ValueError, TypeError, KeyError):
                        pass

                    live_entry_val = float(
                        row['live_entry']) if row.get('live_entry') else float(
                            row['entry_price'])

                    PRICE_TRACKING_CONFIG['active_trades'][trade_key] = {
                        'message_id':
                        original_message_id,
                        'trade_key':
                        trade_key,
                        'chat_id':
                        chat_id,
                        'pair':
                        row['pair'],
                        'action':
                        row['action'],
                        'entry_type':
                        row['entry_type'],
                        'entry_price':
                        float(row['entry_price']),
                        'tp1_price':
                        float(row['tp1_price']),
                        'tp2_price':
                        float(row['tp2_price']),
                        'tp3_price':
                        float(row['tp3_price']),
                        'sl_price':
                        float(row['sl_price']),
                        'entry':
                        live_entry_val,
                        'tp1':
                        float(row['tp1_price']),
                        'tp2':
                        float(row['tp2_price']),
                        'tp3':
                        float(row['tp3_price']),
                        'sl':
                        float(row['sl_price']),
                        'telegram_entry':
                        float(row['telegram_entry'])
                        if row.get('telegram_entry') else None,
                        'telegram_tp1':
                        float(row['telegram_tp1'])
                        if row.get('telegram_tp1') else None,
                        'telegram_tp2':
                        float(row['telegram_tp2'])
                        if row.get('telegram_tp2') else None,
                        'telegram_tp3':
                        float(row['telegram_tp3'])
                        if row.get('telegram_tp3') else None,
                        'telegram_sl':
                        float(row['telegram_sl'])
                        if row.get('telegram_sl') else None,
                        'live_entry':
                        float(row['live_entry'])
                        if row.get('live_entry') else None,
                        'assigned_api':
                        row.get('assigned_api', 'currencybeacon'),
                        'status':
                        row['status'],
                        'tp_hits':
                        tp_hits,
                        'breakeven_active':
                        row['breakeven_active'],
                        'manual_overrides':
                        manual_overrides,
                        'created_at':
                        row['created_at'].isoformat()
                        if row['created_at'] else None,
                        'channel_message_map':
                        channel_message_map,
                        'all_channel_ids':
                        all_channel_ids,
                        'group_name':
                        row.get('group_name', '')
                    }

                logger.info(
                    f"Loaded {len(PRICE_TRACKING_CONFIG['active_trades'])} active trades from database"
                )
        except Exception as e:
            logger.error(f"Error loading active trades: {e}")

    async def save_trade_to_db(self, message_id: str, trade_data: dict):
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                tp_hits_str = ','.join(trade_data.get('tp_hits', []))
                manual_overrides_str = ','.join(
                    trade_data.get('manual_overrides', []))
                channel_message_map_str = json.dumps(
                    trade_data.get('channel_message_map', {}))
                all_channel_ids = trade_data.get('all_channel_ids', [])
                all_channel_ids_str = ','.join(
                    str(cid) for cid in all_channel_ids)
                group_name = trade_data.get('group_name', '')

                await conn.execute(
                    '''
                    INSERT INTO active_trades 
                    (message_id, channel_id, guild_id, pair, action, entry_price, tp1_price, tp2_price, tp3_price, sl_price,
                     telegram_entry, telegram_tp1, telegram_tp2, telegram_tp3, telegram_sl, live_entry, assigned_api,
                     status, tp_hits, breakeven_active, entry_type, manual_overrides, channel_message_map, all_channel_ids, group_name)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
                    ON CONFLICT (message_id) DO UPDATE SET
                    status = $18, tp_hits = $19, breakeven_active = $20, manual_overrides = $22, 
                    channel_message_map = $23, all_channel_ids = $24, group_name = $25, last_updated = NOW()
                ''', message_id, trade_data.get('chat_id', 0),
                    trade_data.get('chat_id', 0), trade_data.get('pair'),
                    trade_data.get('action'), trade_data.get('entry_price'),
                    trade_data.get('tp1_price'), trade_data.get('tp2_price'),
                    trade_data.get('tp3_price'), trade_data.get('sl_price'),
                    trade_data.get('telegram_entry'),
                    trade_data.get('telegram_tp1'),
                    trade_data.get('telegram_tp2'),
                    trade_data.get('telegram_tp3'),
                    trade_data.get('telegram_sl'),
                    trade_data.get('live_entry'),
                    trade_data.get('assigned_api', 'currencybeacon'),
                    trade_data.get('status', 'active'), tp_hits_str,
                    trade_data.get('breakeven_active', False),
                    trade_data.get('entry_type',
                                   'execution'), manual_overrides_str,
                    channel_message_map_str, all_channel_ids_str, group_name)

                await self.log_to_debug(
                    f"Database INSERT successful for message_id: {message_id}")
        except Exception as e:
            logger.error(f"Error saving trade to database: {e}")
            await self.log_to_debug(
                f"Database INSERT failed for message_id {message_id}: {str(e)}"
            )

    async def update_trade_in_db(self, message_id: str, trade_data: dict):
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                tp_hits_str = ','.join(trade_data.get('tp_hits', []))
                manual_overrides_str = ','.join(
                    trade_data.get('manual_overrides', []))

                await conn.execute(
                    '''
                    UPDATE active_trades 
                    SET status = $2, tp_hits = $3, breakeven_active = $4, 
                        manual_overrides = $5, live_entry = $6, last_updated = NOW()
                    WHERE message_id = $1
                ''', message_id, trade_data.get('status',
                                                'active'), tp_hits_str,
                    trade_data.get('breakeven_active', False),
                    manual_overrides_str, trade_data.get('live_entry'))
        except Exception as e:
            logger.error(f"Error updating trade in database: {e}")

    async def archive_trade_to_completed(self, message_id: str,
                                         trade_data: dict,
                                         completion_reason: str):
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                tp_hits_str = ','.join(trade_data.get('tp_hits', []))
                manual_overrides_str = ','.join(
                    trade_data.get('manual_overrides', []))

                created_at = trade_data.get('created_at')
                if isinstance(created_at, str):
                    created_at = datetime.fromisoformat(
                        created_at.replace('Z', '+00:00'))
                if created_at is None:
                    created_at = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)

                await conn.execute(
                    '''
                    INSERT INTO completed_trades 
                    (message_id, channel_id, guild_id, pair, action, entry_price, tp1_price, tp2_price, tp3_price, sl_price,
                     telegram_entry, telegram_tp1, telegram_tp2, telegram_tp3, telegram_sl, live_entry, assigned_api,
                     final_status, tp_hits, breakeven_active, entry_type, manual_overrides, created_at, completion_reason)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
                    ON CONFLICT (message_id) DO NOTHING
                ''', message_id, trade_data.get('chat_id', 0),
                    trade_data.get('chat_id', 0), trade_data.get('pair'),
                    trade_data.get('action'),
                    trade_data.get('entry_price') or trade_data.get('entry'),
                    trade_data.get('tp1_price') or trade_data.get('tp1'),
                    trade_data.get('tp2_price') or trade_data.get('tp2'),
                    trade_data.get('tp3_price') or trade_data.get('tp3'),
                    trade_data.get('sl_price') or trade_data.get('sl'),
                    trade_data.get('telegram_entry'),
                    trade_data.get('telegram_tp1'),
                    trade_data.get('telegram_tp2'),
                    trade_data.get('telegram_tp3'),
                    trade_data.get('telegram_sl'),
                    trade_data.get('live_entry'),
                    trade_data.get('assigned_api', 'currencybeacon'),
                    trade_data.get('status', 'completed'), tp_hits_str,
                    trade_data.get('breakeven_active', False),
                    trade_data.get('entry_type', 'execution'),
                    manual_overrides_str, created_at, completion_reason)

                await self.log_to_debug(
                    f"Trade {message_id} archived to completed_trades: {completion_reason}"
                )
        except Exception as e:
            logger.error(
                f"CRITICAL: Error archiving trade to completed_trades: {e}")
            await self.log_to_debug(
                f"CRITICAL: Archive FAILED for {message_id} (reason: {completion_reason}): {e}"
            )
            raise

    async def remove_trade_from_db(self, message_id: str, reason: str):
        if not self.db_pool:
            return

        try:
            trade_data = PRICE_TRACKING_CONFIG['active_trades'].get(
                message_id, {})
            if trade_data:
                try:
                    await self.archive_trade_to_completed(
                        message_id, trade_data, reason)
                except Exception as archive_err:
                    logger.error(
                        f"CRITICAL: Failed to archive trade {message_id} before deletion: {archive_err}"
                    )
                    await self.log_to_debug(
                        f"CRITICAL: Archive failed for {message_id}: {archive_err}"
                    )

            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    'DELETE FROM active_trades WHERE message_id = $1',
                    message_id)

            if message_id in PRICE_TRACKING_CONFIG['active_trades']:
                del PRICE_TRACKING_CONFIG['active_trades'][message_id]

            logger.info(
                f"Trade {message_id} removed from database (reason: {reason})")
        except Exception as e:
            logger.error(f"Error removing trade from database: {e}")
            await self.log_to_debug(f"Error removing trade {message_id}: {e}")

    def is_weekend_market_closed(self) -> bool:
        amsterdam_now = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
        weekday = amsterdam_now.weekday()
        hour = amsterdam_now.hour
        minute = amsterdam_now.minute

        if (weekday == 4 and hour >= 23) or weekday == 5 or (
                weekday == 6 and hour < 23) or (weekday == 6 and hour == 23
                                                and minute < 55):
            return True
        return False

    async def check_offline_tp_sl_hits(self):
        await self.load_active_trades_from_db()

        if not PRICE_TRACKING_CONFIG['active_trades']:
            await self.log_to_debug(
                "No active trades to check for offline TP/SL hits")
            return

        await self.log_to_debug(
            f"Checking {len(PRICE_TRACKING_CONFIG['active_trades'])} active trades for TP/SL hits that occurred while offline..."
        )

        offline_hits_found = 0

        for message_id, trade_data in list(
                PRICE_TRACKING_CONFIG['active_trades'].items()):
            try:
                current_price = await self.get_live_price(trade_data['pair'])
                if current_price is None:
                    continue

                action = trade_data['action']
                live_entry = trade_data.get('live_entry') or trade_data.get(
                    'entry', 0)
                tp_hits = trade_data.get('tp_hits', [])
                breakeven_active = trade_data.get('breakeven_active', False)

                if action == "BUY":
                    if breakeven_active and live_entry:
                        if current_price <= live_entry:
                            await self.handle_breakeven_hit(
                                message_id, trade_data)
                            offline_hits_found += 1
                            continue
                    elif current_price <= trade_data['sl_price']:
                        await self.handle_sl_hit(message_id, trade_data,
                                                 current_price)
                        offline_hits_found += 1
                        continue
                elif action == "SELL":
                    if breakeven_active and live_entry:
                        if current_price >= live_entry:
                            await self.handle_breakeven_hit(
                                message_id, trade_data)
                            offline_hits_found += 1
                            continue
                    elif current_price >= trade_data['sl_price']:
                        await self.handle_sl_hit(message_id, trade_data,
                                                 current_price)
                        offline_hits_found += 1
                        continue

                tp_levels_hit = []

                if action == "BUY":
                    if "tp1" not in tp_hits and current_price >= trade_data[
                            'tp1_price']:
                        tp_levels_hit.append("tp1")
                    if "tp2" not in tp_hits and current_price >= trade_data[
                            'tp2_price']:
                        tp_levels_hit.append("tp2")
                    if "tp3" not in tp_hits and current_price >= trade_data[
                            'tp3_price']:
                        tp_levels_hit.append("tp3")
                elif action == "SELL":
                    if "tp1" not in tp_hits and current_price <= trade_data[
                            'tp1_price']:
                        tp_levels_hit.append("tp1")
                    if "tp2" not in tp_hits and current_price <= trade_data[
                            'tp2_price']:
                        tp_levels_hit.append("tp2")
                    if "tp3" not in tp_hits and current_price <= trade_data[
                            'tp3_price']:
                        tp_levels_hit.append("tp3")

                if tp_levels_hit:
                    for tp_level in ["tp1", "tp2", "tp3"]:
                        if tp_level in tp_levels_hit:
                            await self.handle_tp_hit(message_id, trade_data,
                                                     tp_level, current_price)
                            offline_hits_found += 1
                    continue

            except Exception as e:
                logger.error(
                    f"Error checking offline TP/SL for {message_id}: {e}")
                continue

        if offline_hits_found > 0:
            await self.log_to_debug(
                f"Found and processed {offline_hits_found} TP/SL hits that occurred while offline"
            )
        else:
            await self.log_to_debug("No offline TP/SL hits detected")

    async def price_tracking_loop(self):
        await asyncio.sleep(10)

        while self.running:
            try:
                if self.is_weekend_market_closed():
                    await asyncio.sleep(PRICE_TRACKING_CONFIG['check_interval']
                                        )
                    continue

                trades = dict(PRICE_TRACKING_CONFIG['active_trades'])

                for message_id, trade_data in trades.items():
                    if message_id not in PRICE_TRACKING_CONFIG[
                            'active_trades']:
                        continue

                    await self.check_price_levels(message_id, trade_data)
                    await asyncio.sleep(2)

            except Exception as e:
                logger.error(f"Error in price tracking loop: {e}")

            await asyncio.sleep(PRICE_TRACKING_CONFIG['check_interval'])

    async def trial_expiry_loop(self):
        await asyncio.sleep(60)

        while self.running:
            try:
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                expired_members = []

                for member_id, data in list(
                        AUTO_ROLE_CONFIG['active_members'].items()):
                    expiry_time = datetime.fromisoformat(data['expiry_time'])
                    if expiry_time.tzinfo is None:
                        expiry_time = AMSTERDAM_TZ.localize(expiry_time)

                    if current_time >= expiry_time:
                        expired_members.append(member_id)

                for member_id in expired_members:
                    await self.expire_trial(member_id)

            except Exception as e:
                logger.error(f"Error in trial expiry loop: {e}")

            await asyncio.sleep(60)

    async def expire_trial(self, member_id: str):
        try:
            data = AUTO_ROLE_CONFIG['active_members'].get(member_id)
            if not data:
                return

            try:
                await self.app.ban_chat_member(VIP_GROUP_ID, int(member_id))
                await asyncio.sleep(1)
                await self.app.unban_chat_member(VIP_GROUP_ID, int(member_id))
            except Exception as e:
                logger.error(f"Error kicking member {member_id}: {e}")

            current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)

            if member_id in AUTO_ROLE_CONFIG['role_history']:
                AUTO_ROLE_CONFIG['role_history'][member_id][
                    'last_expired'] = current_time.isoformat()

            AUTO_ROLE_CONFIG['dm_schedule'][member_id] = {
                'role_expired': current_time.isoformat(),
                'dm_3_sent': False,
                'dm_7_sent': False,
                'dm_14_sent': False
            }

            del AUTO_ROLE_CONFIG['active_members'][member_id]

            try:
                expiry_msg = (
                    f"Hey! Your **3-day free access** to the VIP Group has unfortunately **ran out**. "
                    f"We truly hope you were able to benefit with us & we hope to see you back soon! "
                    f"For now, feel free to continue following our trade signals in the Free Group: https://t.me/fxpippioneers\n\n"
                    f"**Want to rejoin the VIP Group? You can regain access through this link:** https://whop.com/gold-pioneer/gold-pioneer/"
                )
                await self.app.send_message(int(member_id), expiry_msg)
            except Exception as e:
                logger.error(f"Could not send expiry DM to {member_id}: {e}")

            await self.save_auto_role_config()
            await self.log_to_debug(f"Trial expired for user {member_id}")

        except Exception as e:
            logger.error(f"Error expiring trial for {member_id}: {e}")

    async def followup_dm_loop(self):
        await asyncio.sleep(300)

        while self.running:
            try:
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)

                for member_id, data in list(
                        AUTO_ROLE_CONFIG['dm_schedule'].items()):
                    role_expired = datetime.fromisoformat(data['role_expired'])
                    if role_expired.tzinfo is None:
                        role_expired = AMSTERDAM_TZ.localize(role_expired)

                    try:
                        is_vip = await self.check_vip_membership(int(member_id)
                                                                 )
                        if is_vip:
                            continue
                    except Exception:
                        pass

                    dm_3_time = role_expired + timedelta(days=3)
                    dm_7_time = role_expired + timedelta(days=7)
                    dm_14_time = role_expired + timedelta(days=14)

                    if not data['dm_3_sent'] and current_time >= dm_3_time:
                        await self.send_followup_dm(member_id, 3)
                        AUTO_ROLE_CONFIG['dm_schedule'][member_id][
                            'dm_3_sent'] = True

                    if not data['dm_7_sent'] and current_time >= dm_7_time:
                        await self.send_followup_dm(member_id, 7)
                        AUTO_ROLE_CONFIG['dm_schedule'][member_id][
                            'dm_7_sent'] = True

                    if not data['dm_14_sent'] and current_time >= dm_14_time:
                        await self.send_followup_dm(member_id, 14)
                        AUTO_ROLE_CONFIG['dm_schedule'][member_id][
                            'dm_14_sent'] = True

            except Exception as e:
                logger.error(f"Error in followup DM loop: {e}")

            await asyncio.sleep(3600)

    async def check_vip_membership(self, user_id: int) -> bool:
        try:
            member = await self.app.get_chat_member(VIP_GROUP_ID, user_id)
            return member.status in [
                ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.OWNER
            ]
        except UserNotParticipant:
            return False
        except Exception:
            return False

    async def send_followup_dm(self, member_id: str, days: int):
        try:
            if days == 3:
                message = (
                    f"Hey! It's been 3 days since your **3-day free access to the VIP Group** ended. We truly hope that you were able to catch good trades with us during that time.\n\n"
                    f"As you've probably seen, our free signals channel gets **1 free signal per day**, while our **VIP members** in the VIP Group receive **6+ high-quality signals per day**. That means that our VIP Group offers way more chances to profit and grow consistently.\n\n"
                    f"We'd love to **invite you back to the VIP Group,** so you don't miss out on more solid opportunities.\n\n"
                    f"**Feel free to join us again through this link:** https://whop.com/gold-pioneer/gold-pioneer/"
                )
            elif days == 7:
                message = (
                    f"It's been a week since your VIP Group trial ended. Since then, our **VIP members have been catching trade setups daily in the VIP Group**.\n\n"
                    f"If you found value in just 3 days, imagine what results you could've been seeing by now with full access. It's all about **consistency and staying connected to the right information**.\n\n"
                    f"We'd like to **personally invite you to rejoin the VIP Group** and get back into the rhythm.\n\n"
                    f"**Feel free to join us again through this link:** https://whop.com/gold-pioneer/gold-pioneer/"
                )
            elif days == 14:
                message = (
                    f"Hey! It's been two weeks since your free access to the VIP Group ended. We hope you've stayed active since then.\n\n"
                    f"If you've been trading solo or passively following the free channel, you might be feeling the difference. In the VIP Group, it's not just about more signals. It's about the **structure, support, and smarter decision-making**. That edge can make all the difference over time.\n\n"
                    f"We'd love to **invite you back into the VIP Group** and help you start compounding results again.\n\n"
                    f"**Feel free to join us again through this link:** https://whop.com/gold-pioneer/gold-pioneer/"
                )
            else:
                return

            await self.app.send_message(int(member_id), message)
            await self.log_to_debug(
                f"Sent {days}-day follow-up DM to user {member_id}")
        except Exception as e:
            logger.error(
                f"Could not send {days}-day follow-up DM to {member_id}: {e}")

    async def monday_activation_loop(self):
        await asyncio.sleep(60)

        while self.running:
            try:
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                weekday = current_time.weekday()
                hour = current_time.hour

                if weekday == 0 and hour <= 1:
                    for member_id, data in list(
                            AUTO_ROLE_CONFIG['active_members'].items()):
                        try:
                            if data.get('weekend_delayed',
                                        False) and not data.get(
                                            'monday_notification_sent', False):
                                try:
                                    activation_message = (
                                        "Hey! The weekend is over, so the trading markets have been opened again. "
                                        "That means your 3-day welcome gift has officially started. "
                                        "You now have full access to the VIP Group. "
                                        "Let's make the most of it by securing some wins together!"
                                    )
                                    await self.app.send_message(
                                        int(member_id), activation_message)
                                    logger.info(
                                        f"Sent Monday activation DM to {member_id}"
                                    )

                                    AUTO_ROLE_CONFIG['active_members'][
                                        member_id][
                                            'monday_notification_sent'] = True
                                    await self.save_auto_role_config()
                                except Exception as e:
                                    logger.error(
                                        f"Could not send Monday activation DM to {member_id}: {e}"
                                    )
                        except Exception as e:
                            logger.error(
                                f"Error processing Monday activation for {member_id}: {e}"
                            )
            except Exception as e:
                logger.error(f"Error in monday activation loop: {e}")

            await asyncio.sleep(3600)

    async def register_bot_commands(self):
        try:
            public_commands = [
                BotCommand("activetrades", "View active trading signals"),
                BotCommand("pricetest", "Test live price for a pair"),
            ]
            await self.app.set_bot_commands(public_commands)
            logger.info("Public bot commands registered")

            if BOT_OWNER_USER_ID:
                owner_commands = [
                    BotCommand("activetrades", "View active trading signals"),
                    BotCommand("pricetest", "Test live price for a pair"),
                    BotCommand("entry", "Create trading signal (menu)"),
                    BotCommand("tradeoverride",
                               "Override trade status (menu)"),
                    BotCommand("freetrialusers", "Manage trial system (menu)"),
                    BotCommand("dbstatus", "Database health check"),
                    BotCommand("dmstatus", "DM delivery statistics"),
                    BotCommand("retracttrial",
                               "Retract trial minutes from user"),
                    BotCommand("clearmember", "Remove user from trial system"),
                ]
                await self.app.set_bot_commands(
                    owner_commands,
                    scope=BotCommandScopeChat(chat_id=BOT_OWNER_USER_ID))
                logger.info(
                    f"Owner commands registered for user {BOT_OWNER_USER_ID}")
        except Exception as e:
            logger.error(f"Error registering bot commands: {e}")

    async def heartbeat_loop(self):
        while self.running:
            try:
                if self.db_pool:
                    async with self.db_pool.acquire() as conn:
                        current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                        await conn.execute(
                            '''
                            INSERT INTO bot_status (id, last_online, heartbeat_time)
                            VALUES (1, $1, $1)
                            ON CONFLICT (id) DO UPDATE SET
                            last_online = $1, heartbeat_time = $1
                        ''', current_time)
            except Exception as e:
                logger.error(f"Error in heartbeat: {e}")

            await asyncio.sleep(60)

    async def run(self):
        await self.init_database()

        await self.app.start()
        logger.info("Telegram bot started!")

        await self.register_bot_commands()

        if DEBUG_GROUP_ID:
            try:
                await self.app.get_chat(DEBUG_GROUP_ID)
                await self.app.send_message(
                    DEBUG_GROUP_ID,
                    "**Bot Started!** Trading bot is now online and ready.")
            except Exception as e:
                logger.error(f"Could not send startup message: {e}")

        await self.check_offline_tp_sl_hits()

        asyncio.create_task(self.price_tracking_loop())
        asyncio.create_task(self.trial_expiry_loop())
        asyncio.create_task(self.followup_dm_loop())
        asyncio.create_task(self.heartbeat_loop())
        asyncio.create_task(self.monday_activation_loop())

        try:
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            if self.db_pool:
                await self.db_pool.close()
            await self.app.stop()


async def run_web_server():

    async def health_check(request):
        return web.Response(text="OK", status=200)

    app = web.Application()
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    logger.info("Health check server running on port 8080")


async def main():
    await run_web_server()

    bot = TelegramTradingBot()
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
