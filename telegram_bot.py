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
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Union
import asyncpg
import aiohttp
from aiohttp import web, ClientTimeout
import requests
import re
from urllib.parse import quote

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
        if not value:
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default


TELEGRAM_API_ID = safe_int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")

BOT_OWNER_USER_ID = safe_int(os.getenv("BOT_OWNER_USER_ID", "6664440870"))
FREE_GROUP_ID = safe_int(os.getenv("FREE_GROUP_ID", "0"))
VIP_GROUP_ID = safe_int(os.getenv("VIP_GROUP_ID", "0"))
DEBUG_GROUP_ID = safe_int(os.getenv("DEBUG_GROUP_ID", "0"))
FREE_GROUP_LINK = os.getenv("FREE_GROUP_LINK", "")
VIP_GROUP_LINK = os.getenv("VIP_GROUP_LINK", "")
VIP_TRIAL_INVITE_LINK = "https://t.me/+5X18tTjgM042ODU0"
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

print(f"🔗 VIP Trial invite link configured: https://t.me/+5X18tTjgM042ODU0")
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
    "last_price_check_time": None,
}

PENDING_ENTRIES = {}

MESSAGE_TEMPLATES = {
    "Trial Status & Expiry": {
        "Trial Expired": {
            "id": "trial_expired",
            "message": "Hey! Your **3-day free access** to the VIP Group has unfortunately **ran out**. We truly hope you were able to benefit with us & we hope to see you back soon! For now, feel free to continue following our trade signals in our Free Group: https://t.me/fxpippioneers\n\n**Want to rejoin the VIP Group? You can regain access through this link:** https://whop.com/gold-pioneer/gold-pioneer/"
        },
        "Trial Rejected (Used Before)": {
            "id": "trial_rejected",
            "message": "Hey! Unfortunately, our free trial can only be used once per person. Your trial has already ran out, so we can't give you another.\n\nWe truly hope that you were able to profit with us during your free trial. If you were happy with the results you got, then feel free to rejoin our VIP group through this link: https://whop.com/gold-pioneer/gold-pioneer/"
        }
    },
    "Free Trial Heads Up": {
        "24-Hour Warning": {
            "id": "ft_24hr",
            "message": "⏰ **REMINDER! Your 3-day free trial for our VIP Group will expire in 24 hours**.\n\nAfter that, you'll unfortunately lose access to the VIP Group. You've had great opportunities during these past 2 days. Don't let this last day slip away!"
        },
        "3-Hour Warning": {
            "id": "ft_3hr",
            "message": "⏰ **FINAL REMINDER! Your 3-day free trial for our VIP Group will expire in just 3 hours**.\n\nYou're about to lose access to our VIP Group and the 6+ daily trade signals and opportunities it comes with. However, you can also keep your access! Upgrade from FREE to VIP through our website and get permanent access to our VIP Group.\n\n**Upgrade to VIP to keep your access:** https://whop.com/gold-pioneer/gold-pioneer/"
        }
    },
    "3/7/14 Day Follow-ups": {
        "3 Days After Trial Ends": {
            "id": "fu_3day",
            "message": "Hey! It's been 3 days since your **3-day free access to the VIP Group** ended. We truly hope you got value from the **20+ trading signals** you received during that time.\n\nAs you've probably seen, our free signals channel gets **1 free signal per day**, while our **VIP members** in the VIP Group receive **6+ high-quality signals per day**. That means that our VIP Group offers way more chances to profit and grow consistently.\n\nWe'd love to **invite you back to the VIP Group,** so you don't miss out on more solid opportunities.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer/gold-pioneer/"
        },
        "7 Days After Trial Ends": {
            "id": "fu_7day",
            "message": "It's been a week since your VIP Group trial ended. Since then, our **VIP members have been catching trade setups daily in the VIP Group**.\n\nIf you found value in just 3 days, imagine what results you could've been seeing by now with full access. It's all about **consistency and staying connected to the right information**.\n\nWe'd like to **personally invite you to rejoin the VIP Group** and get back into the rhythm.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer/gold-pioneer/"
        },
        "14 Days After Trial Ends": {
            "id": "fu_14day",
            "message": "Hey! It's been two weeks since your free access to the VIP Group ended. We hope you've stayed active since then.\n\nIf you've been trading solo or passively following the free channel, you might be feeling the difference. In the VIP Group, it's not just about more signals. It's about the **structure, support, and smarter decision-making**. That edge can make all the difference over time.\n\nWe'd love to **invite you back into the VIP Group** and help you start compounding results again.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer/gold-pioneer/"
        }
    },
    "Welcome & Onboarding": {
        "Welcome DM (New Free Group Member)": {
            "id": "welcome_free",
            "message": "**Hey, Welcome to FX Pip Pioneers!**\n\n**Want to try our VIP Group for FREE?**\nWe're offering a **3-day free trial** of our VIP Group where you'll receive **6+ high-quality trade signals per day**.\n\n**Your free trial will automatically be activated once you join our VIP group through this link:** https://t.me/+5X18tTjgM042ODU0\n\nGood luck trading!"
        },
        "Monday Activation (Weekend Delay)": {
            "id": "monday_activation",
            "message": "Hey! The weekend is over, so the trading markets have been opened again. That means your 3-day welcome gift has officially started. You now have full access to the VIP Group. Let's make the most of it by securing some wins together!"
        }
    },
    "Engagement & Offers": {
        "Engagement Discount (50% Off)": {
            "id": "engagement_discount",
            "message": "Hey! 👋 We noticed that you've been engaging with our signals in the Free Group. We want to say that we truly appreciate it!\n\nAs a thank you for your loyalty and engagement, we want to give you something special: **exclusive 50% discount for access to our VIP Group.**\n\nHere's what you'll unlock:\n• **36+ expert signals per week** (vs 6 per week in the free group)\n• **6+ trade signals PER DAY** from our professional trading team\n• Real-time price tracking and risk management\n\n**Your exclusive discount code is:** `Thank_You!50!`\n\n**You can upgrade to VIP and apply your discount code here:** https://whop.com/gold-pioneer/gold-pioneer/"
        },
        "Daily VIP Trial Offer": {
            "id": "daily_trial_offer",
            "message": "Want to try our VIP Group for FREE?\n\nWe're offering a 3-day free trial of our VIP Group where you'll receive 6+ high-quality trade signals per day.\n\nYour free trial will automatically be activated once you join our VIP group through this link: https://t.me/+5X18tTjgM042ODU0"
        }
    }
}


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
                          bot_token=TELEGRAM_BOT_TOKEN,
                          workdir=".") # Fix 2: Session storage enabled by providing workdir

        # Database connection pool
        self.db_pool = None
        db_url = os.getenv("DATABASE_URL_OVERRIDE") or os.getenv("DATABASE_URL")
        if db_url:
            try:
                # Basic initialization for Render's external Postgres
                import asyncpg
                import ssl
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                
                async def _init_pool():
                    for attempt in range(5):
                        try:
                            self.db_pool = await asyncpg.create_pool(
                                db_url, 
                                ssl=ctx
                            )
                            print("Database pool successfully initialized")
                            return
                        except Exception as e:
                            wait_time = (attempt + 1) * 5
                            print(f"Async database initialization attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                            if attempt < 4:
                                await asyncio.sleep(wait_time)
                            else:
                                print("All database initialization attempts failed.")

                self.db_pool_future = asyncio.create_task(_init_pool())
                print("Started database pool initialization task")
            except Exception as e:
                print(f"Failed to start DB pool: {e}")
        else:
            print("No DATABASE_URL found for main bot")
        
        self.client_session = None
        self.last_online_time = None
        self.running = True
        self.startup_complete = False
        self.awaiting_price_input = {}
        self.awaiting_custom_pair = {}
        self.override_trade_mappings = {}  # menu_id -> {idx: message_id}
        self.trial_pending_approvals = set(
        )  # Track user IDs approved for trial
        self.last_warning_send_time = {}  # Track last warning send time per user_id
        self.peer_id_check_state = {}  # Track peer ID checks: user_id -> {joined_at, delay_level, interval, established}
        self.userbot_login_state = {}  # user_id -> {client, phone, phone_code_hash}

        # Handle BOT_OWNER_USER_ID from environment if 0
        global BOT_OWNER_USER_ID
        if BOT_OWNER_USER_ID == 0:
            env_owner = os.getenv("BOT_OWNER_USER_ID_OVERRIDE") or os.getenv("BOT_OWNER_USER_ID")
            if env_owner:
                BOT_OWNER_USER_ID = safe_int(str(env_owner))
                print(f"Updated BOT_OWNER_USER_ID to {BOT_OWNER_USER_ID} from environment")

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

        @self.app.on_message(filters.command("newmemberslist"))
        async def newmemberslist_command(client, message: Message):
            await self.handle_newmemberslist(client, message)

        @self.app.on_message(filters.command("dmmessages"))
        async def dmmessages_command(client, message: Message):
            await self.handle_dmmessages(client, message)

        @self.app.on_message(filters.command("peeridstatus"))
        async def peeridstatus_command(client, message: Message):
            await self.handle_peer_id_status(client, message)

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

        @self.app.on_callback_query(filters.regex("^clrtrl_"))
        async def cleartrial_callback(client, callback_query: CallbackQuery):
            await self.handle_cleartrial_callback(client, callback_query)

        @self.app.on_callback_query(filters.regex("^swdm_"))
        async def sendwelcomedm_callback(client, callback_query: CallbackQuery):
            await self.handle_sendwelcomedm_callback(client, callback_query)

        @self.app.on_callback_query(filters.regex("^nml_"))
        async def newmemberslist_callback(client, callback_query: CallbackQuery):
            await self._handle_newmemberslist_callback(client, callback_query)

        @self.app.on_callback_query(filters.regex("^dmm_"))
        async def dmmessages_callback(client, callback_query: CallbackQuery):
            await self._handle_dmmessages_callback(client, callback_query)

        @self.app.on_callback_query(filters.regex("^pid_"))
        async def peerid_callback(client, callback_query: CallbackQuery):
            await self.handle_peerid_callback(client, callback_query)

        @self.app.on_callback_query(filters.regex("^at_"))
        async def active_trades_callback(client, callback_query: CallbackQuery):
            await self.handle_active_trades_callback(client, callback_query)

        @self.app.on_message(filters.command("login"))
        async def login_command(client, message: Message):
            if not await self.is_owner(message.from_user.id):
                return
            
            # Use text-based logic for /login status
            parts = message.text.split()
            if len(parts) > 1:
                subcommand = parts[1].lower()
                if subcommand == "status":
                    await self.handle_login_status(client, message)
                    return
                elif subcommand == "setup":
                    await self.handle_login_setup(client, message)
                    return

            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📊 Check Status", callback_data="login_status"),
                    InlineKeyboardButton("🔑 Start Setup", callback_data="login_setup")
                ]
            ])
            await message.reply(
                "**Userbot Login Management System**\n\n"
                "Use the buttons below to manage your Userbot session.",
                reply_markup=keyboard
            )

        @self.app.on_callback_query(filters.regex("^login_"))
        async def login_callback(client, callback_query: CallbackQuery):
            if not await self.is_owner(callback_query.from_user.id):
                await callback_query.answer("Unauthorized.", show_alert=True)
                return

            action = callback_query.data.split("_")[1]
            if action == "status":
                await callback_query.answer()
                await self.handle_login_status(client, callback_query.message)
            elif action == "setup":
                await callback_query.answer()
                await self.handle_login_setup(client, callback_query.message)

        @self.app.on_message(filters.text & filters.private)
        async def handle_private_message(client, message: Message):
            if not await self.is_owner(message.from_user.id):
                return
            
            # Check if we're waiting for a login code
            if await self.process_userbot_code(client, message):
                return

            # Handle price inputs for manual signals
            user_id = message.from_user.id
            if user_id in self.awaiting_price_input:
                # ... existing logic ...
                pass

    async def handle_login_status(self, client, message: Message):
        """Check the status of the Userbot service"""
        if not self.db_pool:
            await message.reply("❌ Database connection not available.")
            return

        try:
            async with self.db_pool.acquire() as conn:
                session = await conn.fetchval(
                    "SELECT setting_value FROM bot_settings WHERE setting_key = 'userbot_session_string'"
                )
            
            if not session:
                await message.reply("🔴 **Userbot Status**: Disconnected (No session found).")
            else:
                await message.reply("🟢 **Userbot Status**: Connected (Session exists in database).")
        except Exception as e:
            await message.reply(f"❌ Error checking status: {str(e)}")

    async def handle_login_setup(self, client, message: Message):
        """Initiate Userbot Setup (Login)"""
        if not self.db_pool:
            await message.reply("❌ Database connection not available.")
            return

        user_id = message.from_user.id
        phone = os.getenv("USERBOT_PHONE")
        api_id = safe_int(os.getenv("USERBOT_API_ID", "0"))
        api_hash = os.getenv("USERBOT_API_HASH", "")

        if not phone or not api_id or not api_hash:
            await message.reply("❌ Userbot credentials missing in environment variables.")
            return

        await message.reply(f"🔄 **Starting Userbot Login** for `{phone}`...\nRequesting code from Telegram...")

        try:
            # Create a temporary client for login
            temp_client = Client(
                name=f"temp_userbot_{user_id}",
                api_id=api_id,
                api_hash=api_hash,
                in_memory=True
            )
            await temp_client.connect()
            
            # Request phone code
            code_info = await temp_client.send_code(phone)
            
            self.userbot_login_state[user_id] = {
                "client": temp_client,
                "phone": phone,
                "phone_code_hash": code_info.phone_code_hash
            }

            await message.reply(
                "📩 **Code Sent!**\n\nPlease check your Telegram account for the 5-digit verification code and reply with it here.\n\n"
                "**Format:** Just send the 5 digits (e.g., `12345`)."
            )
        except Exception as e:
            await message.reply(f"❌ Failed to initiate login: {str(e)}")
            if user_id in self.userbot_login_state:
                del self.userbot_login_state[user_id]

    async def process_userbot_code(self, client, message: Message):
        """Process the 5-digit code sent by the owner"""
        user_id = message.from_user.id
        state = self.userbot_login_state.get(user_id)
        
        if not state:
            return False

        code = message.text.strip()
        if not code.isdigit() or len(code) != 5:
            await message.reply("❌ Invalid format. Please send exactly 5 digits.")
            return True

        await message.reply("⏳ **Verifying code and generating session...**")
        
        try:
            temp_client = state["client"]
            await temp_client.sign_in(
                phone_number=state["phone"],
                phone_code_hash=state["phone_code_hash"],
                phone_code=code
            )
            
            session_string = await temp_client.export_session_string()
            
            # Save session to database
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO bot_settings (setting_key, setting_value) "
                    "VALUES ('userbot_session_string', $1) "
                    "ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value",
                    session_string
                )
            
            await message.reply("✅ **Success!** Userbot session saved to database.\n\nThe userbot service will now pick up the new session.")
            await self.log_to_debug("✅ New Userbot session generated and stored.")
            
            await temp_client.disconnect()
            del self.userbot_login_state[user_id]
            return True
            
        except Exception as e:
            await message.reply(f"❌ Login failed: {str(e)}")
            await temp_client.disconnect()
            del self.userbot_login_state[user_id]
            return True

    async def handle_group_message(self, client: Client, message: Message):
        """Handle messages in groups"""
        # Auto-delete service messages
        if message.service:
            if message.chat.id in [VIP_GROUP_ID, FREE_GROUP_ID]:
                try:
                    await message.delete()
                except Exception:
                    pass
            return
            
        # Track engagement in free group
        if message.chat.id == FREE_GROUP_ID and message.reactions:
             asyncio.create_task(self._fetch_and_store_reactions(message))

        # detect manual signals from owner
        if not message.from_user or not message.text:
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

            # For excluded pairs, we track them manually but don't auto-check prices
            manual_tracking_only = pair in EXCLUDED_FROM_TRACKING
            if manual_tracking_only:
                await self.log_to_debug(
                    f"Adding {pair} to manual tracking only (no automatic price monitoring)"
                )
            
            assigned_api = await self.get_working_api_for_pair(pair) if not manual_tracking_only else 'manual'

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
                'manual_tracking_only':
                manual_tracking_only,
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

    async def log_to_debug(self, message: str, is_error: bool = False, user_id: Optional[int] = None, failed_message: Optional[str] = None):
        if DEBUG_GROUP_ID:
            try:
                # Standardize professional debug headers
                if is_error:
                    msg_text = f"🚨 **SYSTEM ERROR**\n\n**Issue:** {message}\n\n@fx_pippioneers"
                else:
                    msg_text = f"📊 **SYSTEM LOG**\n\n**Event:** {message}"
                
                # Add user ID button if user_id is provided
                keyboard = None
                if user_id:
                    msg_text += f"\n\n👤 **User ID:** `{user_id}`"
                    
                    # Build the button URL to open user's profile
                    button_url = f"tg://user?id={user_id}"
                    
                    keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton("👤 View User Profile", url=button_url)]
                    ])
                
                await self.app.send_message(DEBUG_GROUP_ID, msg_text, reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Failed to send debug log: {e}")
        log_level = logging.ERROR if is_error else logging.INFO
        logger.log(log_level, message)

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
        Trading days: Monday-Friday only (3 full trading days AFTER join date)
        
        Rules:
        - Saturday/Sunday joiners: Always expire Wednesday 22:59 (regardless of join time)
        - All other days: Expire exactly 3 trading days after join at the same time they joined
        
        Examples:
        - Saturday 13:37 join → expires Wednesday 22:59
        - Sunday 03:00 join → expires Wednesday 22:59
        - Monday 00:10 join → expires Thursday 00:10 (Tue, Wed, Thu = 3 trading days)
        - Friday 13:37 join → expires Wednesday 13:37 (Mon, Tue, Wed = 3 trading days)
        - Wednesday 14:00 join → expires Monday 14:00 (Thu, Fri, Mon = 3 trading days)
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

        # For other weekdays, count exactly 3 trading days AFTER the join date
        trading_days_counted = 0
        current_date = join_time.date() + timedelta(days=1)  # Start from the day after join

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
            action = data[len("entry_action_"):].upper()
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
            entry_type = data[len("entry_type_"):]
            entry_data['entry_type'] = entry_type

            self.awaiting_custom_pair[user_id] = callback_query.message.id
            await callback_query.message.edit_text(
                f"**Create Trading Signal**\n\n"
                f"Action: **{entry_data['action']}**\n"
                f"Type: **{entry_type.upper()}**\n\n"
                f"Step 3: Type the trading pair (e.g., EURUSD, GBPJPY, XAUUSD):"
            )

        elif data.startswith("entry_group_"):
            group_choice = data[len("entry_group_"):]

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
                    'message_id':
                    str(sent_msg.id),
                    'trade_key':
                    trade_key,
                    'chat_id':
                    sent_msg.chat.id,
                    'pair':
                    pair,
                    'action':
                    action,
                    'entry_type':
                    entry_type,
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
                    float(levels['tp1']),
                    'telegram_tp2':
                    float(levels['tp2']),
                    'telegram_tp3':
                    float(levels['tp3']),
                    'telegram_sl':
                    float(levels['sl']),
                    'live_entry':
                    float(live_price),
                    'assigned_api':
                    assigned_api,
                    'status':
                    'active',
                    'tp_hits': [],
                    'manual_overrides': [],
                    'breakeven_active':
                    False,
                    'created_at':
                    datetime.now(
                        pytz.UTC).astimezone(AMSTERDAM_TZ).isoformat(),
                    'group_name':
                    group_name,
                    'channel_id':
                    channel_id
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
        """Handle user input"""
        user_id = message.from_user.id

        # 1. Handle code input for Userbot login
        if hasattr(self, 'awaiting_login_code') and self.awaiting_login_code:
            code = message.text.strip()
            if len(code) == 5 and code.isdigit():
                try:
                    login_data = self.awaiting_login_code
                    temp_client = login_data["temp_client"]
                    
                    await temp_client.sign_in(
                        login_data["phone_number"],
                        login_data["phone_code_hash"],
                        code
                    )
                    
                    session_string = await temp_client.export_session_string()
                    
                    async with self.db_pool.acquire() as conn:
                        # Ensure bot_settings table exists
                        await conn.execute("CREATE TABLE IF NOT EXISTS bot_settings (setting_key TEXT PRIMARY KEY, setting_value TEXT)")
                        await conn.execute("""
                            INSERT INTO bot_settings (setting_key, setting_value)
                            VALUES ('userbot_session_string', $1)
                            ON CONFLICT (setting_key) DO UPDATE SET setting_value = $1
                        """, session_string)
                    
                    await message.reply("✅ **Userbot Authenticated Successfully!**\nThe session has been saved. The Userbot service will now use this session.")
                    self.awaiting_login_code = None
                    await temp_client.disconnect()
                except Exception as e:
                    await message.reply(f"❌ **Sign-in Failed**: {str(e)}")
                return

        if not await self.is_owner(user_id):
            # Send support message to non-owners
            await message.reply(
                "This is a private trading bot that can only be used by members of the FX Pip Pioneers team. \n\nIf you need support or have questions, please contact @fx_pippioneers."
            )
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
                    await message.reply(
                        "Please enter a valid time (e.g., '5h' or '30m' or '2h 15m')"
                    )
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

        # Handle sendwelcomedm user ID input
        if not hasattr(self, 'awaiting_sendwelcomedm_input'):
            self.awaiting_sendwelcomedm_input = {}
        
        if user_id in self.awaiting_sendwelcomedm_input:
            menu_id = self.awaiting_sendwelcomedm_input.pop(user_id)
            try:
                user_id_input = int(message.text.strip())
                
                if not hasattr(self, 'sendwelcomedm_context'):
                    self.sendwelcomedm_context = {}
                
                context = self.sendwelcomedm_context.get(menu_id, {})
                context['user_id'] = user_id_input
                context['msg_type'] = 'welcome'
                context['stage'] = 'confirmed'
                self.sendwelcomedm_context[menu_id] = context

                # Send welcome message directly
                await self.execute_send_welcome_dm(None, menu_id, context, user_message=message)
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
                await self._show_peer_id_status(message, peer_user_id)
            except ValueError:
                await message.reply("❌ Invalid user ID. Please provide a numeric ID only.")
            return

        # Handle cleartrial custom member ID input
        if not hasattr(self, 'awaiting_cleartrial_input'):
            self.awaiting_cleartrial_input = {}
        
        if user_id in self.awaiting_cleartrial_input:
            menu_id = self.awaiting_cleartrial_input.pop(user_id)
            try:
                selected_user_id_str = str(int(message.text.strip()))
                
                # Clear the user from trial system
                AUTO_ROLE_CONFIG['active_members'].pop(selected_user_id_str, None)
                AUTO_ROLE_CONFIG['role_history'].pop(selected_user_id_str, None)
                AUTO_ROLE_CONFIG['dm_schedule'].pop(selected_user_id_str, None)
                AUTO_ROLE_CONFIG['weekend_pending'].pop(selected_user_id_str, None)

                if self.db_pool:
                    try:
                        async with self.db_pool.acquire() as conn:
                            await conn.execute(
                                "DELETE FROM active_members WHERE member_id = $1",
                                int(selected_user_id_str))
                            await conn.execute(
                                "DELETE FROM role_history WHERE member_id = $1",
                                int(selected_user_id_str))
                            await conn.execute(
                                "DELETE FROM dm_schedule WHERE member_id = $1",
                                int(selected_user_id_str))
                            await conn.execute(
                                "DELETE FROM weekend_pending WHERE member_id = $1",
                                int(selected_user_id_str))
                    except Exception as e:
                        logger.error(f"Error clearing user {selected_user_id_str} from database: {e}")

                await self.save_auto_role_config()
                if hasattr(self, 'cleartrial_mappings'):
                    self.cleartrial_mappings.pop(menu_id, None)
                
                await message.reply(
                    f"✅ User {selected_user_id_str} removed from all trial tracking")
            except ValueError:
                await message.reply("Invalid member ID. Please provide a numeric ID only.")
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

        # Simple widget for the owner
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📊 Show List", callback_data="at_list"),
                InlineKeyboardButton("📖 Position Guide", callback_data="at_guide")
            ]
        ])
        
        await message.reply(
            "**Active Trades System**\n\nSelect an option below to view trade status or understand the indicators:",
            reply_markup=keyboard
        )

    async def handle_active_trades_callback(self, client: Client, callback_query: CallbackQuery):
        user_id = callback_query.from_user.id
        if not await self.is_owner(user_id):
            await callback_query.answer("Restricted access.", show_alert=True)
            return

        data = callback_query.data
        if data == "at_list":
            await self.show_active_trades_list(callback_query.message)
            await callback_query.answer()
        elif data == "at_guide":
            await self.show_position_guide(callback_query.message)
            await callback_query.answer()

    async def show_active_trades_list(self, message: Message):
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

            live_price = await self.get_live_price(pair)

            if live_price:
                position_info = self.analyze_trade_position(
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
    
    async def show_position_guide(self, message: Message):
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
                        await self.remove_trade_from_db(full_msg_id, 'manual_sl_hit')
                        await self.send_sl_notification(full_msg_id, trade, trade.get('sl_price', 0))
                        successful_trades.append(f"{pair} {action}{group_label} - SL Hit")

                    elif action_type == 'tp1hit':
                        if 'TP1' not in trade.get('tp_hits', []):
                            trade['tp_hits'] = trade.get('tp_hits', []) + ['TP1']
                        trade['status'] = 'active (tp1 hit - manual override)'
                        await self.update_trade_in_db(full_msg_id, trade)
                        await self.send_tp_notification(full_msg_id, trade, 'TP1', trade.get('tp1_price', 0))
                        successful_trades.append(f"{pair} {action}{group_label} - TP1 Hit")

                    elif action_type == 'tp2hit':
                        current_tp_hits = trade.get('tp_hits', [])
                        if 'TP1' not in current_tp_hits:
                            trade['tp_hits'] = trade.get('tp_hits', []) + ['TP1']
                            await self.update_trade_in_db(full_msg_id, trade)
                            await self.send_tp_notification(full_msg_id, trade, 'TP1', trade.get('tp1_price', 0))
                            await asyncio.sleep(1)
                        
                        if 'TP2' not in trade.get('tp_hits', []):
                            trade['tp_hits'] = trade.get('tp_hits', []) + ['TP2']
                        trade['breakeven_active'] = True
                        trade['status'] = 'active (tp2 hit - manual override - breakeven active)'
                        await self.update_trade_in_db(full_msg_id, trade)
                        await self.send_tp_notification(full_msg_id, trade, 'TP2', trade.get('tp2_price', 0))
                        successful_trades.append(f"{pair} {action}{group_label} - TP2 Hit")

                    elif action_type == 'tp3hit':
                        current_tp_hits = trade.get('tp_hits', [])
                        if 'TP1' not in current_tp_hits:
                            trade['tp_hits'] = trade.get('tp_hits', []) + ['TP1']
                            await self.update_trade_in_db(full_msg_id, trade)
                            await self.send_tp_notification(full_msg_id, trade, 'TP1', trade.get('tp1_price', 0))
                            await asyncio.sleep(1)
                        
                        if 'TP2' not in trade.get('tp_hits', []):
                            trade['tp_hits'] = trade.get('tp_hits', []) + ['TP2']
                            trade['breakeven_active'] = True
                            await self.update_trade_in_db(full_msg_id, trade)
                            await self.send_tp_notification(full_msg_id, trade, 'TP2', trade.get('tp2_price', 0))
                            await asyncio.sleep(1)
                        
                        if 'TP3' not in trade.get('tp_hits', []):
                            trade['tp_hits'] = trade.get('tp_hits', []) + ['TP3']
                        trade['status'] = 'completed (tp3 hit - manual override)'
                        await self.remove_trade_from_db(full_msg_id, 'manual_tp3_hit')
                        await self.send_tp_notification(full_msg_id, trade, 'TP3', trade.get('tp3_price', 0))
                        successful_trades.append(f"{pair} {action}{group_label} - TP3 Hit")

                    elif action_type == 'behit':
                        trade['status'] = 'closed (breakeven after tp2 - manual override)'
                        await self.remove_trade_from_db(full_msg_id, 'manual_breakeven_hit')
                        await self.send_breakeven_notification(full_msg_id, trade)
                        successful_trades.append(f"{pair} {action}{group_label} - Breakeven After TP2")

                    elif action_type == 'endhit':
                        trade['status'] = 'closed (ended by manual override)'
                        await self.remove_trade_from_db(full_msg_id, 'manual_end_tracking')
                        successful_trades.append(f"{pair} {action}{group_label} - Tracking Ended")

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
            pair = data[len("pricetest_"):].upper()
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

    async def handle_send_welcome_dm(self, client: Client, message: Message):
        """Owner-only widget to manually send welcome DMs to users"""
        if not await self.is_owner(message.from_user.id):
            await message.reply("This command is restricted to the bot owner.")
            return

        # Store context for this menu session
        menu_id = f"{int(time.time() * 1000)}"
        if not hasattr(self, 'sendwelcomedm_context'):
            self.sendwelcomedm_context = {}
        
        self.sendwelcomedm_context[menu_id] = {
            'stage': 'waiting_for_user_id',
            'user_id': None,
            'msg_type': None
        }

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Enter User ID", callback_data=f"swdm_{menu_id}_input")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"swdm_{menu_id}_cancel")]
        ])

        await message.reply(
            "**Send Welcome DM Widget**\n\n"
            "Click **Enter User ID** to start sending a welcome message to a user.",
            reply_markup=keyboard
        )

    async def handle_sendwelcomedm_callback(self, client: Client, callback_query: CallbackQuery):
        """Handle sendwelcomedm widget callbacks"""
        user_id = callback_query.from_user.id
        
        if not await self.is_owner(user_id):
            await callback_query.answer("This is restricted to the bot owner.", show_alert=True)
            return

        data = callback_query.data
        if not data.startswith("swdm_"):
            return

        parts = data.split("_")
        if len(parts) < 3:
            await callback_query.answer("Invalid callback data.", show_alert=True)
            return

        menu_id = parts[1]
        action = parts[2] if len(parts) > 2 else ""

        if not hasattr(self, 'sendwelcomedm_context'):
            self.sendwelcomedm_context = {}

        context = self.sendwelcomedm_context.get(menu_id, {})

        # Cancel button
        if action == "cancel":
            self.sendwelcomedm_context.pop(menu_id, None)
            await callback_query.message.edit_text("❌ Cancelled.")
            await callback_query.answer()
            return

        # Input button - ask for user ID
        if action == "input":
            context['stage'] = 'waiting_for_user_id'
            self.sendwelcomedm_context[menu_id] = context
            
            # Track that we're awaiting user ID input for this menu
            if not hasattr(self, 'awaiting_sendwelcomedm_input'):
                self.awaiting_sendwelcomedm_input = {}
            self.awaiting_sendwelcomedm_input[callback_query.from_user.id] = menu_id

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data=f"swdm_{menu_id}_cancel")]
            ])

            await callback_query.message.edit_text(
                "**Enter User ID**\n\n"
                "Reply to this message with the Telegram user ID (numeric only).",
                reply_markup=keyboard
            )
            await callback_query.answer()
            return

        await callback_query.answer()

    async def handle_sendwelcomedm_user_input(self, client: Client, message: Message):
        """Handle user ID input for sendwelcomedm widget - implemented in handle_text_input"""
        # This is handled by handle_text_input function at lines 1409-1431
        pass

    async def handle_newmemberslist(self, client: Client, message: Message):
        """Show new members list with subcommands for free group joiners and VIP trial activations"""
        if not await self.is_owner(message.from_user.id):
            await message.reply("This command is restricted to the bot owner.")
            return

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 Free Group Joiners", callback_data="nml_free_group")],
            [InlineKeyboardButton("⭐ VIP Trial Joiners", callback_data="nml_vip_trial")],
            [InlineKeyboardButton("❌ Close", callback_data="nml_close")]
        ])

        await message.reply(
            "**New Members Tracking**\n\n"
            "Select an option to view member data:\n\n"
            "📊 **Free Group Joiners** - Shows all free group joiners grouped by week\n"
            "⭐ **VIP Trial Joiners** - Shows members with active VIP trial and days remaining",
            reply_markup=keyboard
        )

    async def _handle_newmemberslist_callback(self, client: Client, callback_query: CallbackQuery):
        """Handle newmemberslist subcommand callbacks"""
        if not await self.is_owner(callback_query.from_user.id):
            await callback_query.answer("Restricted to bot owner.", show_alert=True)
            return

        data = callback_query.data

        if data == "nml_close":
            await callback_query.message.edit_text("❌ Closed.")
            await callback_query.answer()
            return

        if data == "nml_free_group":
            await self._show_free_group_joiners(callback_query)
        elif data == "nml_vip_trial":
            await self._show_vip_trial_joiners(callback_query)

    async def _show_free_group_joiners(self, callback_query: CallbackQuery):
        """Show free group joiners grouped by week with daily and weekly totals"""
        if not self.db_pool:
            await callback_query.message.edit_text("❌ Database not available")
            await callback_query.answer()
            return

        try:
            current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
            monday = current_time - timedelta(days=current_time.weekday())
            sunday = monday + timedelta(days=6)

            async with self.db_pool.acquire() as conn:
                joins = await conn.fetch(
                    '''SELECT user_id, joined_at FROM free_group_joins 
                       WHERE joined_at >= $1 AND joined_at <= $2
                       ORDER BY joined_at DESC''',
                    monday, sunday
                )

            if not joins:
                await callback_query.message.edit_text(
                    f"**Free Group Joiners - This Week**\n\n"
                    f"Monday {monday.strftime('%d-%m-%Y')} to Sunday {sunday.strftime('%d-%m-%Y')}\n\n"
                    f"No new joiners this week."
                )
                await callback_query.answer()
                return

            joiners_by_date = {}
            for row in joins:
                date_key = row['joined_at'].strftime('%d-%m-%Y')
                if date_key not in joiners_by_date:
                    joiners_by_date[date_key] = []
                joiners_by_date[date_key].append(f"User {row['user_id']}")

            # Calculate weekly total
            total_weekly = len(joins)
            
            text = f"**Free Group Joiners - This Week**\n\nMonday {monday.strftime('%d-%m-%Y')} to Sunday {sunday.strftime('%d-%m-%Y')}\n"
            text += f"**📊 Total This Week: {total_weekly} members**\n\n"
            
            for date in sorted(joiners_by_date.keys(), reverse=True):
                users = ", ".join(joiners_by_date[date])
                daily_count = len(joiners_by_date[date])
                text += f"**{date}** ({daily_count}): {users}\n"

            await callback_query.message.edit_text(text)
            await callback_query.answer()
        except Exception as e:
            await callback_query.message.edit_text(f"❌ Error: {str(e)}")
            await callback_query.answer()

    async def _show_vip_trial_joiners(self, callback_query: CallbackQuery):
        """Show all active VIP trial members with time remaining and join dates"""
        try:
            current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
            
            if not AUTO_ROLE_CONFIG['active_members']:
                await callback_query.message.edit_text(
                    "**Active VIP Trial Members**\n\n"
                    "No active trial members at this time."
                )
                await callback_query.answer()
                return

            # Get all active trial members with dates
            trials_with_time = []
            joiners_by_date = {}
            
            for user_id_str, member_data in AUTO_ROLE_CONFIG['active_members'].items():
                expiry = datetime.fromisoformat(
                    member_data.get('expiry_time', current_time.isoformat()))
                if expiry.tzinfo is None:
                    expiry = AMSTERDAM_TZ.localize(expiry)
                
                joined = datetime.fromisoformat(
                    member_data.get('joined_at', current_time.isoformat()))
                if joined.tzinfo is None:
                    joined = AMSTERDAM_TZ.localize(joined)
                
                join_date_str = joined.strftime('%d-%m-%Y')
                
                time_left = expiry - current_time
                total_seconds = max(0, time_left.total_seconds())
                
                trials_with_time.append({
                    'user_id': user_id_str,
                    'total_seconds': total_seconds,
                    'expiry': expiry,
                    'join_date': join_date_str,
                    'joined': joined
                })
                
                # Group by date for weekly summary
                if join_date_str not in joiners_by_date:
                    joiners_by_date[join_date_str] = 0
                joiners_by_date[join_date_str] += 1
            
            # Sort by time remaining (ascending - least time first)
            trials_with_time.sort(key=lambda x: x['total_seconds'])
            
            text = "**Active VIP Trial Members**\n\n"
            
            # Weekly summary
            total_weekly = len(trials_with_time)
            text += f"**📊 Total This Week: {total_weekly} members**\n\n"
            
            # Group members by date for compact display
            members_by_date = {}
            for trial in trials_with_time:
                date_key = trial['join_date']
                if date_key not in members_by_date:
                    members_by_date[date_key] = []
                
                hours = int(trial['total_seconds'] // 3600)
                minutes = int((trial['total_seconds'] % 3600) // 60)
                members_by_date[date_key].append(f"User {trial['user_id']} ({hours}h {minutes}m)")

            # Compact display grouped by date
            for date in sorted(members_by_date.keys(), reverse=True):
                users = ", ".join(members_by_date[date])
                daily_count = len(members_by_date[date])
                text += f"**{date}** ({daily_count}): {users}\n"

            await callback_query.message.edit_text(text)
            await callback_query.answer()
        except Exception as e:
            await callback_query.message.edit_text(f"❌ Error: {str(e)}")
            await callback_query.answer()

    async def handle_peer_id_status(self, client: Client, message: Message):
        """Check if a user's peer ID has been established - widget style"""
        if not await self.is_owner(message.from_user.id):
            return
        
        args = message.text.split()
        
        # If user ID provided as argument, show status directly
        if len(args) >= 2:
            try:
                user_id = int(args[1])
                await self._show_peer_id_status(message, user_id)
            except ValueError:
                await message.reply("❌ Invalid user ID. Please provide a valid number.")
            return
        
        # Otherwise show widget for user to input ID
        text = "**🔍 Peer ID Status Checker**\n\nSend me a user ID to check their peer establishment status."
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="pid_cancel")]
        ])
        
        await message.reply(text, reply_markup=keyboard)
        
        # Set up context for text input
        if not hasattr(self, '_waiting_for_peer_id'):
            self._waiting_for_peer_id = {}
        self._waiting_for_peer_id[message.from_user.id] = True
    
    async def _show_peer_id_status(self, message: Message, user_id: int):
        """Display peer ID status for a user"""
        if not self.db_pool:
            await message.reply("❌ Database not available")
            return
        
        try:
            async with self.db_pool.acquire() as conn:
                peer_check = await conn.fetchrow(
                    'SELECT * FROM peer_id_checks WHERE user_id = $1',
                    user_id
                )
            
            if not peer_check:
                await message.reply(f"❌ No peer ID check found for user {user_id}")
                return
            
            is_established = peer_check['peer_id_established']
            established_at = peer_check['established_at']
            joined_at = peer_check['joined_at']
            current_delay = peer_check['current_delay_minutes']
            welcome_sent = peer_check['welcome_dm_sent']
            
            status_text = f"**Peer ID Status for User {user_id}**\n\n"
            
            if is_established:
                status_text += f"✅ **Peer ID: ESTABLISHED**\n"
                if established_at:
                    status_text += f"📅 Established at: {established_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                status_text += f"💬 Welcome DM sent: {'Yes ✓' if welcome_sent else 'No ✗'}\n"
            else:
                status_text += f"⏳ **Peer ID: NOT YET ESTABLISHED**\n"
                status_text += f"📅 Joined at: {joined_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                status_text += f"⏱️ Current retry delay: {current_delay} minutes\n"
                status_text += f"💬 Welcome DM sent: {'Yes ✓' if welcome_sent else 'No ✗'}\n"
                
                # Calculate time elapsed
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                if joined_at.tzinfo is None:
                    joined_at = AMSTERDAM_TZ.localize(joined_at)
                elapsed = (current_time - joined_at).total_seconds() / 3600
                status_text += f"⏳ Time elapsed: {elapsed:.1f} hours\n"
            
            await message.reply(status_text)
            
        except Exception as e:
            await message.reply(f"❌ Error checking peer ID status: {str(e)}")

    async def handle_peerid_callback(self, client: Client, callback_query: CallbackQuery):
        """Handle peer ID callback (cancel button)"""
        if not await self.is_owner(callback_query.from_user.id):
            await callback_query.answer("Restricted to bot owner.", show_alert=True)
            return
        
        if callback_query.data == "pid_cancel":
            if not hasattr(self, '_waiting_for_peer_id'):
                self._waiting_for_peer_id = {}
            
            user_id = callback_query.from_user.id
            self._waiting_for_peer_id.pop(user_id, None)
            
            await callback_query.message.edit_text("❌ Peer ID check cancelled.")
            await callback_query.answer()

    async def handle_dmmessages(self, client: Client, message: Message):
        """Show DM message topics menu"""
        if not await self.is_owner(message.from_user.id):
            return

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Free Trial Heads Up", callback_data="dmm_topic_ft")],
            [InlineKeyboardButton("📅 3/7/14 Day Follow-ups", callback_data="dmm_topic_fu")],
            [InlineKeyboardButton("👋 Welcome & Onboarding", callback_data="dmm_topic_wo")],
            [InlineKeyboardButton("🎁 Engagement & Offers", callback_data="dmm_topic_eo")],
            [InlineKeyboardButton("❌ Close", callback_data="dmm_close")]
        ])

        await message.reply(
            "**DM Message Templates**\n\n"
            "Select a topic to preview the DM messages:\n\n"
            "📢 **Free Trial Heads Up** - Trial expiration warnings (24hr, 3hr)\n"
            "📅 **3/7/14 Day Follow-ups** - Post-trial follow-up messages\n"
            "👋 **Welcome & Onboarding** - Welcome and activation messages\n"
            "🎁 **Engagement & Offers** - Discount and trial offer messages",
            reply_markup=keyboard
        )

    async def _handle_dmmessages_callback(self, client: Client, callback_query: CallbackQuery):
        """Handle DM message selection"""
        if not await self.is_owner(callback_query.from_user.id):
            await callback_query.answer("Restricted to bot owner.", show_alert=True)
            return

        data = callback_query.data

        if data == "dmm_close":
            await callback_query.message.edit_text("❌ Closed.")
            await callback_query.answer()
            return

        topic_map = {
            "dmm_topic_ft": "Free Trial Heads Up",
            "dmm_topic_fu": "3/7/14 Day Follow-ups",
            "dmm_topic_wo": "Welcome & Onboarding",
            "dmm_topic_eo": "Engagement & Offers"
        }

        for callback_id, topic_name in topic_map.items():
            if data == callback_id:
                topic = MESSAGE_TEMPLATES[topic_name]
                buttons = []
                
                for msg_title in topic.keys():
                    msg_id = topic[msg_title]["id"]
                    buttons.append([InlineKeyboardButton(f"📝 {msg_title}", callback_data=f"dmm_msg_{msg_id}")])
                
                buttons.append([InlineKeyboardButton("🔙 Back", callback_data="dmm_back")])
                keyboard = InlineKeyboardMarkup(buttons)

                await callback_query.message.edit_text(
                    f"**{topic_name}**\n\nSelect a message to preview:",
                    reply_markup=keyboard
                )
                await callback_query.answer()
                return

        if data == "dmm_back":
            await self.handle_dmmessages(client, callback_query.message)
            await callback_query.answer()
            return

        if data.startswith("dmm_msg_"):
            msg_id = data[len("dmm_msg_"):]
            
            for topic in MESSAGE_TEMPLATES.values():
                for msg_title, msg_data in topic.items():
                    if msg_data["id"] == msg_id:
                        msg_content = msg_data["message"]
                        buttons = [[InlineKeyboardButton("🔙 Back", callback_data="dmm_back")]]
                        keyboard = InlineKeyboardMarkup(buttons)
                        
                        await callback_query.message.edit_text(
                            f"**Preview: {msg_title}**\n\n{msg_content}",
                            reply_markup=keyboard
                        )
                        await callback_query.answer()
                        logger.info(f"Owner previewed DM message: {msg_title}")
                        return

        await callback_query.answer()

    async def execute_send_welcome_dm(self, callback_query: CallbackQuery, menu_id: str, context: dict, user_message: Message = None):
        """Execute the actual welcome DM send"""
        user_id = context.get('user_id')

        if not user_id:
            if callback_query:
                await callback_query.message.edit_text("❌ Missing user ID.")
                await callback_query.answer()
            else:
                await user_message.reply("❌ Missing user ID.")
            self.sendwelcomedm_context.pop(menu_id, None)
            return

        # Prepare the welcome message
        welcome_msg = MESSAGE_TEMPLATES["Welcome & Onboarding"]["Welcome DM (New Free Group Member)"]["message"]

        # Queue the message for the userbot instead of sending directly
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO userbot_dm_queue (user_id, message, label, status) VALUES ($1, $2, $3, 'pending')",
                        user_id, welcome_msg, "Welcome DM (Manual)"
                    )
                
                if callback_query:
                    await callback_query.message.edit_text(
                        f"✅ **Queued!**\n\n"
                        f"Welcome DM queued for user **{user_id}** via Userbot Service."
                    )
                else:
                    await user_message.reply(
                        f"✅ **Queued!**\n\n"
                        f"Welcome DM queued for user **{user_id}** via Userbot Service."
                    )
                return
            except Exception as db_err:
                logger.warning(f"Could not queue welcome DM for user {user_id}: {db_err}")

        # Fallback to direct send if DB fails or not available
        try:
            # Establish peer connection first
            try:
                await self.app.resolve_peer(user_id)
            except Exception:
                try:
                    await self.app.get_users(user_id)
                except Exception:
                    pass
            await asyncio.sleep(1)
            await self.app.send_message(user_id, welcome_msg)
            
            # ✅ SYNC WITH PEER_ID_CHECKS TABLE
            if self.db_pool:
                try:
                    async with self.db_pool.acquire() as conn:
                        await conn.execute(
                            'UPDATE peer_id_checks SET welcome_dm_sent = TRUE WHERE user_id = $1',
                            user_id
                        )
                except Exception as db_err:
                    logger.warning(f"Could not update peer_id_checks for user {user_id}: {db_err}")
            
            if callback_query:
                await callback_query.message.edit_text(
                    f"✅ **Success!**\n\n"
                    f"Welcome DM sent to user **{user_id}**"
                )
            else:
                await user_message.reply(
                    f"✅ **Success!**\n\n"
                    f"Welcome DM sent to user **{user_id}**"
                )
            await self.log_to_debug(f"Owner sent welcome DM to user {user_id} via /sendwelcomedm widget", user_id=user_id)
        except Exception as e:
            error_str = str(e)
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
            else:
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

    async def handle_timed_auto_role(self, client: Client, message: Message):
        if not await self.is_owner(message.from_user.id):
            return

        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("📊 View Stats", callback_data="tar_status")],
             [InlineKeyboardButton("📋 List Active Users", callback_data="tar_list")],
             [InlineKeyboardButton("⏱️ Edit Time Duration", callback_data="tar_retract")],
             [InlineKeyboardButton("🗑️ Clear Trial History", callback_data="tar_clear")],
             [InlineKeyboardButton("Cancel", callback_data="tar_cancel")]])

        await message.reply("**Free Trial Management**\n\n"
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

            # Sort by time remaining (ascending - least time first)
            members_with_time = []
            for member_id, data_item in AUTO_ROLE_CONFIG['active_members'].items():
                expiry = datetime.fromisoformat(
                    data_item.get('expiry_time', current_time.isoformat()))
                if expiry.tzinfo is None:
                    expiry = AMSTERDAM_TZ.localize(expiry)

                time_left = expiry - current_time
                total_seconds = max(0, time_left.total_seconds())
                members_with_time.append((member_id, data_item, total_seconds))

            # Sort by time remaining (ascending)
            members_with_time.sort(key=lambda x: x[2])

            for member_id, data_item, total_seconds in members_with_time[:20]:
                hours = int(total_seconds // 3600)
                minutes = int((total_seconds % 3600) // 60)

                weekend = " (weekend)" if data_item.get(
                    'weekend_delayed') else ""
                response += f"User {member_id}: {hours}h {minutes}m left{weekend}\n"

            if len(AUTO_ROLE_CONFIG['active_members']) > 20:
                response += f"\n_...and {len(AUTO_ROLE_CONFIG['active_members']) - 20} more_"

            await callback_query.message.edit_text(response)

        elif data == "tar_retract":
            if not AUTO_ROLE_CONFIG['active_members']:
                await callback_query.message.edit_text(
                    "No active trial members found.")
                await callback_query.answer()
                return

            if not hasattr(self, 'retracttrial_mappings'):
                self.retracttrial_mappings = {}

            menu_id = f"rt_{int(time.time() * 1000)}"
            user_mapping = {}
            buttons = []
            current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)

            # Sort by time remaining (ascending - least time first)
            members_with_time = []
            for user_id_str, member_data in AUTO_ROLE_CONFIG['active_members'].items():
                expiry = datetime.fromisoformat(member_data.get('expiry_time', ''))
                if expiry.tzinfo is None:
                    expiry = AMSTERDAM_TZ.localize(expiry)

                total_seconds = max(0, (expiry - current_time).total_seconds())
                members_with_time.append((user_id_str, member_data, total_seconds))

            # Sort by time remaining (ascending)
            members_with_time.sort(key=lambda x: x[2])

            for idx, (user_id_str, member_data, total_seconds) in enumerate(members_with_time[:20]):
                user_mapping[str(idx)] = user_id_str
                hours = int(total_seconds // 3600)
                minutes = int((total_seconds % 3600) // 60)

                buttons.append([
                    InlineKeyboardButton(
                        f"User {user_id_str} ({hours}h {minutes}m left)",
                        callback_data=f"retrt_{menu_id}_select_{idx}")
                ])

            self.retracttrial_mappings[menu_id] = user_mapping
            buttons.append([
                InlineKeyboardButton("Back", callback_data="tar_status"),
                InlineKeyboardButton("Cancel", callback_data=f"retrt_{menu_id}_cancel")
            ])

            keyboard = InlineKeyboardMarkup(buttons)
            await callback_query.message.edit_text(
                "**Retract Trial Time**\n\n"
                "Select a user to adjust their trial expiry time:",
                reply_markup=keyboard)

        elif data == "tar_clear":
            if not hasattr(self, 'cleartrial_mappings'):
                self.cleartrial_mappings = {}

            menu_id = f"ct_{int(time.time() * 1000)}"
            user_mapping = {}
            buttons = []

            # Add active members if they exist
            if AUTO_ROLE_CONFIG['active_members']:
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                
                # Sort by time remaining (ascending - least time first)
                members_with_time = []
                for user_id_str, member_data in AUTO_ROLE_CONFIG['active_members'].items():
                    expiry = datetime.fromisoformat(member_data.get('expiry_time', ''))
                    if expiry.tzinfo is None:
                        expiry = AMSTERDAM_TZ.localize(expiry)
                    
                    total_seconds = max(0, (expiry - current_time).total_seconds())
                    members_with_time.append((user_id_str, member_data, total_seconds))
                
                # Sort by time remaining (ascending)
                members_with_time.sort(key=lambda x: x[2])
                
                for idx, (user_id_str, member_data, total_seconds) in enumerate(members_with_time[:20]):
                    user_mapping[str(idx)] = user_id_str
                    buttons.append([
                        InlineKeyboardButton(
                            f"User {user_id_str}",
                            callback_data=f"clrtrl_{menu_id}_select_{idx}")
                    ])

            buttons.append([
                InlineKeyboardButton(
                    "📝 Enter Custom Member ID",
                    callback_data=f"clrtrl_{menu_id}_custom")
            ])
            buttons.append([
                InlineKeyboardButton("Back", callback_data="tar_status"),
                InlineKeyboardButton("Cancel", callback_data=f"clrtrl_{menu_id}_cancel")
            ])

            self.cleartrial_mappings[menu_id] = user_mapping
            keyboard = InlineKeyboardMarkup(buttons)
            
            status_text = "**Clear Trial History**\n\n"
            if AUTO_ROLE_CONFIG['active_members']:
                status_text += "Select a user to remove from trial tracking, or enter a custom member ID:"
            else:
                status_text += "No active trial members. Enter a custom member ID to clear history:"
            
            await callback_query.message.edit_text(
                status_text,
                reply_markup=keyboard)

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
        for idx, (user_id_str, member_data) in enumerate(
                list(AUTO_ROLE_CONFIG['active_members'].items())[:20]):
            user_mapping[str(idx)] = user_id_str
            expiry = datetime.fromisoformat(member_data.get('expiry_time', ''))
            if expiry.tzinfo is None:
                expiry = AMSTERDAM_TZ.localize(expiry)

            total_seconds = max(0, (expiry - datetime.now(
                pytz.UTC).astimezone(AMSTERDAM_TZ)).total_seconds())
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
                await callback_query.answer("User not found.", show_alert=True)
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
                InlineKeyboardButton(
                    "Custom Hours/Minutes",
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
                await callback_query.answer("Invalid action.", show_alert=True)
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
                await callback_query.answer("User not found.", show_alert=True)
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
                await callback_query.answer("Invalid action.", show_alert=True)
                return

            user_mapping = self.retracttrial_mappings.get(menu_id, {})
            user_id_str = user_mapping.get(idx)

            if not user_id_str or user_id_str not in AUTO_ROLE_CONFIG[
                    'active_members']:
                await callback_query.answer("User not found.", show_alert=True)
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

                    total_seconds = max(0, (expiry - datetime.now(
                        pytz.UTC).astimezone(AMSTERDAM_TZ)).total_seconds())
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


    async def handle_cleartrial_callback(self, client: Client,
                                         callback_query: CallbackQuery):
        """Handle clear trial callback for user selection and clearing"""
        user_id = callback_query.from_user.id

        if not await self.is_owner(user_id):
            await callback_query.answer("This is restricted to the bot owner.",
                                        show_alert=True)
            return

        data = callback_query.data

        if not data.startswith("clrtrl_"):
            return

        parts = data.split("_")
        if len(parts) < 3:
            await callback_query.answer("Invalid callback data.",
                                        show_alert=True)
            return

        menu_id = parts[1]
        action_type = parts[2]

        if not hasattr(self, 'cleartrial_mappings'):
            self.cleartrial_mappings = {}

        if action_type == "cancel":
            self.cleartrial_mappings.pop(menu_id, None)
            await callback_query.message.edit_text("Cancelled.")
            await callback_query.answer()
            return

        if action_type == "custom":
            self.cleartrial_mappings[menu_id] = {'waiting_for_id': True}
            
            # Track that we're awaiting custom member ID input
            if not hasattr(self, 'awaiting_cleartrial_input'):
                self.awaiting_cleartrial_input = {}
            self.awaiting_cleartrial_input[callback_query.from_user.id] = menu_id
            
            await callback_query.message.edit_text(
                "**Enter Member ID**\n\n"
                "Please reply with the member ID you want to clear from trial tracking."
            )
            await callback_query.answer()
            return

        if action_type == "select":
            idx = parts[3] if len(parts) > 3 else None
            if not idx:
                await callback_query.answer("Invalid selection.",
                                            show_alert=True)
                return

            user_mapping = self.cleartrial_mappings.get(menu_id, {})
            selected_user_id_str = user_mapping.get(idx)

            if not selected_user_id_str or selected_user_id_str == 'waiting_for_id':
                await callback_query.answer("User not found.", show_alert=True)
                return

            # Clear the user from trial system
            AUTO_ROLE_CONFIG['active_members'].pop(selected_user_id_str, None)
            AUTO_ROLE_CONFIG['role_history'].pop(selected_user_id_str, None)
            AUTO_ROLE_CONFIG['dm_schedule'].pop(selected_user_id_str, None)
            AUTO_ROLE_CONFIG['weekend_pending'].pop(selected_user_id_str, None)

            if self.db_pool:
                try:
                    async with self.db_pool.acquire() as conn:
                        await conn.execute(
                            "DELETE FROM active_members WHERE member_id = $1",
                            int(selected_user_id_str))
                        await conn.execute(
                            "DELETE FROM role_history WHERE member_id = $1",
                            int(selected_user_id_str))
                        await conn.execute(
                            "DELETE FROM dm_schedule WHERE member_id = $1",
                            int(selected_user_id_str))
                        await conn.execute(
                            "DELETE FROM weekend_pending WHERE member_id = $1",
                            int(selected_user_id_str))
                except Exception as e:
                    logger.error(f"Error clearing user {selected_user_id_str} from database: {e}")

            await self.save_auto_role_config()
            self.cleartrial_mappings.pop(menu_id, None)
            await callback_query.message.edit_text(
                f"✅ User {selected_user_id_str} removed from all trial tracking")
            await callback_query.answer()
            return

        await callback_query.answer()

    async def process_join_request(self, client: Client,
                                   join_request: ChatJoinRequest):
        # Debug: always log that we received a join request
        chat_name = getattr(join_request.chat, 'title', 'Unknown')
        logger.info(
            f"📨 Join request received from {join_request.from_user.first_name} (ID: {join_request.from_user.id}) in chat {chat_name} (ID: {join_request.chat.id})"
        )

        # Validate VIP_GROUP_ID
        if VIP_GROUP_ID == 0:
            logger.error(
                "❌ VIP_GROUP_ID not set! Join requests cannot be processed.")
            return

        if join_request.chat.id != VIP_GROUP_ID:
            logger.debug(
                f"Join request is for different group (got {join_request.chat.id}, expected {VIP_GROUP_ID})"
            )
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
                    f"❌ Rejected join request from {user_name} (ID: {user_id}) - trial already used before"
                )

                try:
                    await join_request.decline()
                    logger.info(
                        f"✅ Declined join request for {user_name} (ID: {user_id})"
                    )
                except Exception as e:
                    logger.error(f"Error declining join request: {e}")

                # Send friendly DM about re-using trial
                try:
                    rejection_dm = MESSAGE_TEMPLATES["Trial Status & Expiry"]["Trial Rejected (Used Before)"]["message"].replace("{user_name}", user_name)
                    await client.send_message(user_id,
                                              rejection_dm,
                                              disable_web_page_preview=True)
                    logger.info(f"Sent rejection DM to {user_name}")
                except Exception as e:
                    logger.error(
                        f"Could not send rejection DM to {user_name}: {e}")
                return

            # Track this user as trial approval (they're joining via trial link with approval)
            self.trial_pending_approvals.add(user_id)
            logger.info(f"✅ Added {user_id} to trial_pending_approvals set")

            # Actually approve the request
            await join_request.approve()
            logger.info(
                f"✅ Successfully approved join request for {user_name} (ID: {user_id})"
            )

            await self.log_to_debug(
                f"🎯 Auto-approved trial join request from {user_name} (ID: {user_id}) - waiting for member join event..."
            )
        except Exception as e:
            logger.error(
                f"❌ Error processing join request from {join_request.from_user.first_name}: {type(e).__name__}: {e}"
            )
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
        # Track free group join for engagement tracking and peer ID verification
        if self.db_pool:
            try:
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                
                # FIX 1: Forced Resolution - Immediately try to get member info to prime cache
                try:
                    await client.get_chat_member(FREE_GROUP_ID, user.id)
                    # ALSO immediately attempt to establish a Peer ID globally
                    await client.get_users([user.id])
                except Exception as e:
                    logger.debug(f"Forced resolution/global fetch failed for {user.id}: {e}")

                async with self.db_pool.acquire() as conn:
                    # Track for engagement
                    await conn.execute(
                        '''INSERT INTO free_group_joins (user_id, joined_at, discount_sent)
                           VALUES ($1, $2, FALSE)
                           ON CONFLICT (user_id) DO NOTHING''',
                        user.id, current_time)
                    
                    # Track for peer ID verification (30 min delay, check every 3 min)
                    next_check = current_time + timedelta(minutes=3)
                    await conn.execute('''
                        INSERT INTO peer_id_checks (user_id, joined_at, current_delay_minutes, current_interval_minutes, next_check_at)
                        VALUES ($1, $2, 30, 3, $3)
                        ON CONFLICT (user_id) DO NOTHING
                    ''', user.id, current_time, next_check)
                    
                    # Queue Welcome DM for Userbot
                    welcome_dm = MESSAGE_TEMPLATES["Welcome & Onboarding"]["Welcome DM (New Free Group Member)"]["message"].replace("{user_name}", user.first_name or "Trader")
                    await conn.execute(
                        "INSERT INTO userbot_dm_queue (user_id, message, label, status) VALUES ($1, $2, 'Welcome DM', 'pending')",
                        user.id, welcome_dm
                    )
                
                await self.log_to_debug(f"👤 New member joined FREE group: {user.first_name} (ID: {user.id}) - Peer ID verification and Welcome DM queued")
            except Exception as e:
                logger.error(f"Error tracking free group join for {user.id}: {e}")

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
                f"💳 {user.first_name} (ID: {user.id}) joined via paid link - no trial registration needed",
                user_id=user.id
            )
            return

        await self.log_to_debug(
            f"🆓 Trial join detected: {user.first_name} (ID: {user.id})",
            user_id=user.id)

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
                f"⚠️ Warning: User {user.first_name} ({user.id}) joined despite trial already used - should have been rejected at join request"
            )

            try:
                rejection_dm = MESSAGE_TEMPLATES["Trial Status & Expiry"]["Trial Rejected (Used Before)"]["message"].replace("{user_name}", user.first_name)
                await client.send_message(user.id,
                                          rejection_dm,
                                          disable_web_page_preview=True)
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
        
        # Record trial activation in database for /newmemberslist command
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute('''
                        INSERT INTO vip_trial_activations (user_id, activation_date, expiry_date)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (user_id) DO UPDATE SET
                        activation_date = $2, expiry_date = $3
                    ''', user.id, current_time, expiry_time)
            except Exception as e:
                logger.error(f"Error recording trial activation in database: {e}")
        
        # NOTE: Welcome DM is sent to users when they join the FREE group, not here.
        # Users who join the VIP group have already activated their trial by clicking the invite link.
        await self.log_to_debug(
            f"Trial activated for {user.first_name} (expires {expiry_time.strftime('%A at %H:%M')})")

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

    async def get_live_price_with_fallback(self, pair: str, assigned_api: Optional[str] = None) -> Optional[float]:
        """Get live price - try assigned API first, then fallback to all APIs if assigned fails"""
        pair_clean = pair.upper().replace("/",
                                          "").replace("-",
                                                      "").replace("_", "")

        priority = PRICE_TRACKING_CONFIG["api_priority_order"]
        
        # Ensure we have a valid session
        if not self.client_session or self.client_session.closed:
            self.client_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        
        # If an API is already assigned to this trade, try that first
        search_order = priority.copy()
        if assigned_api and assigned_api in search_order:
            search_order.remove(assigned_api)
            search_order.insert(0, assigned_api)
        
        for api_name in search_order:
            try:
                price = await self.get_price_from_api(api_name, pair_clean)
                if price is not None:
                    return price
            except Exception as e:
                logger.debug(f"Price fallback error for {api_name}: {e}")
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

    async def check_message_still_exists(self, message_id: str, trade_data: dict) -> bool:
        """Check if the original trading signal message still exists in Telegram
        
        SECURITY: Only deletes trades if message explicitly verified as deleted.
        Assumes message exists on any error to prevent accidental deletion of active trades.
        """
        try:
            # Validate input
            chat_id = trade_data.get('group_id')
            if not chat_id:
                logger.warning(f"check_message_still_exists: No group_id in trade_data for message {message_id}")
                return True  # Assume exists if we can't verify
            
            # Extract the actual message number from composite ID (format: "group_id_message_id")
            actual_msg_id_str = str(message_id)
            if '_' in actual_msg_id_str:
                actual_msg_id_str = actual_msg_id_str.split('_', 1)[1]
            
            # Validate that we have a numeric message ID
            try:
                actual_msg_id = int(actual_msg_id_str)
            except ValueError:
                logger.error(f"check_message_still_exists: Invalid message ID format '{message_id}' - cannot parse to integer. Assuming message exists.")
                return True  # Safer to assume exists than to delete on parse error
            
            # Validate chat_id is numeric
            try:
                chat_id_int = int(chat_id)
            except (ValueError, TypeError):
                logger.error(f"check_message_still_exists: Invalid chat_id '{chat_id}' - cannot parse to integer. Assuming message exists.")
                return True  # Safer to assume exists
            
            # Try to fetch the message from the group with timeout
            try:
                message = await asyncio.wait_for(
                    self.app.get_messages(chat_id_int, actual_msg_id),
                    timeout=10
                )
                return message is not None
            except asyncio.TimeoutError:
                logger.warning(f"Timeout checking if message {message_id} exists in chat {chat_id}. Assuming it still exists.")
                return True  # Timeout = assume exists, don't delete
                
        except Exception as e:
            # Only treat as deleted if it's a specific "not found" error
            error_str = str(e).lower()
            if "not found" in error_str or "message_id_invalid" in error_str or "message deleted" in error_str:
                logger.info(f"Message {message_id} verified as deleted from Telegram: {e}")
                return False
            
            # For any other error (network, permissions, etc), assume message still exists
            # This prevents false deletions due to transient issues
            logger.warning(f"Unexpected error checking message {message_id}: {type(e).__name__}: {e}. Assuming message still exists to prevent accidental deletion.")
            return True

    async def check_single_trade_immediately(self, message_id: str,
                                             trade_data: dict):
        await asyncio.sleep(5)
        await self.check_price_levels(message_id, trade_data)

    async def verify_trade_data_consistency(self, message_id: str, trade_data: dict) -> dict:
        """Verify trade data consistency between memory and database - prevents missed hits"""
        try:
            if not self.db_pool:
                return trade_data
                
            # Fetch latest data from database
            async with self.db_pool.acquire() as conn:
                db_trade = await conn.fetchrow(
                    'SELECT * FROM active_trades WHERE message_id = $1',
                    message_id)
                
                if not db_trade:
                    logger.warning(f"Trade {message_id} not found in database - may have been deleted")
                    return trade_data
                
                # Sync critical fields from database (these are source of truth)
                synced_data = dict(trade_data)
                synced_data['status'] = db_trade.get('status', trade_data.get('status', 'active'))
                
                # Parse TP hits from database
                tp_hits_str = db_trade.get('tp_hits', '')
                synced_data['tp_hits'] = [h.strip() for h in tp_hits_str.split(',') if h.strip()]
                
                synced_data['breakeven_active'] = db_trade.get('breakeven_active', False)
                synced_data['manual_overrides'] = db_trade.get('manual_overrides', '')
                
                # Check if memory data differs from database
                if synced_data.get('tp_hits') != trade_data.get('tp_hits', []):
                    logger.info(f"Trade {message_id}: TP hits synced from DB. Memory: {trade_data.get('tp_hits')}, DB: {synced_data.get('tp_hits')}")
                    # Update memory with database values
                    trade_data['tp_hits'] = synced_data['tp_hits']
                
                if synced_data.get('breakeven_active') != trade_data.get('breakeven_active'):
                    logger.info(f"Trade {message_id}: Breakeven status synced from DB")
                    trade_data['breakeven_active'] = synced_data['breakeven_active']
                    
                if synced_data.get('status') != trade_data.get('status'):
                    logger.info(f"Trade {message_id}: Status synced from DB. Memory: {trade_data.get('status')}, DB: {synced_data.get('status')}")
                    trade_data['status'] = synced_data['status']
                
                return trade_data
                
        except Exception as e:
            logger.error(f"Error verifying trade data consistency for {message_id}: {e}")
            return trade_data

    async def check_price_levels(self, message_id: str, trade_data: dict):
        # Skip price monitoring for manually-tracked pairs (e.g., XAUUSD, BTCUSD, GER40, US100)
        if trade_data.get('manual_tracking_only', False):
            return
        
        # First check if the original message still exists (cleanup deleted signals)
        if not await self.check_message_still_exists(message_id, trade_data):
            await self.remove_trade_from_db(message_id, "message_deleted")
            if message_id in PRICE_TRACKING_CONFIG['active_trades']:
                del PRICE_TRACKING_CONFIG['active_trades'][message_id]
            return
        
        # Verify trade data consistency between memory and database
        trade_data = await self.verify_trade_data_consistency(message_id, trade_data)
        
        pair = trade_data.get('pair')
        action = trade_data.get('action')
        tp1 = trade_data.get('tp1_price')
        tp2 = trade_data.get('tp2_price')
        tp3 = trade_data.get('tp3_price')
        sl = trade_data.get('sl_price')
        tp_hits = trade_data.get('tp_hits', [])
        breakeven_active = trade_data.get('breakeven_active', False)
        live_entry = trade_data.get('live_entry') or trade_data.get('entry')
        
        # Rule 1: If TP2 was already hit, ensure breakeven protection is active
        if 'TP2' in tp_hits:
            breakeven_active = True

        # Try assigned API first, then fallback to all APIs if it fails
        current_price = await self.get_live_price_with_fallback(pair, trade_data.get('assigned_api'))
        if not current_price:
            return

        is_buy = action.upper() == "BUY"

        if is_buy:
            if breakeven_active and live_entry:
                if current_price <= live_entry:
                    await self.handle_breakeven_hit(message_id, trade_data)
                    return
            elif current_price <= sl:
                # Rule 2: SL cannot hit after TP2 (breakeven protection)
                if 'TP2' not in tp_hits:
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
                # Rule 2: SL cannot hit after TP2 (breakeven protection)
                if 'TP2' not in tp_hits:
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
            f"{trade['pair']} {trade['action']} hit breakeven @ {trade['entry_price']:.5f}"
        )

    def validate_chronological_hits(self, hits: list) -> list:
        """Validate hits chronologically according to trading rules (Feature 3: Chronological Hit Validation)"""
        valid_hits = []
        sl_hit = False
        tp_levels_hit = set()

        for hit in sorted(hits, key=lambda x: x.get('hit_time', 0)):
            hit_type = hit.get('hit_type')
            hit_level = hit.get('hit_level')

            # Rule 1: If SL was hit first, ignore all subsequent TP hits
            if sl_hit and hit_type == 'tp':
                continue

            # Rule 2: If SL hits after TP2, ignore it (breakeven protection)
            if hit_type == 'sl' and 'TP2' in tp_levels_hit:
                continue

            # Rule 3: Cannot hit both TP3 and SL
            if hit_type == 'sl' and 'TP3' in tp_levels_hit:
                continue
            if hit_type == 'tp' and hit_level == 'TP3' and sl_hit:
                continue

            # Hit is valid according to trading rules
            valid_hits.append(hit)

            # Update state tracking
            if hit_type == 'sl':
                sl_hit = True
            elif hit_type == 'tp':
                tp_levels_hit.add(hit_level)

        return valid_hits

    async def recover_missed_signals(self):
        """Check for trading signals sent while bot was offline (Feature 6: Missed Signal Recovery)"""
        if not PRICE_TRACKING_CONFIG["enabled"]:
            return

        try:
            await self.log_to_debug("🔍 Scanning for missed trading signals while offline...")

            # Get the last known online time
            offline_check_time = self.last_online_time
            if not offline_check_time:
                # If we don't know when we were last online, check last 12 hours
                offline_check_time = datetime.now(AMSTERDAM_TZ) - timedelta(hours=12)

            recovered_signals = 0

            # Check both VIP and Free groups for missed signals
            for group_id in [VIP_GROUP_ID, FREE_GROUP_ID]:
                if not group_id:
                    continue

                try:
                    # Scan recent messages from the group
                    async for message in self.app.get_chat_history(group_id, limit=100):
                        if not message or not message.text:
                            continue

                        # Only process signals from owner
                        if message.from_user and message.from_user.id != BOT_OWNER_USER_ID:
                            continue

                        # Check if message contains trading signal
                        if "Trade Signal For:" not in message.text:
                            continue

                        # Skip if already being tracked
                        trade_key = f"{group_id}_{message.id}"
                        if trade_key in PRICE_TRACKING_CONFIG['active_trades']:
                            continue

                        # Parse the signal
                        trade_data = self.parse_signal_message(message.text)
                        if not trade_data:
                            continue

                        # Save to database and start tracking
                        trade_data['group_id'] = group_id
                        trade_data['message_id'] = str(message.id)
                        trade_data['chat_id'] = group_id
                        trade_data['created_at'] = datetime.now(AMSTERDAM_TZ)

                        PRICE_TRACKING_CONFIG['active_trades'][trade_key] = trade_data
                        await self.save_trade_to_db(trade_key, trade_data)

                        await self.log_to_debug(f"✅ Recovered: {trade_data.get('pair')} {trade_data.get('action')}")
                        recovered_signals += 1

                except Exception as e:
                    logger.error(f"Error scanning group {group_id} for missed signals: {e}")
                    continue

            if recovered_signals > 0:
                await self.log_to_debug(f"📊 Successfully recovered {recovered_signals} missed trading signals!")

        except Exception as e:
            logger.error(f"Error during missed signal recovery: {e}")

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
            "Huge win, TP3 reached. Perfect trade 📊🚀",
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

    async def handle_login_setup(self, client, message: Message):
        """Initiate the Userbot login process via command"""
        user_id = message.from_user.id if message.from_user else BOT_OWNER_USER_ID
        if not await self.is_owner(user_id):
            return
        
        # Ensure we're using the correct message object for replies
        # If it's a callback query message, it's already set
        await self.initiate_login_flow(message)

    async def handle_login_status(self, client, message: Message):
        """Check the status of the Userbot service"""
        if not self.db_pool:
            await message.reply("❌ Database connection not available.")
            return

        try:
            async with self.db_pool.acquire() as conn:
                session = await conn.fetchval(
                    "SELECT setting_value FROM bot_settings WHERE setting_key = 'userbot_session_string'"
                )
            
            if not session:
                await message.reply("🔴 **Userbot Status**: Disconnected (No session found).")
            else:
                await message.reply("🟢 **Userbot Status**: Connected (Session exists in database).")
        except Exception as e:
            await message.reply(f"❌ Error checking status: {str(e)}")

    async def log_to_debug(self, message: str, is_error: bool = False, user_id: Optional[int] = None, failed_message: Optional[str] = None):
        if DEBUG_GROUP_ID:
            try:
                # Standardize professional debug headers
                if is_error:
                    msg_text = f"🚨 **SYSTEM ERROR**\n\n**Issue:** {message}\n\n@fx_pippioneers"
                else:
                    msg_text = f"📊 **SYSTEM LOG**\n\n**Event:** {message}"
                
                # Add user ID button if user_id is provided
                keyboard = None
                if user_id:
                    msg_text += f"\n\n👤 **User ID:** `{user_id}`"
                    
                    # Build the button URL to open user's profile
                    button_url = f"tg://user?id={user_id}"
                    
                    keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton("👤 View User Profile", url=button_url)]
                    ])
                
                await self.app.send_message(DEBUG_GROUP_ID, msg_text, reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Failed to send debug log: {e}")
        log_level = logging.ERROR if is_error else logging.INFO
        logger.log(log_level, message)

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
                min_size=1, ssl=ctx,
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
                    CREATE TABLE IF NOT EXISTS bot_status (
                        id INTEGER PRIMARY KEY DEFAULT 1,
                        last_online TIMESTAMP WITH TIME ZONE,
                        heartbeat_time TIMESTAMP WITH TIME ZONE,
                        CONSTRAINT tg_single_row_constraint UNIQUE (id)
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS bot_settings (
                        setting_key VARCHAR(100) PRIMARY KEY,
                        setting_value TEXT NOT NULL
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
                        manual_tracking_only BOOLEAN DEFAULT FALSE,
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
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS manual_tracking_only BOOLEAN DEFAULT FALSE'
                    )
                    await conn.execute(
                        'ALTER TABLE active_members ADD COLUMN IF NOT EXISTS monday_notification_sent BOOLEAN DEFAULT FALSE'
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

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS emoji_reactions (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        message_id BIGINT NOT NULL,
                        emoji VARCHAR(10) NOT NULL,
                        reaction_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        UNIQUE(user_id, message_id, emoji)
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS free_group_joins (
                        user_id BIGINT PRIMARY KEY,
                        joined_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        discount_sent BOOLEAN DEFAULT FALSE
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS pending_welcome_dms (
                        user_id BIGINT PRIMARY KEY,
                        first_name VARCHAR(255) NOT NULL,
                        message_type VARCHAR(20) NOT NULL,
                        message_content TEXT NOT NULL,
                        failed_attempts INT DEFAULT 0,
                        last_attempt TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS vip_trial_activations (
                        user_id BIGINT PRIMARY KEY,
                        activation_date TIMESTAMP WITH TIME ZONE NOT NULL,
                        expiry_date TIMESTAMP WITH TIME ZONE NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS trial_offer_history (
                        user_id BIGINT PRIMARY KEY,
                        offer_sent_date TIMESTAMP WITH TIME ZONE NOT NULL,
                        accepted BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS peer_id_checks (
                        user_id BIGINT PRIMARY KEY,
                        joined_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        peer_id_established BOOLEAN DEFAULT FALSE,
                        established_at TIMESTAMP WITH TIME ZONE,
                        current_delay_minutes INT DEFAULT 30,
                        current_interval_minutes INT DEFAULT 3,
                        last_check_at TIMESTAMP WITH TIME ZONE,
                        next_check_at TIMESTAMP WITH TIME ZONE,
                        welcome_dm_sent BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS userbot_dm_queue (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        message_type VARCHAR(50) NOT NULL,
                        message_content TEXT NOT NULL,
                        status VARCHAR(20) DEFAULT 'pending',
                        retry_count INTEGER DEFAULT 0,
                        last_attempt TIMESTAMP WITH TIME ZONE,
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
                        'group_id':
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
                        row.get('group_name', ''),
                        'manual_tracking_only':
                        row.get('manual_tracking_only', False)
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
                     status, tp_hits, breakeven_active, entry_type, manual_overrides, channel_message_map, all_channel_ids, group_name, manual_tracking_only)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
                    ON CONFLICT (message_id) DO UPDATE SET
                    status = $18, tp_hits = $19, breakeven_active = $20, manual_overrides = $22, 
                    channel_message_map = $23, all_channel_ids = $24, group_name = $25, manual_tracking_only = $26, last_updated = NOW()
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
                    channel_message_map_str, all_channel_ids_str, group_name,
                    trade_data.get('manual_tracking_only', False))

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
                    created_at = datetime.now(
                        pytz.UTC).astimezone(AMSTERDAM_TZ)

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

    async def restore_trades_from_completed(self, reason_filter: str = "message_deleted"):
        """Restore trades that were incorrectly moved to completed_trades"""
        if not self.db_pool:
            return []

        restored = []
        try:
            async with self.db_pool.acquire() as conn:
                # Get trades marked as deleted
                trades = await conn.fetch(
                    'SELECT * FROM completed_trades WHERE completion_reason = $1',
                    reason_filter)

                for trade in trades:
                    try:
                        # Use .get() with defaults for fields that may not exist in older records
                        channel_message_map = trade.get('channel_message_map', '')
                        all_channel_ids = trade.get('all_channel_ids', '')
                        
                        # Re-insert into active_trades
                        await conn.execute(
                            '''INSERT INTO active_trades (
                                message_id, channel_id, guild_id, pair, action,
                                entry_price, tp1_price, tp2_price, tp3_price, sl_price,
                                telegram_entry, telegram_tp1, telegram_tp2, telegram_tp3,
                                telegram_sl, live_entry, assigned_api, status, tp_hits,
                                breakeven_active, entry_type, manual_overrides, channel_message_map,
                                all_channel_ids, created_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                                    $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)''',
                            trade['message_id'], trade['channel_id'], trade['guild_id'],
                            trade['pair'], trade['action'], trade['entry_price'],
                            trade['tp1_price'], trade['tp2_price'], trade['tp3_price'],
                            trade['sl_price'], trade.get('telegram_entry'), trade.get('telegram_tp1'),
                            trade.get('telegram_tp2'), trade.get('telegram_tp3'), trade.get('telegram_sl'),
                            trade.get('live_entry'), trade.get('assigned_api', 'currencybeacon'), 'active',
                            trade.get('tp_hits', ''), trade.get('breakeven_active', False), trade.get('entry_type'),
                            trade.get('manual_overrides', ''), channel_message_map,
                            all_channel_ids, trade['created_at'])

                        # Remove from completed_trades
                        await conn.execute(
                            'DELETE FROM completed_trades WHERE message_id = $1',
                            trade['message_id'])

                        restored.append(trade['message_id'])
                        # Reload into memory
                        PRICE_TRACKING_CONFIG['active_trades'][trade['message_id']] = {
                            'pair': trade['pair'],
                            'action': trade['action'],
                            'entry': float(trade['entry_price']),
                            'tp1_price': float(trade['tp1_price']),
                            'tp2_price': float(trade['tp2_price']),
                            'tp3_price': float(trade['tp3_price']),
                            'sl_price': float(trade['sl_price']),
                            'group_id': trade['guild_id'],
                            'status': 'active',
                            'tp_hits': trade.get('tp_hits', '').split(',') if trade.get('tp_hits') else []
                        }

                        logger.info(f"✅ Restored trade {trade['message_id']} ({trade['pair']})")
                    except Exception as e:
                        logger.error(f"Error restoring trade {trade['message_id']}: {e}")

        except Exception as e:
            logger.error(f"Error restoring trades: {e}")

        return restored

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
            logger.info(f"Found and processed {offline_hits_found} TP/SL hits that occurred while offline")

    # REMOVED: check_offline_joiners() was redundant.
    # New members are already registered in peer_id_checks via handle_free_group_join()
    # which triggers on every member_update event (including on bot restart when cached events replay)

    async def check_peer_id_established(self, user_id: int) -> bool:
        """Attempt to verify peer ID is established by checking if we can interact with the user"""
        try:
            # Feature 1: Global Resolve - try to fetch user details from Telegram servers
            # This is the most reliable way to get the access hash
            await self.app.get_users([user_id])
            return True
        except Exception:
            # Feature 3: Wait strategy is handled by the escalation loop calling this
            return False

    async def escalate_peer_id_check(self, delay_level: int) -> tuple:
        """Get next delay and interval based on escalation level.
        Level 0: 30 min delay, 3 min interval
        Level 1: 1 hour delay, 10 min interval  
        Level 2: 3 hour delay, 20 min interval
        Level 3+: 24 hour limit, then give up
        """
        escalation = [
            (30, 3),    # Level 0: 30 min delay, check every 3 min
            (60, 10),   # Level 1: 1 hour delay, check every 10 min
            (180, 20),  # Level 2: 3 hours delay, check every 20 min
        ]
        if delay_level < len(escalation):
            return escalation[delay_level]
        return (1440, 20)  # Level 3+: 24 hours (give up after this)

    async def peer_id_escalation_loop(self):
        """Background loop: escalate peer ID checks until established or 24 hours passed"""
        await asyncio.sleep(30)
        
        while self.running:
            try:
                if not self.db_pool:
                    await asyncio.sleep(60)
                    continue
                
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                
                async with self.db_pool.acquire() as conn:
                    # Get all pending peer ID checks
                    pending = await conn.fetch('''
                        SELECT user_id, joined_at, peer_id_established, current_delay_minutes, 
                               current_interval_minutes, next_check_at
                        FROM peer_id_checks 
                        WHERE NOT peer_id_established AND welcome_dm_sent = FALSE
                        ORDER BY next_check_at ASC
                    ''')
                    
                    for row in pending:
                        user_id = row['user_id']
                        joined_at = row['joined_at']
                        if joined_at.tzinfo is None:
                            joined_at = AMSTERDAM_TZ.localize(joined_at)
                        next_check_at = row['next_check_at']
                        if next_check_at.tzinfo is None:
                            next_check_at = AMSTERDAM_TZ.localize(next_check_at)
                        
                        # Check if it's time to do a peer ID check
                        if current_time >= next_check_at:
                            time_elapsed = (current_time - joined_at).total_seconds() / 3600
                            
                            # Give up after 24 hours
                            if time_elapsed > 24:
                                await conn.execute('''
                                    UPDATE peer_id_checks SET peer_id_established = FALSE 
                                    WHERE user_id = $1
                                ''', user_id)
                                logger.warning(f"Peer ID check gave up for user {user_id} after 24 hours")
                                continue
                            
                            # Try peer ID check
                            if await self.check_peer_id_established(user_id):
                                # ... existing success logic ...
                                await conn.execute('''
                                    UPDATE peer_id_checks SET peer_id_established = TRUE, established_at = $1
                                    WHERE user_id = $2
                                ''', current_time, user_id)
                                
                                try:
                                    user_data = await self.app.get_users([user_id])
                                    first_name = user_data[0].first_name if user_data else "Trader"
                                    
                                    # Fix: Use correct nested dictionary keys for Welcome DM
                                    welcome_dm = MESSAGE_TEMPLATES["Welcome & Onboarding"]["Welcome DM (New Free Group Member)"]["message"]
                                    # Optional: replace {user_name} if template uses it
                                    welcome_dm = welcome_dm.replace("{user_name}", first_name)
                                    
                                    await self.app.send_message(user_id, welcome_dm)
                                    await conn.execute('UPDATE peer_id_checks SET welcome_dm_sent = TRUE WHERE user_id = $1', user_id)
                                    await self.log_to_debug(f"✅ Welcome DM successfully sent to {first_name} (ID: {user_id}) - Peer ID established after {time_elapsed:.1f} hours", user_id=user_id)
                                except Exception as e:
                                    error_msg = str(e)
                                    # Handle errors gracefully without resetting established status
                                    await self.log_to_debug(f"❌ Peer ID established for user {user_id} but welcome DM failed: {e}", is_error=True, user_id=user_id)
                            else:
                                # Still not established - schedule next check based on delay progression
                                delay_mins = row['current_delay_minutes']
                                interval_mins = row['current_interval_minutes']
                                
                                # LOG FAILURE AT EACH INTERVAL
                                await self.log_to_debug(f"⏳ Peer ID check failed for user {user_id} (Joined {time_elapsed:.1f}h ago). Next check in {interval_mins}m.", user_id=user_id)
                                
                                # Calculate time since join
                                mins_since_join = (current_time - joined_at).total_seconds() / 60
                                
                                # Escalate if we've passed current delay threshold
                                if mins_since_join >= delay_mins:
                                    # Move to next escalation level
                                    next_delay, next_interval = await self.escalate_peer_id_check(
                                        [30, 60, 180].index(delay_mins) + 1 if delay_mins in [30, 60, 180] else 3
                                    )
                                    next_check = current_time + timedelta(minutes=next_interval)
                                    
                                    await conn.execute('''
                                        UPDATE peer_id_checks 
                                        SET current_delay_minutes = $1, current_interval_minutes = $2, next_check_at = $3
                                        WHERE user_id = $4
                                    ''', next_delay, next_interval, next_check, user_id)
                                    
                                    if next_delay == 1440:
                                        logger.warning(f"Peer ID check for user {user_id} escalated to 24-hour cycle (final attempt)")
                                else:
                                    # Same delay level, schedule next interval check
                                    next_check = current_time + timedelta(minutes=interval_mins)
                                    await conn.execute('''
                                        UPDATE peer_id_checks SET next_check_at = $1 WHERE user_id = $2
                                    ''', next_check, user_id)
                
                await asyncio.sleep(10)  # Check every 10 seconds for due checks
                
            except Exception as e:
                logger.error(f"Error in peer_id_escalation_loop: {e}")
                await asyncio.sleep(60)

    async def check_offline_preexpiration_warnings(self):
        """Recover and send missed 24h/3h pre-expiration warnings"""
        current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
        recovered_warnings = 0
        warning_statuses = []
        
        try:
            for member_id, data in list(AUTO_ROLE_CONFIG['active_members'].items()):
                expiry_time = datetime.fromisoformat(data['expiry_time'])
                if expiry_time.tzinfo is None:
                    expiry_time = AMSTERDAM_TZ.localize(expiry_time)
                
                # Initialize flags if missing
                if 'warning_24h_sent' not in data:
                    data['warning_24h_sent'] = False
                if 'warning_3h_sent' not in data:
                    data['warning_3h_sent'] = False
                
                time_until_expiry = expiry_time - current_time
                hours_left = time_until_expiry.total_seconds() / 3600
                
                # Check for missed 24-hour warning
                if not data['warning_24h_sent'] and hours_left <= 24 and hours_left > 0:
                    await self.send_24hr_warning(member_id)
                    data['warning_24h_sent'] = True
                    recovered_warnings += 1
                    await self.save_auto_role_config()
                    warning_statuses.append(f"24h warning: {member_id}")
                elif data['warning_24h_sent'] and hours_left <= 24 and hours_left > 0:
                    warning_statuses.append(f"24h already sent: {member_id}")
                
                # Check for missed 3-hour warning
                if not data['warning_3h_sent'] and hours_left <= 3 and hours_left > 0:
                    await self.send_3hr_warning(member_id)
                    data['warning_3h_sent'] = True
                    recovered_warnings += 1
                    await self.save_auto_role_config()
                    warning_statuses.append(f"3h warning: {member_id}")
                elif data['warning_3h_sent'] and hours_left <= 3 and hours_left > 0:
                    warning_statuses.append(f"3h already sent: {member_id}")
            
            if recovered_warnings > 0:
                status_msg = " | ".join(warning_statuses)
                await self.log_to_debug(f"✅ Recovered {recovered_warnings} missed pre-expiration warnings: {status_msg}")
            elif warning_statuses:
                await self.log_to_debug(f"ℹ️ All {len(AUTO_ROLE_CONFIG['active_members'])} active members checked - all warnings already sent")
        
        except Exception as e:
            logger.error(f"Error checking offline preexpiration warnings: {e}")
            await self.log_to_debug(f"Error recovering preexpiration warnings: {e}", is_error=True)

    async def check_offline_followup_dms(self):
        """Recover and send missed 3/7/14-day follow-up DMs"""
        current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
        recovered_dms = 0
        
        try:
            for member_id, data in list(AUTO_ROLE_CONFIG['dm_schedule'].items()):
                role_expired = datetime.fromisoformat(data['role_expired'])
                if role_expired.tzinfo is None:
                    role_expired = AMSTERDAM_TZ.localize(role_expired)
                
                # Skip if user is VIP now
                try:
                    is_vip = await self.check_vip_membership(int(member_id))
                    if is_vip:
                        continue
                except:
                    pass
                
                dm_3_time = role_expired + timedelta(days=3)
                dm_7_time = role_expired + timedelta(days=7)
                dm_14_time = role_expired + timedelta(days=14)
                
                # Check for missed 3-day DM
                if not data.get('dm_3_sent', False) and current_time >= dm_3_time:
                    await self.send_followup_dm(member_id, 3)
                    AUTO_ROLE_CONFIG['dm_schedule'][member_id]['dm_3_sent'] = True
                    recovered_dms += 1
                
                # Check for missed 7-day DM
                if not data.get('dm_7_sent', False) and current_time >= dm_7_time:
                    await self.send_followup_dm(member_id, 7)
                    AUTO_ROLE_CONFIG['dm_schedule'][member_id]['dm_7_sent'] = True
                    recovered_dms += 1
                
                # Check for missed 14-day DM
                if not data.get('dm_14_sent', False) and current_time >= dm_14_time:
                    await self.send_followup_dm(member_id, 14)
                    AUTO_ROLE_CONFIG['dm_schedule'][member_id]['dm_14_sent'] = True
                    recovered_dms += 1
            
            if recovered_dms > 0:
                await self.log_to_debug(f"✅ Recovered {recovered_dms} missed follow-up DMs")
        
        except Exception as e:
            logger.error(f"Error checking offline followup DMs: {e}")
            await self.log_to_debug(f"Error recovering followup DMs: {e}", is_error=True)

    async def validate_and_fix_trial_expiry_times(self):
        """Check all existing trial members and fix their expiry times if incorrect"""
        fixed_count = 0
        verified_count = 0
        
        try:
            for member_id, data in list(AUTO_ROLE_CONFIG['active_members'].items()):
                join_time_str = data.get('joined_at')
                expiry_time_str = data.get('expiry_time')
                
                if not join_time_str or not expiry_time_str:
                    continue
                
                # Parse join and expiry times
                join_time = datetime.fromisoformat(join_time_str)
                if join_time.tzinfo is None:
                    join_time = AMSTERDAM_TZ.localize(join_time)
                
                expiry_time = datetime.fromisoformat(expiry_time_str)
                if expiry_time.tzinfo is None:
                    expiry_time = AMSTERDAM_TZ.localize(expiry_time)
                
                # Recalculate what the correct expiry time should be
                correct_expiry = self.calculate_trial_expiry_time(join_time)
                
                # Compare (allowing small time differences due to timezone handling)
                time_diff = abs((correct_expiry - expiry_time).total_seconds())
                
                if time_diff > 60:  # More than 1 minute difference = needs fixing
                    # Update the expiry time
                    data['expiry_time'] = correct_expiry.isoformat()
                    fixed_count += 1
                    logger.info(f"Fixed trial expiry for user {member_id}: was {expiry_time.strftime('%A %H:%M')}, now {correct_expiry.strftime('%A %H:%M')}")
                else:
                    verified_count += 1
            
            # Save corrected config
            if fixed_count > 0:
                await self.save_auto_role_config()
            
            # Log results
            if fixed_count > 0 or verified_count > 0:
                message = f"✅ Trial expiry validation complete: {verified_count} verified, {fixed_count} corrected"
                logger.info(message)
                await self.log_to_debug(message)
        
        except Exception as e:
            logger.error(f"Error validating trial expiry times: {e}")
            await self.log_to_debug(f"Error validating trial expiry times: {e}", is_error=True)

    async def check_offline_engagement_discounts(self):
        """Recover and send missed engagement discount DMs"""
        if not self.db_pool:
            return
        
        current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
        recovered_discounts = 0
        
        try:
            async with self.db_pool.acquire() as conn:
                # Get users who should have received discount but didn't
                joins = await conn.fetch(
                    'SELECT user_id, joined_at, discount_sent FROM free_group_joins WHERE discount_sent = FALSE'
                )
            
            for join_row in joins:
                user_id = join_row['user_id']
                joined_at = join_row['joined_at']
                
                two_weeks_ago = joined_at + timedelta(days=14)
                if current_time < two_weeks_ago:
                    continue
                
                # Count reactions after 2-week mark
                async with self.db_pool.acquire() as conn:
                    reaction_count = await conn.fetchval(
                        '''SELECT COUNT(DISTINCT message_id) FROM emoji_reactions
                           WHERE user_id = $1 AND reaction_time > $2''',
                        user_id, two_weeks_ago
                    )
                
                # If qualified, send discount DM
                if reaction_count >= 5:
                    await self.send_engagement_discount_dm(user_id)
                    
                    async with self.db_pool.acquire() as conn:
                        await conn.execute(
                            'UPDATE free_group_joins SET discount_sent = TRUE WHERE user_id = $1',
                            user_id
                        )
                    
                    recovered_discounts += 1
            
            if recovered_discounts > 0:
                await self.log_to_debug(f"✅ Recovered {recovered_discounts} missed engagement discount DMs")
        
        except Exception as e:
            logger.error(f"Error checking offline engagement discounts: {e}")
            await self.log_to_debug(f"Error recovering engagement discounts: {e}", is_error=True)

    async def price_tracking_loop(self):
        await asyncio.sleep(10)

        while self.running:
            try:
                # Record when this price check cycle started
                PRICE_TRACKING_CONFIG['last_price_check_time'] = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                
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
                await self.log_to_debug(f"Error in trial expiry loop: {e}", is_error=True)

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
                # Queuing the message for userbot instead of sending directly
                expiry_msg = MESSAGE_TEMPLATES["Trial Status & Expiry"]["Trial Expired"]["message"]
                if self.db_pool:
                    async with self.db_pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO userbot_dm_queue (user_id, message, label) VALUES ($1, $2, $3)",
                            int(member_id), expiry_msg, "Trial Expired"
                        )
                else:
                    await self.app.send_message(int(member_id), expiry_msg)
            except Exception as e:
                logger.error(f"Could not queue/send expiry DM to {member_id}: {e}")

            await self.save_auto_role_config()
            logger.info(f"Trial expired for user {member_id}")

        except Exception as e:
            logger.error(f"Error expiring trial for {member_id}: {e}")

    async def preexpiration_warning_loop(self):
        await asyncio.sleep(60)

        while self.running:
            try:
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)

                for member_id, data in list(
                        AUTO_ROLE_CONFIG['active_members'].items()):
                    expiry_time = datetime.fromisoformat(data['expiry_time'])
                    if expiry_time.tzinfo is None:
                        expiry_time = AMSTERDAM_TZ.localize(expiry_time)

                    time_until_expiry = expiry_time - current_time
                    hours_left = time_until_expiry.total_seconds() / 3600

                    # Initialize warning tracking if not present
                    if 'warning_24h_sent' not in data:
                        data['warning_24h_sent'] = False
                    if 'warning_3h_sent' not in data:
                        data['warning_3h_sent'] = False

                    # Send 24-hour warning
                    if not data['warning_24h_sent'] and 23 < hours_left <= 24:
                        await self.send_24hr_warning(member_id)
                        data['warning_24h_sent'] = True
                        await self.save_auto_role_config()

                    # Send 3-hour warning
                    if not data['warning_3h_sent'] and 2.9 < hours_left <= 3:
                        await self.send_3hr_warning(member_id)
                        data['warning_3h_sent'] = True
                        await self.save_auto_role_config()

            except Exception as e:
                await self.log_to_debug(f"Error in preexpiration warning loop: {e}", is_error=True)

            await asyncio.sleep(600)

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
                await self.log_to_debug(f"Error in followup DM loop: {e}", is_error=True)

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

    async def handle_welcome_dm_fallback(self, user_id: int, first_name: str, msg_type: str, message_content: str):
        """Final safety: All DMs MUST go to queue, never sent from here"""
        if self.db_pool:
            async with self.db_pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO userbot_dm_queue (user_id, message, label)
                    VALUES ($1, $2, $3)
                ''', user_id, message_content, msg_type)
            logger.info(f"✅ Safe-routed DM for {user_id} to Userbot queue")
        else:
            logger.error(f"❌ Cannot send DM to {user_id}: No DB pool and DMs disabled in main bot")

    async def _get_trial_hours_remaining(self, user_id: int, conn) -> Optional[float]:
        """Get the number of hours remaining in a user's trial"""
        try:
            # Check in-memory config first
            user_id_str = str(user_id)
            if user_id_str in AUTO_ROLE_CONFIG['active_members']:
                expiry_time_str = AUTO_ROLE_CONFIG['active_members'][user_id_str].get('expiry_time')
                if expiry_time_str:
                    expiry_time = datetime.fromisoformat(expiry_time_str)
                    if expiry_time.tzinfo is None:
                        expiry_time = AMSTERDAM_TZ.localize(expiry_time)
                    current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                    hours_left = (expiry_time - current_time).total_seconds() / 3600
                    return hours_left
            
            # Fallback to database
            row = await conn.fetchrow(
                "SELECT expiry_time FROM vip_trial_activations WHERE member_id = $1 AND is_active = TRUE ORDER BY activated_at DESC LIMIT 1",
                user_id
            )
            if row:
                expiry_time = row['expiry_time']
                if expiry_time.tzinfo is None:
                    expiry_time = AMSTERDAM_TZ.localize(expiry_time)
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                hours_left = (expiry_time - current_time).total_seconds() / 3600
                return hours_left
            
            return None
        except Exception as e:
            logger.debug(f"Error getting trial hours remaining for {user_id}: {e}")
            return None

    async def register_bot_commands(self):
        try:
            # All commands are owner-only
            if BOT_OWNER_USER_ID:
                owner_commands = [
                    BotCommand("entry", "Create trading signal (menu)"),
                    BotCommand("activetrades", "View active trading signals"),
                    BotCommand("tradeoverride", "Override trade status (menu)"),
                    BotCommand("pricetest", "Test live price for a pair"),
                    BotCommand("login", "Userbot login/setup (setup|status)"),
                    BotCommand("newmemberslist", "Track new members and trial status"),
                    BotCommand("dmmessages", "Preview DM message templates"),
                    BotCommand("peeridstatus", "Check if peer ID is connected for a user"),
                    BotCommand("dbstatus", "Database health check"),
                    BotCommand("dmstatus", "DM statistics"),
                ]
                try:
                    await self.app.set_bot_commands(
                        owner_commands,
                        scope=BotCommandScopeChat(chat_id=BOT_OWNER_USER_ID))
                    logger.info(
                        f"✅ All bot commands registered as owner-only for user {BOT_OWNER_USER_ID} ({len(owner_commands)} commands)")
                except Exception as scope_error:
                    logger.error(f"❌ Error setting owner-scoped commands: {scope_error}")
            else:
                logger.warning("⚠️  BOT_OWNER_USER_ID not set, cannot register owner-specific commands")
        except Exception as e:
            logger.error(f"❌ Error registering bot commands: {e}")

    async def ensure_active_trial_peers(self):
        """Placeholder for peer discovery logic to fix attribute error"""
        logger.info("Starting active trial peer discovery...")
        pass

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
                    "**Bot Started!** Signal engine is online. DMs delegated to Userbot.")
            except Exception as e:
                logger.error(f"Could not send startup message: {e}")

        # Restore any trades that were incorrectly marked as deleted
        restored_trades = await self.restore_trades_from_completed("message_deleted")
        if restored_trades:
            logger.info(f"✅ Restored {len(restored_trades)} trades that were incorrectly marked as deleted")
            try:
                if DEBUG_GROUP_ID:
                    await self.app.send_message(
                        DEBUG_GROUP_ID,
                        f"✅ **Recovery Complete:** Restored {len(restored_trades)} trades:\n" +
                        "\n".join([f"- Message ID: {mid}" for mid in restored_trades[:5]]) +
                        (f"\n... and {len(restored_trades) - 5} more" if len(restored_trades) > 5 else ""))
            except Exception as e:
                logger.error(f"Could not send recovery message: {e}")

        # Peer escalation for discovery ONLY (helps userbot find users)
        try:
            await self.ensure_active_trial_peers()
        except Exception as e:
            logger.error(f"Error in startup discovery: {e}")

        self.startup_complete = True

        # ONLY Signal Engine loops remain
        asyncio.create_task(self.price_tracking_loop())
        asyncio.create_task(self.peer_id_escalation_loop()) 
        
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
