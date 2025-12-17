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
VIP_TRIAL_INVITE_LINK = os.getenv("VIP_TRIAL_INVITE_LINK", "")
WHOP_PURCHASE_LINK = os.getenv("WHOP_PURCHASE_LINK",
                               "https://whop.com/gold-pioneer/")

if TELEGRAM_BOT_TOKEN:
    print(f"Telegram bot token loaded")
if TELEGRAM_API_ID:
    print(f"Telegram API ID loaded")
if TELEGRAM_API_HASH:
    print(f"Telegram API Hash loaded")
if BOT_OWNER_USER_ID:
    print(f"Bot owner ID loaded: {BOT_OWNER_USER_ID}")
if VIP_TRIAL_INVITE_LINK:
    print(f"VIP Trial invite link configured")

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
    "dm_schedule": {}
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

        self.app = Client(
            "trading_bot",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            bot_token=TELEGRAM_BOT_TOKEN
        )
        self.db_pool = None
        self.client_session = None
        self.last_online_time = None
        self.running = True
        self.awaiting_price_input = {}
        self.awaiting_custom_pair = {}

        self._register_handlers()

    def _register_handlers(self):

        @self.app.on_message(filters.command("start") & filters.private)
        async def start_command(client, message: Message):
            await self.handle_start(client, message)

        @self.app.on_message(filters.command("help") & filters.private)
        async def help_command(client, message: Message):
            await self.handle_help(client, message)

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

        @self.app.on_message(filters.command("timedautorole"))
        async def timed_auto_role_command(client, message: Message):
            await self.handle_timed_auto_role(client, message)

        @self.app.on_message(filters.command("getid"))
        async def get_id_command(client, message: Message):
            await self.handle_get_id(client, message)

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

        @self.app.on_message(
            filters.private & filters.text & ~filters.command([
                "start", "help", "entry", "activetrades", "tradeoverride",
                "pricetest", "dbstatus", "dmstatus", "timedautorole", "getid"
            ]))
        async def text_input_handler(client, message: Message):
            await self.handle_text_input(client, message)

    async def is_owner(self, user_id: int) -> bool:
        return user_id == BOT_OWNER_USER_ID

    async def log_to_debug(self, message: str):
        if DEBUG_GROUP_ID:
            try:
                await self.app.get_chat(DEBUG_GROUP_ID)
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

    async def handle_start(self, client: Client, message: Message):
        user = message.from_user
        welcome_text = (
            f"**Welcome to FX Pip Pioneers Trading Bot!**\n\n"
            f"Hello {user.first_name}! I'm your trading signals assistant.\n\n"
            f"**Available Commands:**\n"
            f"/help - Show all commands\n"
            f"/activetrades - View active trading signals\n"
            f"/pricetest <pair> - Test live price for a pair\n\n"
            f"**Join our community:**\n"
            f"Free Group: {FREE_GROUP_LINK}\n"
            f"VIP Group: {WHOP_PURCHASE_LINK}")
        await message.reply(welcome_text)

    async def handle_help(self, client: Client, message: Message):
        is_owner = await self.is_owner(message.from_user.id)

        help_text = (
            "**FX Pip Pioneers Bot Commands**\n\n"
            "**Public Commands:**\n"
            "/start - Welcome message\n"
            "/help - Show this help\n"
            "/activetrades - View all active trading signals\n"
            "/pricetest <pair> - Test live price (e.g., /pricetest EURUSD)\n"
            "/getid - Get chat/user ID\n")

        if is_owner:
            help_text += (
                "\n**Owner Commands:**\n"
                "/entry - Create trading signal (interactive menu)\n"
                "/tradeoverride <message_id> <status> - Override trade status\n"
                "  Statuses: tp1, tp2, tp3, sl\n"
                "/timedautorole <action> - Manage trial system\n"
                "  Actions: status, list\n"
                "/dbstatus - Database health check\n"
                "/dmstatus - DM delivery statistics\n")

        await message.reply(help_text)

    async def handle_get_id(self, client: Client, message: Message):
        chat = message.chat
        user = message.from_user

        response = f"**ID Information:**\n\n"
        response += f"**Your User ID:** `{user.id}`\n"
        response += f"**Your Username:** @{user.username or 'N/A'}\n\n"

        if chat.type != ChatType.PRIVATE:
            response += f"**Chat ID:** `{chat.id}`\n"
            response += f"**Chat Title:** {chat.title}\n"
            response += f"**Chat Type:** {chat.type.name}\n"

        await message.reply(response)

    async def handle_entry(self, client: Client, message: Message):
        if not await self.is_owner(message.from_user.id):
            await message.reply("This command is restricted to the bot owner.")
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

            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("XAUUSD",
                                         callback_data="entry_pair_XAUUSD"),
                    InlineKeyboardButton("BTCUSD",
                                         callback_data="entry_pair_BTCUSD"),
                ],
                [
                    InlineKeyboardButton("EURUSD",
                                         callback_data="entry_pair_EURUSD"),
                    InlineKeyboardButton("GBPUSD",
                                         callback_data="entry_pair_GBPUSD"),
                ],
                [
                    InlineKeyboardButton("GBPJPY",
                                         callback_data="entry_pair_GBPJPY"),
                    InlineKeyboardButton("USDJPY",
                                         callback_data="entry_pair_USDJPY"),
                ],
                [
                    InlineKeyboardButton("US100",
                                         callback_data="entry_pair_US100"),
                    InlineKeyboardButton("GER40",
                                         callback_data="entry_pair_GER40"),
                ],
                [
                    InlineKeyboardButton("Custom Pair...",
                                         callback_data="entry_pair_custom")
                ],
                [InlineKeyboardButton("Cancel", callback_data="entry_cancel")]
            ])

            await callback_query.message.edit_text(
                f"**Create Trading Signal**\n\n"
                f"Action: **{entry_data['action']}**\n"
                f"Type: **{entry_type.upper()}**\n\n"
                f"Step 3: Select trading pair:",
                reply_markup=keyboard)

        elif data.startswith("entry_pair_"):
            pair = data.replace("entry_pair_", "")

            if pair == "custom":
                self.awaiting_custom_pair[user_id] = callback_query.message.id
                await callback_query.message.edit_text(
                    f"**Create Trading Signal**\n\n"
                    f"Action: **{entry_data['action']}**\n"
                    f"Type: **{entry_data['entry_type'].upper()}**\n\n"
                    f"Please type the trading pair (e.g., AUDUSD, NZDJPY):")
                return

            entry_data['pair'] = pair

            if entry_data['entry_type'] == 'limit':
                self.awaiting_price_input[user_id] = callback_query.message.id
                await callback_query.message.edit_text(
                    f"**Create Trading Signal**\n\n"
                    f"Action: **{entry_data['action']}**\n"
                    f"Type: **{entry_data['entry_type'].upper()}**\n"
                    f"Pair: **{pair}**\n\n"
                    f"Please type the entry price:")
            else:
                await self.show_group_selection(callback_query, entry_data)

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
            elif group_choice == "debug":
                entry_data['groups'] = [DEBUG_GROUP_ID
                                        ] if DEBUG_GROUP_ID else []
                entry_data['track_price'] = False

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
                 InlineKeyboardButton("Debug Group (no tracking)",
                                      callback_data="entry_group_debug")
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
        if DEBUG_GROUP_ID in entry_data['groups']:
            group_names.append("Debug Group")
        groups_text = ", ".join(group_names) if group_names else "None"

        price_text = f"Price: **{entry_data['price']}**\n" if entry_data[
            'price'] else "Price: **Live (auto-fetch)**\n"
        tracking_text = "Price Tracking: **Enabled**" if entry_data.get(
            'track_price',
            True) else "Price Tracking: **Disabled** (debug mode)"

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
            f"Action: **{entry_data['action']}**\n"
            f"Type: **{entry_data['entry_type'].upper()}**\n"
            f"Pair: **{entry_data['pair']}**\n"
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
        pair_name = PAIR_CONFIG.get(pair, {}).get('name', pair)

        signal_text = (
            f"**Trade Signal For: {pair_name}**\n\n"
            f"**Type:** {entry_type.upper()}\n"
            f"**Action:** {action}\n\n"
            f"**Entry:** {entry_price:.5f}\n\n"
            f"**Take Profit Levels:**\n"
            f"TP1: {levels['tp1']:.5f} (+20 pips)\n"
            f"TP2: {levels['tp2']:.5f} (+40 pips)\n"
            f"TP3: {levels['tp3']:.5f} (+70 pips)\n\n"
            f"**Stop Loss:** {levels['sl']:.5f} (-50 pips)\n\n"
            f"**Risk Management:** Never risk more than 1-2% per trade")

        if not signal_channels:
            signal_channels = [callback_query.message.chat.id]

        primary_message = None
        sent_count = 0
        channel_message_map = {}

        for channel_id in signal_channels:
            try:
                sent_msg = await self.app.send_message(channel_id, signal_text)
                sent_count += 1
                channel_message_map[str(channel_id)] = sent_msg.id

                if primary_message is None:
                    primary_message = sent_msg
            except Exception as e:
                logger.error(
                    f"Failed to send signal to channel {channel_id}: {e}")

        track_price = entry_data.get('track_price', True)

        if pair in EXCLUDED_FROM_TRACKING:
            track_price = False

        if primary_message and track_price:
            trade_data = {
                'message_id': str(primary_message.id),
                'chat_id': primary_message.chat.id,
                'pair': pair,
                'action': action,
                'entry_type': entry_type,
                'entry_price': float(entry_price),
                'tp1_price': float(levels['tp1']),
                'tp2_price': float(levels['tp2']),
                'tp3_price': float(levels['tp3']),
                'sl_price': float(levels['sl']),
                'status': 'active',
                'tp_hits': [],
                'breakeven_active': False,
                'created_at': datetime.now(AMSTERDAM_TZ).isoformat(),
                'all_channel_ids': signal_channels,
                'channel_message_map': channel_message_map
            }

            PRICE_TRACKING_CONFIG['active_trades'][str(
                primary_message.id)] = trade_data
            await self.save_trade_to_db(str(primary_message.id), trade_data)
            asyncio.create_task(
                self.check_single_trade_immediately(str(primary_message.id),
                                                    trade_data))

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
            f"Entry: **{entry_price:.5f}**\n\n"
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

        if user_id in self.awaiting_custom_pair:
            pair = message.text.upper().strip()
            if user_id in PENDING_ENTRIES:
                entry_data = PENDING_ENTRIES[user_id]
                entry_data['pair'] = pair

                msg_id = self.awaiting_custom_pair.pop(user_id)

                if entry_data['entry_type'] == 'limit':
                    self.awaiting_price_input[user_id] = True
                    await message.reply(
                        f"Pair set to **{pair}**.\n\nNow please type the entry price:"
                    )
                else:
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
                                 "Debug Group (no tracking)",
                                 callback_data="entry_group_debug")
                         ],
                         [
                             InlineKeyboardButton("Cancel",
                                                  callback_data="entry_cancel")
                         ]])
                    await message.reply(
                        f"**Create Trading Signal**\n\n"
                        f"Action: **{entry_data['action']}**\n"
                        f"Type: **{entry_data['entry_type'].upper()}**\n"
                        f"Pair: **{pair}**\n\n"
                        f"Step 4: Select where to send:",
                        reply_markup=keyboard)
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
                                 "Debug Group (no tracking)",
                                 callback_data="entry_group_debug")
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
        trades = PRICE_TRACKING_CONFIG['active_trades']

        if not trades:
            await message.reply(
                "**No Active Trades**\n\nThere are currently no active trading signals being tracked."
            )
            return

        response = f"**Active Trades ({len(trades)})**\n\n"

        for msg_id, trade in list(trades.items())[:10]:
            pair = trade.get('pair', 'Unknown')
            action = trade.get('action', 'Unknown')
            entry = trade.get('entry_price', 0)
            status = trade.get('status', 'active')
            tp_hits = trade.get('tp_hits', [])

            live_price = await self.get_live_price(pair)
            price_str = f"{live_price:.5f}" if live_price else "N/A"

            tp_status = ", ".join(tp_hits) if tp_hits else "None"

            response += (f"**{pair}** - {action}\n"
                         f"Entry: {entry:.5f} | Live: {price_str}\n"
                         f"TP Hits: {tp_status} | Status: {status}\n\n")

        if len(trades) > 10:
            response += f"_...and {len(trades) - 10} more trades_"

        await message.reply(response)

    async def handle_trade_override(self, client: Client, message: Message):
        if not await self.is_owner(message.from_user.id):
            await message.reply("This command is restricted to the bot owner.")
            return

        args = message.text.split()[1:] if len(
            message.text.split()) > 1 else []

        if len(args) < 2:
            await message.reply(
                "**Usage:** /tradeoverride <message_id> <status>\n\n"
                "**Statuses:** tp1, tp2, tp3, sl\n\n"
                "**Example:** /tradeoverride 12345 tp1")
            return

        message_id = args[0]
        new_status = args[1].lower()

        if new_status not in ['tp1', 'tp2', 'tp3', 'sl']:
            await message.reply("Invalid status. Use: tp1, tp2, tp3, or sl")
            return

        trade = PRICE_TRACKING_CONFIG['active_trades'].get(message_id)
        if not trade:
            await message.reply(
                f"Trade with message ID {message_id} not found.")
            return

        pair = trade.get('pair', 'Unknown')
        action = trade.get('action', 'Unknown')

        if new_status == 'sl':
            trade['status'] = 'sl_hit'
            await self.send_sl_notification(message_id, trade,
                                            trade.get('sl_price', 0))
            del PRICE_TRACKING_CONFIG['active_trades'][message_id]
            await self.remove_trade_from_db(message_id, 'manual_sl')
        else:
            tp_level = new_status.upper()
            if tp_level not in trade.get('tp_hits', []):
                trade['tp_hits'] = trade.get('tp_hits', []) + [tp_level]

            tp_price = trade.get(f'{new_status}_price', 0)
            await self.send_tp_notification(message_id, trade, tp_level,
                                            tp_price)

            if tp_level == 'TP3':
                trade['status'] = 'completed'
                del PRICE_TRACKING_CONFIG['active_trades'][message_id]
                await self.remove_trade_from_db(message_id, 'manual_tp3')
            else:
                await self.update_trade_in_db(message_id, trade)

        await message.reply(
            f"Trade {message_id} ({pair} {action}) marked as **{new_status.upper()}** hit."
        )
        await self.log_to_debug(
            f"Manual override: {pair} {action} -> {new_status.upper()}")

    async def handle_price_test(self, client: Client, message: Message):
        args = message.text.split()[1:] if len(
            message.text.split()) > 1 else []

        if not args:
            await message.reply(
                "**Usage:** /pricetest <pair>\n\nExample: /pricetest EURUSD")
            return

        pair = args[0].upper()

        await message.reply(f"Fetching live price for **{pair}**...")

        price = await self.get_live_price(pair)

        if price:
            pair_name = PAIR_CONFIG.get(pair, {}).get('name', pair)
            await message.reply(
                f"**Price Test: {pair_name}**\n\nLive Price: **{price:.5f}**")
        else:
            await message.reply(
                f"Could not retrieve price for **{pair}**. The pair may not be supported or APIs are unavailable."
            )

    async def handle_db_status(self, client: Client, message: Message):
        if not await self.is_owner(message.from_user.id):
            await message.reply("This command is restricted to the bot owner.")
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
            await message.reply("This command is restricted to the bot owner.")
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
            await message.reply("This command is restricted to the bot owner.")
            return

        args = message.text.split()[1:] if len(
            message.text.split()) > 1 else []
        action = args[0].lower() if args else "status"

        if action == "status":
            active_count = len(AUTO_ROLE_CONFIG['active_members'])
            history_count = len(AUTO_ROLE_CONFIG['role_history'])

            status = (
                f"**Trial System Status**\n\n"
                f"**Status:** {'Enabled' if AUTO_ROLE_CONFIG['enabled'] else 'Disabled'}\n"
                f"**Duration:** {AUTO_ROLE_CONFIG['duration_hours']} hours (3 days)\n"
                f"**Weekend Duration:** 120 hours (5 days)\n\n"
                f"**Active Trials:** {active_count}\n"
                f"**Anti-abuse Records:** {history_count}\n")
            await message.reply(status)

        elif action == "list":
            if not AUTO_ROLE_CONFIG['active_members']:
                await message.reply("No active trial members.")
                return

            response = "**Active Trial Members**\n\n"
            current_time = datetime.now(AMSTERDAM_TZ)

            for member_id, data in list(
                    AUTO_ROLE_CONFIG['active_members'].items())[:20]:
                expiry = datetime.fromisoformat(
                    data.get('expiry_time', current_time.isoformat()))
                if expiry.tzinfo is None:
                    expiry = AMSTERDAM_TZ.localize(expiry)

                time_left = expiry - current_time
                hours_left = max(0, time_left.total_seconds() / 3600)

                weekend = " (weekend)" if data.get('weekend_delayed') else ""
                response += f"User {member_id}: {hours_left:.1f}h left{weekend}\n"

            if len(AUTO_ROLE_CONFIG['active_members']) > 20:
                response += f"\n_...and {len(AUTO_ROLE_CONFIG['active_members']) - 20} more_"

            await message.reply(response)
        else:
            await message.reply("**Usage:** /timedautorole <status|list>")

    async def process_join_request(self, client: Client,
                                   join_request: ChatJoinRequest):
        if join_request.chat.id != VIP_GROUP_ID:
            return

        try:
            await join_request.approve()
            await self.log_to_debug(
                f"Auto-approved join request from {join_request.from_user.first_name} - member update handler will process trial logic"
            )
        except Exception as e:
            logger.error(f"Error auto-approving join request: {e}")

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
            f"**Welcome to FX Pip Pioneers!**\n\n"
            f"Hey {user.first_name}! Thanks for joining our trading community.\n\n"
            f"**Want to try our VIP Group for FREE?**\n"
            f"We're offering a **3-day free trial** of our VIP Group where you'll receive "
            f"**6+ high-quality signals per day** with automatic TP/SL calculations and live price tracking.\n\n"
            f"**Activate your free trial here:**\n{VIP_TRIAL_INVITE_LINK or VIP_GROUP_LINK}\n\n"
            f"Good luck trading!")

        try:
            await client.send_message(user.id, welcome_dm)
            await self.log_to_debug(
                f"Sent welcome DM to {user.first_name} about VIP trial")
        except Exception as e:
            logger.error(
                f"Could not send welcome DM to {user.first_name}: {e}")

    async def handle_vip_group_join(self, client: Client, user, invite_link):
        user_id_str = str(user.id)
        current_time = datetime.now(AMSTERDAM_TZ)

        used_trial_link = False
        if invite_link and VIP_TRIAL_INVITE_LINK:
            invite_link_str = invite_link.invite_link if hasattr(
                invite_link, 'invite_link') else str(invite_link)
            if VIP_TRIAL_INVITE_LINK in invite_link_str or invite_link_str in VIP_TRIAL_INVITE_LINK:
                used_trial_link = True

        if not used_trial_link:
            await self.log_to_debug(
                f"{user.first_name} (ID: {user.id}) joined VIP via non-trial link - treating as paid member"
            )
            return

        await self.log_to_debug(
            f"{user.first_name} (ID: {user.id}) joined VIP via TRIAL link - processing trial"
        )

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
                f"Database check failed for {user.first_name} - allowing access but logging warning"
            )

        if has_used_trial:
            await self.log_to_debug(
                f"Kicking {user.first_name} - already used trial")

            try:
                await client.send_message(
                    user.id, f"**Trial Already Used**\n\n"
                    f"You have already used your 3-day free trial.\n\n"
                    f"To continue accessing the VIP Group, please subscribe:\n{WHOP_PURCHASE_LINK}"
                )
            except Exception as e:
                logger.error(f"Could not send DM to {user.first_name}: {e}")

            try:
                await client.ban_chat_member(VIP_GROUP_ID, user.id)
                await asyncio.sleep(1)
                await client.unban_chat_member(VIP_GROUP_ID, user.id)
            except Exception as e:
                logger.error(f"Error kicking user {user.first_name}: {e}")
            return

        is_weekend = self.is_weekend_time(current_time)
        duration_hours = 120 if is_weekend else 72
        expiry_time = current_time + timedelta(hours=duration_hours)

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

        if is_weekend:
            welcome_msg = (
                f"**Welcome to FX Pip Pioneers!** As a welcome gift, we've given you free "
                f"**access to our VIP Group for 3 trading days.** Since you joined during the weekend, "
                f"your access will expire in 5 days (120 hours) to account for the 2 weekend days when the markets are closed. "
                f"This way, you get the full 3 trading days of premium access. Good luck trading!"
            )
        else:
            welcome_msg = (
                f"**Welcome to FX Pip Pioneers!** As a welcome gift, we've given you free "
                f"**access to our VIP Group for 3 days.** "
                f"Good luck trading!")

        try:
            await client.send_message(user.id, welcome_msg)
        except Exception as e:
            logger.error(
                f"Could not send welcome DM to {user.first_name}: {e}")

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
        entry_price = trade_data.get('entry_price')
        tp1 = trade_data.get('tp1_price')
        tp2 = trade_data.get('tp2_price')
        tp3 = trade_data.get('tp3_price')
        sl = trade_data.get('sl_price')
        tp_hits = trade_data.get('tp_hits', [])

        live_price = await self.get_live_price(pair)
        if not live_price:
            return

        is_buy = action.upper() == "BUY"

        if is_buy:
            if live_price <= sl:
                await self.handle_sl_hit(message_id, trade_data, live_price)
                return
            if 'TP1' not in tp_hits and live_price >= tp1:
                await self.handle_tp_hit(message_id, trade_data, 'TP1',
                                         live_price)
            if 'TP2' not in tp_hits and live_price >= tp2:
                await self.handle_tp_hit(message_id, trade_data, 'TP2',
                                         live_price)
            if 'TP3' not in tp_hits and live_price >= tp3:
                await self.handle_tp_hit(message_id, trade_data, 'TP3',
                                         live_price)
                return
        else:
            if live_price >= sl:
                await self.handle_sl_hit(message_id, trade_data, live_price)
                return
            if 'TP1' not in tp_hits and live_price <= tp1:
                await self.handle_tp_hit(message_id, trade_data, 'TP1',
                                         live_price)
            if 'TP2' not in tp_hits and live_price <= tp2:
                await self.handle_tp_hit(message_id, trade_data, 'TP2',
                                         live_price)
            if 'TP3' not in tp_hits and live_price <= tp3:
                await self.handle_tp_hit(message_id, trade_data, 'TP3',
                                         live_price)
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
        entry_price = trade.get('entry_price', 0)
        tp_hits = trade.get('tp_hits', [])

        tp_status = f"TPs hit: {', '.join(tp_hits)}" if tp_hits else ""

        notification = (
            f"**BREAKEVEN HIT** {pair} {action}\n\n"
            f"Price returned to entry ({entry_price:.5f})\n"
            f"{tp_status}\n"
            f"Trade closed at breakeven.")

        channel_message_map = trade.get('channel_message_map', {})
        all_channels = trade.get('all_channel_ids', [])

        if not all_channels:
            chat_id = trade.get('chat_id')
            if chat_id:
                all_channels = [chat_id]

        for channel_id in all_channels:
            try:
                reply_msg_id = channel_message_map.get(str(channel_id))
                if reply_msg_id:
                    await self.app.send_message(
                        channel_id,
                        notification,
                        reply_to_message_id=reply_msg_id)
                else:
                    await self.app.send_message(channel_id, notification)
            except Exception as e:
                logger.error(f"Failed to send breakeven notification to {channel_id}: {e}")

        del PRICE_TRACKING_CONFIG['active_trades'][message_id]
        await self.remove_trade_from_db(message_id, 'breakeven_hit')

        await self.log_to_debug(
            f"{trade['pair']} {trade['action']} hit breakeven @ {entry_price:.5f}")

    async def send_tp_notification(self, message_id: str, trade_data: dict,
                                   tp_level: str, hit_price: float):
        pair = trade_data.get('pair', 'Unknown')
        action = trade_data.get('action', 'Unknown')

        messages = [
            f"**{tp_level} HIT!** {pair} {action}\n\nPrice reached: {hit_price:.5f}",
            f"**{tp_level} Target Reached!** {pair} {action}\n\nHit price: {hit_price:.5f}",
            f"**{pair} {action} - {tp_level} SECURED!**\n\nPrice: {hit_price:.5f}"
        ]

        notification = random.choice(messages)

        channel_message_map = trade_data.get('channel_message_map', {})
        all_channels = trade_data.get('all_channel_ids', [])

        if not all_channels:
            chat_id = trade_data.get('chat_id')
            if chat_id:
                all_channels = [chat_id]

        for channel_id in all_channels:
            try:
                reply_msg_id = channel_message_map.get(str(channel_id))
                if reply_msg_id:
                    await self.app.send_message(
                        channel_id,
                        notification,
                        reply_to_message_id=reply_msg_id)
                else:
                    await self.app.send_message(channel_id, notification)
            except Exception as e:
                logger.error(
                    f"Failed to send TP notification to {channel_id}: {e}")
                try:
                    await self.app.send_message(channel_id, notification)
                except Exception as e2:
                    logger.error(
                        f"Failed to send TP notification without reply: {e2}")

    async def send_sl_notification(self, message_id: str, trade_data: dict,
                                   hit_price: float):
        pair = trade_data.get('pair', 'Unknown')
        action = trade_data.get('action', 'Unknown')
        tp_hits = trade_data.get('tp_hits', [])

        tp_status = f"TPs hit before SL: {', '.join(tp_hits)}" if tp_hits else "No TPs hit"

        notification = (f"**STOP LOSS HIT** {pair} {action}\n\n"
                        f"Price: {hit_price:.5f}\n"
                        f"{tp_status}")

        channel_message_map = trade_data.get('channel_message_map', {})
        all_channels = trade_data.get('all_channel_ids', [])

        if not all_channels:
            chat_id = trade_data.get('chat_id')
            if chat_id:
                all_channels = [chat_id]

        for channel_id in all_channels:
            try:
                reply_msg_id = channel_message_map.get(str(channel_id))
                if reply_msg_id:
                    await self.app.send_message(
                        channel_id,
                        notification,
                        reply_to_message_id=reply_msg_id)
                else:
                    await self.app.send_message(channel_id, notification)
            except Exception as e:
                logger.error(
                    f"Failed to send SL notification to {channel_id}: {e}")
                try:
                    await self.app.send_message(channel_id, notification)
                except Exception as e2:
                    logger.error(
                        f"Failed to send SL notification without reply: {e2}")

    async def send_breakeven_notification(self, message_id: str,
                                          trade_data: dict):
        pair = trade_data.get('pair', 'Unknown')
        action = trade_data.get('action', 'Unknown')
        entry_price = trade_data.get('entry_price', 0)

        notification = (
            f"**BREAKEVEN ALERT** {pair} {action}\n\n"
            f"TP1 hit! Consider moving stop loss to entry ({entry_price:.5f}) to secure profits."
        )

        channel_message_map = trade_data.get('channel_message_map', {})
        all_channels = trade_data.get('all_channel_ids', [])

        if not all_channels:
            chat_id = trade_data.get('chat_id')
            if chat_id:
                all_channels = [chat_id]

        for channel_id in all_channels:
            try:
                reply_msg_id = channel_message_map.get(str(channel_id))
                if reply_msg_id:
                    await self.app.send_message(
                        channel_id,
                        notification,
                        reply_to_message_id=reply_msg_id)
                else:
                    await self.app.send_message(channel_id, notification)
            except Exception as e:
                logger.error(
                    f"Failed to send breakeven notification to {channel_id}: {e}"
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
                    CREATE TABLE IF NOT EXISTS active_trades (
                        message_id VARCHAR(20) PRIMARY KEY,
                        channel_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        pair VARCHAR(20) NOT NULL,
                        action VARCHAR(10) NOT NULL,
                        entry_price DECIMAL(30,15) NOT NULL,
                        tp1_price DECIMAL(30,15) NOT NULL,
                        tp2_price DECIMAL(30,15) NOT NULL,
                        tp3_price DECIMAL(30,15) NOT NULL,
                        sl_price DECIMAL(30,15) NOT NULL,
                        status VARCHAR(50) DEFAULT 'active',
                        tp_hits TEXT DEFAULT '',
                        breakeven_active BOOLEAN DEFAULT FALSE,
                        entry_type VARCHAR(30),
                        channel_message_map TEXT DEFAULT '',
                        all_channel_ids TEXT DEFAULT '',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

                try:
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS channel_message_map TEXT DEFAULT \'\''
                    )
                    await conn.execute(
                        'ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS all_channel_ids TEXT DEFAULT \'\''
                    )
                except Exception:
                    pass

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS bot_status (
                        id INTEGER PRIMARY KEY DEFAULT 1,
                        last_online TIMESTAMP WITH TIME ZONE,
                        heartbeat_time TIMESTAMP WITH TIME ZONE,
                        CONSTRAINT tg_single_row_constraint UNIQUE (id)
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

                logger.info(
                    f"Loaded {len(AUTO_ROLE_CONFIG['active_members'])} active members from database"
                )
                logger.info(
                    f"Loaded {len(AUTO_ROLE_CONFIG['role_history'])} role history records"
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
                    message_id = row['message_id']
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

                    chat_id = row['channel_id']
                    if not channel_message_map and chat_id:
                        channel_message_map = {
                            str(chat_id):
                            int(message_id) if message_id.isdigit() else 0
                        }
                    if not all_channel_ids and chat_id:
                        all_channel_ids = [chat_id]

                    PRICE_TRACKING_CONFIG['active_trades'][message_id] = {
                        'message_id':
                        message_id,
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
                        'status':
                        row['status'],
                        'tp_hits':
                        tp_hits,
                        'breakeven_active':
                        row['breakeven_active'],
                        'created_at':
                        row['created_at'].isoformat()
                        if row['created_at'] else None,
                        'channel_message_map':
                        channel_message_map,
                        'all_channel_ids':
                        all_channel_ids
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
                channel_message_map_str = json.dumps(
                    trade_data.get('channel_message_map', {}))
                all_channel_ids = trade_data.get('all_channel_ids', [])
                all_channel_ids_str = ','.join(
                    str(cid) for cid in all_channel_ids)

                await conn.execute(
                    '''
                    INSERT INTO active_trades 
                    (message_id, channel_id, guild_id, pair, action, entry_price, tp1_price, tp2_price, tp3_price, sl_price, status, tp_hits, breakeven_active, entry_type, channel_message_map, all_channel_ids)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                    ON CONFLICT (message_id) DO UPDATE SET
                    status = $11, tp_hits = $12, breakeven_active = $13, channel_message_map = $15, all_channel_ids = $16, last_updated = NOW()
                ''', message_id, trade_data.get('chat_id', 0),
                    trade_data.get('chat_id', 0), trade_data.get('pair'),
                    trade_data.get('action'), trade_data.get('entry_price'),
                    trade_data.get('tp1_price'), trade_data.get('tp2_price'),
                    trade_data.get('tp3_price'), trade_data.get('sl_price'),
                    trade_data.get('status', 'active'), tp_hits_str,
                    trade_data.get('breakeven_active', False),
                    trade_data.get('entry_type', 'instant'),
                    channel_message_map_str, all_channel_ids_str)
        except Exception as e:
            logger.error(f"Error saving trade to database: {e}")

    async def update_trade_in_db(self, message_id: str, trade_data: dict):
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                tp_hits_str = ','.join(trade_data.get('tp_hits', []))

                await conn.execute(
                    '''
                    UPDATE active_trades 
                    SET status = $2, tp_hits = $3, breakeven_active = $4, last_updated = NOW()
                    WHERE message_id = $1
                ''', message_id, trade_data.get('status', 'active'),
                    tp_hits_str, trade_data.get('breakeven_active', False))
        except Exception as e:
            logger.error(f"Error updating trade in database: {e}")

    async def remove_trade_from_db(self, message_id: str, reason: str):
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    'DELETE FROM active_trades WHERE message_id = $1',
                    message_id)
        except Exception as e:
            logger.error(f"Error removing trade from database: {e}")

    def is_weekend_market_closed(self) -> bool:
        amsterdam_now = datetime.now(AMSTERDAM_TZ)
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
            await self.log_to_debug("No active trades to check for offline TP/SL hits")
            return

        await self.log_to_debug(f"Checking {len(PRICE_TRACKING_CONFIG['active_trades'])} active trades for TP/SL hits that occurred while offline...")

        offline_hits_found = 0

        for message_id, trade_data in list(PRICE_TRACKING_CONFIG['active_trades'].items()):
            try:
                current_price = await self.get_live_price(trade_data['pair'])
                if current_price is None:
                    continue

                action = trade_data['action']
                entry = trade_data.get('entry', 0)
                tp_hits = trade_data.get('tp_hits', [])

                if action == "BUY" and current_price <= trade_data['sl']:
                    await self.handle_sl_hit(message_id, trade_data, current_price)
                    offline_hits_found += 1
                    continue
                elif action == "SELL" and current_price >= trade_data['sl']:
                    await self.handle_sl_hit(message_id, trade_data, current_price)
                    offline_hits_found += 1
                    continue

                tp_levels_hit = []

                if action == "BUY":
                    if "tp1" not in tp_hits and current_price >= trade_data['tp1']:
                        tp_levels_hit.append("tp1")
                    if "tp2" not in tp_hits and current_price >= trade_data['tp2']:
                        tp_levels_hit.append("tp2")
                    if "tp3" not in tp_hits and current_price >= trade_data['tp3']:
                        tp_levels_hit.append("tp3")
                elif action == "SELL":
                    if "tp1" not in tp_hits and current_price <= trade_data['tp1']:
                        tp_levels_hit.append("tp1")
                    if "tp2" not in tp_hits and current_price <= trade_data['tp2']:
                        tp_levels_hit.append("tp2")
                    if "tp3" not in tp_hits and current_price <= trade_data['tp3']:
                        tp_levels_hit.append("tp3")

                if tp_levels_hit:
                    for tp_level in ["tp1", "tp2", "tp3"]:
                        if tp_level in tp_levels_hit:
                            await self.handle_tp_hit(message_id, trade_data, tp_level, current_price)
                            offline_hits_found += 1
                    continue

                if trade_data.get('breakeven_active'):
                    if action == "BUY" and current_price <= entry:
                        await self.handle_breakeven_hit(message_id, trade_data)
                        offline_hits_found += 1
                        continue
                    elif action == "SELL" and current_price >= entry:
                        await self.handle_breakeven_hit(message_id, trade_data)
                        offline_hits_found += 1
                        continue

            except Exception as e:
                logger.error(f"Error checking offline TP/SL for {message_id}: {e}")
                continue

        if offline_hits_found > 0:
            await self.log_to_debug(f"Found and processed {offline_hits_found} TP/SL hits that occurred while offline")
        else:
            await self.log_to_debug("No offline TP/SL hits detected")

    async def price_tracking_loop(self):
        await asyncio.sleep(10)

        while self.running:
            try:
                if self.is_weekend_market_closed():
                    await asyncio.sleep(PRICE_TRACKING_CONFIG['check_interval'])
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
                current_time = datetime.now(AMSTERDAM_TZ)
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

            current_time = datetime.now(AMSTERDAM_TZ)

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
                    f"For now, feel free to continue following our trade signals in the Free Group: {FREE_GROUP_LINK}\n\n"
                    f"**Feel free to join us again through this link:** {WHOP_PURCHASE_LINK}"
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
                current_time = datetime.now(AMSTERDAM_TZ)

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
                    f"Hey! It's been 3 days since your **3-day free trial for our VIP Group** ended. "
                    f"We truly hope that you were able to catch good trades with us during that time.\n\n"
                    f"As you've probably seen, our Free Group gets **1 free signal per day**, while our "
                    f"**VIP members** receive **6+ high-quality signals per day**. That means that our "
                    f"VIP Group offers way more chances to profit and grow consistently.\n\n"
                    f"We'd love to **invite you back to the VIP Group,** so you don't miss out on more solid opportunities.\n\n"
                    f"**Feel free to join us again through this link:** {WHOP_PURCHASE_LINK}"
                )
            elif days == 7:
                message = (
                    f"It's been a week since your free trial for our VIP Group ended. Since then, our "
                    f"**VIP members have been catching trade setups daily in the VIP Group**.\n\n"
                    f"If you found value in just 3 days, imagine what results you could've been seeing by now with full access. "
                    f"It's all about **consistency and staying connected to the right information**.\n\n"
                    f"We'd like to **personally invite you to rejoin the VIP Group** and get back into the rhythm.\n\n"
                    f"**Feel free to join us again through this link:** {WHOP_PURCHASE_LINK}"
                )
            elif days == 14:
                message = (
                    f"Hey! It's been two weeks since your free trial for our VIP Group ended. We hope you've stayed active since then.\n\n"
                    f"If you've been trading solo or passively following the Free Group, you might be feeling the difference. "
                    f"In the VIP Group, it's not just about more signals. It's about the **structure, support, and smarter decision-making**. "
                    f"That edge can make all the difference over time.\n\n"
                    f"We'd love to **invite you back into the VIP Group** and help you start compounding results again.\n\n"
                    f"**Feel free to join us again through this link:** {WHOP_PURCHASE_LINK}"
                )
            else:
                return

            await self.app.send_message(int(member_id), message)
            await self.log_to_debug(
                f"Sent {days}-day follow-up DM to user {member_id}")
        except Exception as e:
            logger.error(
                f"Could not send {days}-day follow-up DM to {member_id}: {e}")

    async def register_bot_commands(self):
        try:
            public_commands = [
                BotCommand("start", "Welcome message"),
                BotCommand("help", "Show all commands"),
                BotCommand("activetrades", "View active trading signals"),
                BotCommand("pricetest", "Test live price for a pair"),
                BotCommand("getid", "Get chat/user ID"),
            ]
            await self.app.set_bot_commands(public_commands)
            logger.info("Public bot commands registered")

            if BOT_OWNER_USER_ID:
                owner_commands = [
                    BotCommand("start", "Welcome message"),
                    BotCommand("help", "Show all commands"),
                    BotCommand("activetrades", "View active trading signals"),
                    BotCommand("pricetest", "Test live price for a pair"),
                    BotCommand("getid", "Get chat/user ID"),
                    BotCommand("entry", "Create trading signal (menu)"),
                    BotCommand("tradeoverride", "Override trade status"),
                    BotCommand("timedautorole", "Manage trial system"),
                    BotCommand("dbstatus", "Database health check"),
                    BotCommand("dmstatus", "DM delivery statistics"),
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
                        current_time = datetime.now(AMSTERDAM_TZ)
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
