import os
import pytz
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

import pyrogram.utils as pyrogram_utils
# Essential fix for "Peer id invalid" errors with channel IDs
pyrogram_utils.MIN_CHANNEL_ID = -10099999999999
pyrogram_utils.MIN_CHAT_ID = -9999999999999
pyrogram_utils.MIN_USER_ID = -10099999999999

load_dotenv()

def safe_int(value: str, default: int = 0) -> int:
    try:
        return int(value) if value else default
    except (ValueError, TypeError):
        return default

# Telegram Configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_API_ID = safe_int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")

# Standardized Group/Owner IDs
BOT_OWNER_USER_ID = 1045239145
FREE_GROUP_ID = -1002360811986
VIP_GROUP_ID = -1002446702636
DEBUG_GROUP_ID = -1003277177952

SIGNAL_SOURCE_GROUP_ID = safe_int(os.getenv("SIGNAL_SOURCE_GROUP_ID", "0"))

# Links
FREE_GROUP_LINK = os.getenv("FREE_GROUP_LINK", "")
VIP_GROUP_LINK = os.getenv("VIP_GROUP_LINK", "")
VIP_TRIAL_INVITE_LINK = "https://t.me/+5X18tTjgM042ODU0"
WHOP_PURCHASE_LINK = "https://whop.com/gold-pioneer/gold-pioneer/"

# Global State for Pending Operations
PENDING_ENTRIES = {}

# Timezone
AMSTERDAM_TZ = pytz.timezone('Europe/Amsterdam')

# Database Configuration
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Pair Configuration
PAIR_CONFIG = {
    'XAUUSD': {'decimals': 2, 'pip_value': 0.1, 'name': 'Gold (XAU/USD)'},
    'GBPJPY': {'decimals': 3, 'pip_value': 0.01, 'name': 'GBP/JPY'},
    'GBPUSD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'GBP/USD'},
    'EURUSD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'EUR/USD'},
    'AUDUSD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'AUD/USD'},
    'NZDUSD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'NZD/USD'},
    'US100': {'decimals': 1, 'pip_value': 1.0, 'name': 'US100 (Nasdaq)'},
    'US500': {'decimals': 2, 'pip_value': 0.1, 'name': 'US500 (S&P 500)'},
    'GER40': {'decimals': 1, 'pip_value': 1.0, 'name': 'GER40 (DAX)'},
    'BTCUSD': {'decimals': 1, 'pip_value': 10, 'name': 'Bitcoin (BTC/USD)'},
    'GBPCHF': {'decimals': 4, 'pip_value': 0.0001, 'name': 'GBP/CHF'},
    'USDCHF': {'decimals': 4, 'pip_value': 0.0001, 'name': 'USD/CHF'},
    'CADCHF': {'decimals': 4, 'pip_value': 0.0001, 'name': 'CAD/CHF'},
    'AUDCHF': {'decimals': 4, 'pip_value': 0.0001, 'name': 'AUD/CHF'},
    'CHFJPY': {'decimals': 3, 'pip_value': 0.01, 'name': 'CHF/JPY'},
    'CADJPY': {'decimals': 3, 'pip_value': 0.01, 'name': 'CAD/JPY'},
    'AUDJPY': {'decimals': 3, 'pip_value': 0.01, 'name': 'AUD/JPY'},
    'USDCAD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'USD/CAD'},
    'GBPCAD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'GBP/CAD'},
    'EURCAD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'EUR/CAD'},
    'AUDCAD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'AUD/CAD'},
    'AUDNZD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'AUD/NZD'},
    'USDJPY': {'decimals': 3, 'pip_value': 0.01, 'name': 'USD/JPY'},
    'EURJPY': {'decimals': 3, 'pip_value': 0.01, 'name': 'EUR/JPY'},
    'NZDJPY': {'decimals': 3, 'pip_value': 0.01, 'name': 'NZD/JPY'},
    'EURGBP': {'decimals': 4, 'pip_value': 0.0001, 'name': 'EUR/GBP'},
    'EURAUD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'EUR/AUD'},
    'EURCHF': {'decimals': 4, 'pip_value': 0.0001, 'name': 'EUR/CHF'},
    'EURNZD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'EUR/NZD'},
    'GBPAUD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'GBP/AUD'},
    'GBPNZD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'GBP/NZD'},
    'NZDCAD': {'decimals': 4, 'pip_value': 0.0001, 'name': 'NZD/CAD'},
    'NZDCHF': {'decimals': 4, 'pip_value': 0.0001, 'name': 'NZD/CHF'},
}

EXCLUDED_FROM_TRACKING = ['XAUUSD', 'BTCUSD', 'GER40', 'US100']

# Price Tracking Configuration
PRICE_TRACKING_CONFIG = {
    "enabled": True,
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
    "api_priority_order": ["currencybeacon", "exchangerate_api", "currencylayer", "abstractapi"],
    "check_interval": 120,
    "last_price_check_time": None,
}

# Auto Role and Trial Configuration
AUTO_ROLE_CONFIG = {
    "enabled": True,
    "duration_hours": 72,
    "active_members": {},
    "role_history": {},
    "dm_schedule": {},
    "weekend_pending": {}
}

MESSAGE_TEMPLATES = {
    "Trial Status & Expiry": {
        "Trial Expired": {
            "id": "trial_expired",
            "message": "Hey! Your **3-day free access** to the VIP Group has unfortunately **ran out**. We truly hope you were able to benefit with us & we hope to see you back soon! For now, feel free to continue following our trade signals in our Free Group: https://t.me/fxpippioneers\n\n**Want to rejoin the VIP Group? You can regain access through this link:** https://whop.com/gold-pioneer/gold-pioneer/"
        }
    }
}
