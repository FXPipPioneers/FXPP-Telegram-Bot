import os
import pytz
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

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

# Group IDs
BOT_OWNER_USER_ID = safe_int(os.getenv("BOT_OWNER_USER_ID", "0"))
FREE_GROUP_ID = safe_int(os.getenv("FREE_GROUP_ID", "0"))
VIP_GROUP_ID = safe_int(os.getenv("VIP_GROUP_ID", "0"))

# Use the direct value from environment variable for Peer ID compatibility
_debug_id = os.getenv("DEBUG_GROUP_ID", "0")
# Standardize DEBUG_GROUP_ID as an integer for Telegram channel/group IDs
try:
    _clean_id = _debug_id.strip()
    if not _clean_id:
        DEBUG_GROUP_ID = 0
    elif _clean_id.startswith("-100"):
        # Already has the correct channel prefix
        DEBUG_GROUP_ID = int(_clean_id)
    elif _clean_id.startswith("-"):
        # It's a negative number, keep it as is
        DEBUG_GROUP_ID = int(_clean_id)
    else:
        # If it's a positive number but meant for a group, Telegram often needs it negative
        # However, we'll try to parse it directly first as the user might have provided the full -100 ID
        DEBUG_GROUP_ID = int(_clean_id)
except (ValueError, TypeError):
    DEBUG_GROUP_ID = 0

SIGNAL_SOURCE_GROUP_ID = safe_int(os.getenv("SIGNAL_SOURCE_GROUP_ID", "0"))

# Links
FREE_GROUP_LINK = os.getenv("FREE_GROUP_LINK", "")
VIP_GROUP_LINK = os.getenv("VIP_GROUP_LINK", "")
VIP_TRIAL_INVITE_LINK = "https://t.me/+5X18tTjgM042ODU0"
WHOP_PURCHASE_LINK = "https://whop.com/gold-pioneer/gold-pioneer/"

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


# Logging Configuration
LOGGING_CONFIG = {
    "level": logging.INFO,
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "filename": "bot.log"
}

# Global State for Pending Operations
PENDING_ENTRIES = {}

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
    "check_interval": 75,
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
        },
        "Trial Rejected (Used Before)": {
            "id": "trial_rejected",
            "message": "Hey! Unfortunately, our free trial can only be used once per person. Your trial has already ran out, so we can't give you another.\n\nWe truly hope that you were able to profit with us during your free trial. If you were happy with the results you got, then feel free to rejoin our VIP group through this link: https://whop.com/gold-pioneer/gold-pioneer/"
        }
    },
    "Welcome & Onboarding": {
        "Welcome DM (New Free Group Member)": {
            "id": "welcome_free",
            "message": "**Hey, Welcome to FX Pip Pioneers!**\n\n**Want to try our VIP Group for FREE?**\nWe're offering a **3-day free trial** of our VIP Group where you'll receive **6+ high-quality trade signals per day**.\n\n**Your free trial will automatically be activated once you join our VIP group through this link:** https://t.me/+5X18tTjgM042ODU0\n\nGood luck trading!"
        }
    }
}
