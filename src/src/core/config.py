import os
import pytz
from dotenv import load_dotenv

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
DEBUG_GROUP_ID = safe_int(os.getenv("DEBUG_GROUP_ID", "0"))

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

# Trial Configuration
AUTO_ROLE_CONFIG = {
    "enabled": True,
    "duration_hours": 72,
    "active_members": {},
    "role_history": {},
    "dm_schedule": {},
    "weekend_pending": {}
}

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
    "tracking_active": False
}

# Message Templates
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
