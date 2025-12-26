import aiohttp
import asyncio
import logging
from datetime import datetime
import pytz
from typing import Optional, Dict, Any, List
from src.core.config import PRICE_TRACKING_CONFIG, AMSTERDAM_TZ

logger = logging.getLogger(__name__)

class PriceEngine:
    def __init__(self, db_manager):
        self.db = db_manager
        self.session = None
        self.app = None # To be set after client initialization
        self._tracking_task = None

    async def get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def get_price_from_api(self, api_name: str, pair: str) -> Optional[float]:
        api_keys = PRICE_TRACKING_CONFIG['api_keys']
        session = await self.get_session()
        
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            
            if api_name == "currencybeacon":
                key = api_keys.get('currencybeacon_key')
                if not key: return None
                if pair.startswith("XAU"): base, quote = "XAU", pair[3:]
                elif pair.startswith("XAG"): base, quote = "XAG", pair[3:]
                elif len(pair) == 6: base, quote = pair[:3], pair[3:]
                else: return None
                url = f"https://api.currencybeacon.com/v1/latest?api_key={key}&base={base}&symbols={quote}"
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        return float(data['rates'][quote]) if 'rates' in data and quote in data['rates'] else None

            elif api_name == "exchangerate_api":
                key = api_keys.get('exchangerate_api_key')
                if not key: return None
                if len(pair) == 6: base, quote = pair[:3], pair[3:]
                else: return None
                url = f"https://v6.exchangerate-api.com/v6/{key}/pair/{base}/{quote}"
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        return float(data['conversion_rate']) if 'conversion_rate' in data else None

            elif api_name == "currencylayer":
                key = api_keys.get('currencylayer_key')
                if not key: return None
                url = f"http://api.currencylayer.com/live?access_key={key}&currencies={pair[3:]}&source={pair[:3]}&format=1"
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        quote_key = f"{pair[:3]}{pair[3:]}"
                        return float(data['quotes'][quote_key]) if data.get('success') and quote_key in data.get('quotes', {}) else None

            elif api_name == "abstractapi":
                key = api_keys.get('abstractapi_key')
                if not key: return None
                url = f"https://exchange-rates.abstractapi.com/v1/live/?api_key={key}&base={pair[:3]}&target={pair[3:]}"
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        return float(data['exchange_rates'][pair[3:]]) if 'exchange_rates' in data and pair[3:] in data['exchange_rates'] else None
            
        except Exception as e:
            logger.error(f"Error with {api_name} for {pair}: {e}")
        return None

    async def run_engagement_loop(self):
        """Monitor for member engagement and send offers."""
        while True:
            try:
                # Logic for engagement tracking would go here
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Error in engagement loop: {e}")
                await asyncio.sleep(60)

    async def start_all_loops(self):
        asyncio.create_task(self.run_welcome_logic())
        asyncio.create_task(self.run_trial_expiry_check())
        asyncio.create_task(self.run_engagement_loop())
        logger.info("All background loops started.")

    async def get_live_price(self, pair: str) -> Optional[float]:
        pair_clean = pair.upper().replace("/", "").replace("-", "").replace("_", "")
        for api_name in PRICE_TRACKING_CONFIG['api_priority_order']:
            price = await self.get_price_from_api(api_name, pair_clean)
            if price: return price
        return None

    def is_trading_day(self, dt: datetime) -> bool:
        # 0=Monday, 4=Friday. 5=Saturday, 6=Sunday
        return dt.weekday() < 5

    async def calculate_expiry_trading_days(self, start_time: datetime, days: int = 3) -> datetime:
        current = start_time
        added_days = 0
        while added_days < days:
            current += timedelta(days=1)
            if self.is_trading_day(current):
                added_days += 1
        return current

    async def run_trial_expiry_check(self):
        while True:
            try:
                # Logic to check active_members and remove role if expired
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Error in trial expiry: {e}")
                await asyncio.sleep(60)

    async def check_peer_id(self, user_id: int) -> bool:
        """Check if we can resolve the peer ID for a user."""
        try:
            # In Pyrogram, this attempts to get the user entity
            # If successful, peer ID is established
            await self.app.get_users(user_id)
            return True
        except Exception:
            return False

    async def run_welcome_logic(self):
        """Escalating delay logic for welcome DMs."""
        while True:
            try:
                pending = await self.db.get_pending_peer_checks()
                for user in pending:
                    user_id = user['user_id']
                    if await self.check_peer_id(user_id):
                        await self.db.update_peer_id(user_id, True)
                        await self.send_welcome_dm(user_id)
                        continue

                    # Escalation logic: 30m -> 1h -> 3h -> 24h
                    # Update next_check_at based on current_delay_minutes
                    current_delay = user['current_delay_minutes']
                    next_delay = 30
                    if current_delay == 30: next_delay = 60
                    elif current_delay == 60: next_delay = 180
                    elif current_delay == 180: next_delay = 1440
                    
                    next_check = datetime.now(pytz.UTC) + timedelta(minutes=next_delay)
                    await self.db.update_next_check(user_id, next_delay, next_check)
                await asyncio.sleep(180) # Check every 3 mins
            except Exception as e:
                logger.error(f"Error in welcome logic: {e}")
                await asyncio.sleep(60)

    async def send_welcome_dm(self, user_id: int):
        from src.core.config import MESSAGE_TEMPLATES
        template = MESSAGE_TEMPLATES["Welcome & Onboarding"]["Welcome DM (New Free Group Member)"]["message"]
        try:
            await self.app.send_message(user_id, template)
            await self.db.mark_welcome_sent(user_id)
        except Exception as e:
            logger.error(f"Failed to send welcome DM: {e}")
