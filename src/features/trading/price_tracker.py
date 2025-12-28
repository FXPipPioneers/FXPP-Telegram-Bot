import asyncio
import logging
import aiohttp
from datetime import datetime
import pytz
from src.features.core.config import AMSTERDAM_TZ, PRICE_TRACKING_CONFIG, PAIR_CONFIG

logger = logging.getLogger(__name__)

class PriceTracker:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self._engine = None

    @property
    def engine(self):
        if self._engine is None and self.bot.db_pool:
            from src.features.trading.engine import TradingEngine
            self._engine = TradingEngine(self.bot.db_pool, bot=self.bot)
        return self._engine

    async def get_price_from_api(self, api_name, pair):
        """Fetch price from a specific API"""
        api_key = PRICE_TRACKING_CONFIG["api_keys"].get(f"{api_name}_key")
        if not api_key:
            return None

        url_base = PRICE_TRACKING_CONFIG["api_endpoints"].get(api_name)
        if not url_base:
            return None

        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                if api_name == "currencybeacon":
                    url = f"{url_base}?api_key={api_key}&base=USD&symbols={pair.replace('USD', '')}"
                    async with session.get(url) as resp:
                        data = await resp.json()
                        if data and "rates" in data:
                            symbol = pair.replace('USD', '')
                            return data["rates"].get(symbol)
                
                elif api_name == "exchangerate_api":
                    url = f"{url_base}/{api_key}/latest/USD"
                    async with session.get(url) as resp:
                        data = await resp.json()
                        if data and "conversion_rates" in data:
                            symbol = pair.replace('USD', '')
                            return data["conversion_rates"].get(symbol)

                elif api_name == "currencylayer":
                    url = f"{url_base}?access_key={api_key}&source=USD&currencies={pair.replace('USD', '')}"
                    async with session.get(url) as resp:
                        data = await resp.json()
                        if data and "quotes" in data:
                            quote = f"USD{pair.replace('USD', '')}"
                            return data["quotes"].get(quote)

                elif api_name == "abstractapi":
                    url = f"{url_base}?api_key={api_key}&base=USD&symbols={pair.replace('USD', '')}"
                    async with session.get(url) as resp:
                        data = await resp.json()
                        if data and "exchange_rates" in data:
                            symbol = pair.replace('USD', '')
                            return data["exchange_rates"].get(symbol)

        except Exception as e:
            logger.error(f"Error fetching from {api_name}: {e}")
        return None

    async def get_live_price(self, pair):
        """Fetch live price with multi-API fallback"""
        pair = pair.upper().replace("/", "")
        
        # Priority order from config
        for api in PRICE_TRACKING_CONFIG["api_priority_order"]:
            price = await self.get_price_from_api(api, pair)
            if price:
                return float(price)
        
        return None

    async def check_prices_loop(self):
        """Main background loop for price tracking"""
        await asyncio.sleep(10)  # Initial wait
        while getattr(self.bot, 'running', True):
            try:
                if not PRICE_TRACKING_CONFIG["enabled"]:
                    await asyncio.sleep(60)
                    continue

                if self.bot.is_weekend_market_closed():
                    # Markets are closed, no need to check prices or refresh APIs at all
                    await asyncio.sleep(3600)  # Check once an hour just to see if it's Monday yet
                    continue

                active_trades = list(PRICE_TRACKING_CONFIG['active_trades'].items())
                if not active_trades:
                    await asyncio.sleep(30)
                    continue

                logger.info(f"Price Tracking Loop: Checking {len(active_trades)} active trades...")

                for message_id, trade_data in active_trades:
                    try:
                        current_price = await self.bot.get_live_price(trade_data['pair'])
                        if current_price is None or not self.engine:
                            continue

                        # Pass to engine for hit detection and processing
                        await self.engine.process_price_update(message_id, trade_data, current_price, self.bot)
                        
                    except Exception as e:
                        logger.error(f"Error checking price for trade {message_id}: {e}")

                await asyncio.sleep(PRICE_TRACKING_CONFIG.get("check_interval", 120))
            except Exception as e:
                logger.error(f"Error in check_prices_loop: {e}")
                await asyncio.sleep(60)

    async def check_offline_hits(self):
        """Check for hits that occurred while bot was offline"""
        await self.bot.load_active_trades_from_db()
        if not PRICE_TRACKING_CONFIG['active_trades']:
            return

        await self.bot.log_to_debug(f"🔍 Checking {len(PRICE_TRACKING_CONFIG['active_trades'])} trades for offline hits...")
        
        for message_id, trade_data in list(PRICE_TRACKING_CONFIG['active_trades'].items()):
            try:
                current_price = await self.bot.get_live_price(trade_data['pair'])
                if current_price is None or not self.engine: continue
                await self.engine.process_price_update(message_id, trade_data, current_price, self.bot)
            except Exception as e:
                logger.error(f"Offline check error for {message_id}: {e}")
