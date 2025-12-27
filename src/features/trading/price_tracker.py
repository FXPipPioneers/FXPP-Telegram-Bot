import asyncio
import logging
from datetime import datetime
import pytz
from src.features.core.config import AMSTERDAM_TZ, PRICE_TRACKING_CONFIG, PAIR_CONFIG
from src.features.trading.engine import TradingEngine

logger = logging.getLogger(__name__)

class PriceTracker:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.engine = TradingEngine(bot_instance.db_pool)

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

                await self.bot.log_to_debug(f"🔄 **Price Tracking Loop**: Checking {len(active_trades)} active trades...")

                for message_id, trade_data in active_trades:
                    try:
                        current_price = await self.bot.get_live_price(trade_data['pair'])
                        if current_price is None:
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
                if current_price is None: continue
                await self.engine.process_price_update(message_id, trade_data, current_price, self.bot)
            except Exception as e:
                logger.error(f"Offline check error for {message_id}: {e}")
