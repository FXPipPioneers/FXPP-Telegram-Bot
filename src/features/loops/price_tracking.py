import asyncio
import logging
import pytz
from datetime import datetime, timedelta
from src.features.core.config import AMSTERDAM_TZ, PRICE_TRACKING_CONFIG

logger = logging.getLogger(__name__)

class PriceTrackingLoop:
    """Background loop: continuously monitors active trades for TP/SL hits"""
    
    def __init__(self, app, bot_instance):
        self.app = app
        self.bot = bot_instance
    
    async def run(self):
        """Price tracking loop - checks active trades every 4-8 minutes"""
        await asyncio.sleep(10)
        
        while getattr(self.bot, 'running', True):
            try:
                # Record when this price check cycle started
                PRICE_TRACKING_CONFIG['last_price_check_time'] = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                
                await self.bot.log_to_debug("🔄 **Price Tracking Loop**: Checking active trades...")
                
                if self.bot.is_weekend_market_closed():
                    await asyncio.sleep(PRICE_TRACKING_CONFIG['check_interval'])
                    continue
                
                trades = list(PRICE_TRACKING_CONFIG['active_trades'].items())
                
                from src.features.trading.engine import TradingEngine
                engine = TradingEngine(self.bot.db.pool, self.bot)
                
                for message_id, trade_data in trades:
                    if message_id not in PRICE_TRACKING_CONFIG['active_trades']:
                        continue
                    
                    current_price = await self.bot.get_live_price(trade_data.get('pair'))
                    if current_price:
                        await engine.process_price_update(message_id, trade_data, current_price, self.bot)
                    await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Error in price tracking loop: {e}")
            
            await asyncio.sleep(PRICE_TRACKING_CONFIG['check_interval'])


def create_price_tracking_loop(app, bot_instance):
    """Factory function to create and return the price tracking loop"""
    loop = PriceTrackingLoop(app, bot_instance)
    return loop.run()
