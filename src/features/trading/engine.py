from pyrogram.client import Client
from typing import Dict, Optional, List, Any
import re
import logging
from datetime import datetime
import pytz
import json
import random
import asyncio
from src.features.core.config import PAIR_CONFIG, EXCLUDED_FROM_TRACKING, AMSTERDAM_TZ, BOT_OWNER_USER_ID, PRICE_TRACKING_CONFIG
from src.features.trading.hit_detector import HitDetector

logger = logging.getLogger(__name__)

class SignalParser:
    @staticmethod
    def parse_message(content: str) -> Optional[Dict]:
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
            
            pair_match = re.search(r'Trade Signal For:\s*\*?\*?([A-Z0-9/.\-]+)\*?\*?', content, re.IGNORECASE)
            if pair_match:
                raw_pair = pair_match.group(1).strip()
                trade_data["pair"] = raw_pair.upper().replace("/", "").replace("-", "").replace("_", "").replace(".", "")
            
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
            
            entry_match = re.search(r'Entry Price:\s*\$?([0-9]+(?:\.[0-9]+)?)', content, re.IGNORECASE)
            if entry_match:
                trade_data["entry"] = float(entry_match.group(1))
            else:
                entry_match = re.search(r'Entry[:\s]*\$?([0-9]+(?:\.[0-9]+)?)', content, re.IGNORECASE)
                if entry_match:
                    trade_data["entry"] = float(entry_match.group(1))
            
            tp1_match = re.search(r'Take Profit 1:\s*\$?([0-9]+(?:\.[0-9]+)?)', content, re.IGNORECASE)
            if tp1_match:
                trade_data["tp1"] = float(tp1_match.group(1))
            
            tp2_match = re.search(r'Take Profit 2:\s*\$?([0-9]+(?:\.[0-9]+)?)', content, re.IGNORECASE)
            if tp2_match:
                trade_data["tp2"] = float(tp2_match.group(1))
            
            tp3_match = re.search(r'Take Profit 3:\s*\$?([0-9]+(?:\.[0-9]+)?)', content, re.IGNORECASE)
            if tp3_match:
                trade_data["tp3"] = float(tp3_match.group(1))
            
            sl_match = re.search(r'Stop Loss:\s*\$?([0-9]+(?:\.[0-9]+)?)', content, re.IGNORECASE)
            if sl_match:
                trade_data["sl"] = float(sl_match.group(1))
            
            if trade_data["pair"] and trade_data["action"] and trade_data["entry"]:
                return trade_data
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error parsing signal message: {e}")
            return None

class TradingEngine:
    """Core logic for signal parsing, trade management, and hit validation"""
    def __init__(self, db_pool, bot=None):
        self.db_pool = db_pool
        self.bot = bot
        self.hit_detector = HitDetector(db_pool)

    async def handle_manual_signal(self, message_text, chat_id, message_id, bot):
        """Automatically detect and parse manual signals"""
        if "Trade Signal For:" not in message_text:
            return
            
        trade_data = SignalParser.parse_message(message_text)
        if not trade_data:
            return
            
        trade_key = f"{chat_id}_{message_id}"
        trade_data['chat_id'] = chat_id
        trade_data['message_id'] = str(message_id)
        
        pair = trade_data.get('pair')
        action = trade_data.get('action')
        entry_price = trade_data.get('entry')
        
        if not pair or not action or not entry_price:
            return

        if not trade_data.get('tp1'):
            levels = self.calculate_tp_sl_levels(float(entry_price), str(pair), str(action))
            trade_data.update({
                'tp1_price': levels['tp1'],
                'tp2_price': levels['tp2'],
                'tp3_price': levels['tp3'],
                'sl_price': levels['sl']
            })
        else:
            trade_data['tp1_price'] = trade_data['tp1']
            trade_data['tp2_price'] = trade_data['tp2']
            trade_data['tp3_price'] = trade_data['tp3']
            trade_data['sl_price'] = trade_data['sl']

        PRICE_TRACKING_CONFIG['active_trades'][trade_key] = trade_data
        
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute('''
                        INSERT INTO active_trades (
                            message_id, channel_id, guild_id, pair, action,
                            entry_price, tp1_price, tp2_price, tp3_price, sl_price,
                            status, tp_hits, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
                        ON CONFLICT (message_id) DO UPDATE SET
                            tp_hits = EXCLUDED.tp_hits,
                            status = EXCLUDED.status
                    ''', trade_key, chat_id, chat_id, str(pair), str(action),
                    float(entry_price), float(trade_data['tp1_price']), float(trade_data['tp2_price']),
                    float(trade_data['tp3_price']), float(trade_data['sl_price']), str(trade_data['status']),
                    ",".join(trade_data.get('tp_hits', [])))
            except Exception as e:
                logger.error(f"Error saving manual signal to DB: {e}")
            
        if bot and hasattr(bot, 'log_to_debug'):
            await bot.log_to_debug(f"✅ Manual Signal Detected: {pair} {action}")

    def calculate_tp_sl_levels(self, entry: float, pair: str, action: str) -> Dict[str, float]:
        """Calculate TP/SL levels based on fixed pip values"""
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
            tp1 = entry + tp1_pips
            tp2 = entry + tp2_pips
            tp3 = entry + tp3_pips
            sl = entry - sl_pips
        else:
            tp1 = entry - tp1_pips
            tp2 = entry - tp2_pips
            tp3 = entry - tp3_pips
            sl = entry + sl_pips
        
        return {
            'entry': entry,
            'tp1': tp1,
            'tp2': tp2,
            'tp3': tp3,
            'sl': sl
        }

    def validate_chronological_hits(self, hits: list) -> list:
        """Validate hits chronologically according to trading rules"""
        valid_hits = []
        sl_hit = False
        tp_levels_hit = set()
        
        for hit in sorted(hits, key=lambda x: x.get('hit_time', 0)):
            hit_type = hit.get('hit_type')
            hit_level = hit.get('hit_level')
            
            if sl_hit and hit_type == 'tp':
                continue
            
            if hit_type == 'sl' and 'TP2' in tp_levels_hit:
                continue
            
            if hit_type == 'sl' and 'TP3' in tp_levels_hit:
                continue
            if hit_type == 'tp' and hit_level == 'TP3' and sl_hit:
                continue
            
            valid_hits.append(hit)
            
            if hit_type == 'sl':
                sl_hit = True
            elif hit_type == 'tp':
                tp_levels_hit.add(hit_level)
        
        return valid_hits


    async def process_price_update(self, message_id, trade_data, current_price, bot):
        """Delegate hit detection to HitDetector module"""
        await self.hit_detector.process_price_update(message_id, trade_data, current_price, bot)


class BackgroundEngine:
    """Orchestrates all background loops and trading processes"""
    def __init__(self, db, bot):
        self.db = db
        self.bot = bot
        self.running = True

    async def start(self):
        """Start all background loops"""
        logger.info("Starting Background Engine...")
        
        # 1. Start Peer ID Escalation Loop
        from src.features.loops.peer_id_escalation import PeerIDEscalationLoop
        peer_id_loop = PeerIDEscalationLoop(self.bot, self.db.pool, self.bot)
        asyncio.create_task(peer_id_loop.run())
        
        # 2. Start Price Tracking Loop
        from src.features.loops.price_tracking import PriceTrackingLoop
        price_loop = PriceTrackingLoop(self.bot, self.bot)
        asyncio.create_task(price_loop.run())
        
        # 3. Start Trial Expiry Loop
        from src.features.loops.trial_expiry import TrialExpiryLoop
        expiry_loop = TrialExpiryLoop(self.bot, self.db.pool, self.bot)
        asyncio.create_task(expiry_loop.run())
        
        # 4. Start Daily VIP Trial Offer Loop
        from src.features.loops.daily_trial_offers import DailyVIPTrialOfferLoop
        offer_loop = DailyVIPTrialOfferLoop(self.bot, self.db.pool, self.bot)
        asyncio.create_task(offer_loop.run())
        
        # 5. Start Pre-Expiration Warning Loop
        from src.features.loops.preexpiration_warnings import PreexpirationWarningLoop
        warning_loop = PreexpirationWarningLoop(self.bot, self.bot)
        asyncio.create_task(warning_loop.run())
        
        # 6. Start Follow-up DMs Loop
        from src.features.loops.followup_dms import FollowupDMLoop
        followup_loop = FollowupDMLoop(self.bot, self.bot)
        asyncio.create_task(followup_loop.run())
        
        # 7. Start Engagement Tracking Loop
        from src.features.loops.engagement_tracking import EngagementTrackingLoop
        engagement_loop = EngagementTrackingLoop(self.bot, self.db.pool, self.bot)
        asyncio.create_task(engagement_loop.run())

    async def stop(self):
        """Stop all background loops"""
        self.running = False
        logger.info("Background Engine stopped.")
