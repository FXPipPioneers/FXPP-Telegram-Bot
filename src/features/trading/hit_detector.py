"""
Hit Detection Module: Detects and processes TP/SL/Breakeven hits on active trades
This module handles all price hit detection logic, validation, and notifications.
"""

import logging
import random
import asyncio
from typing import Dict, Optional, List, Any
from src.features.core.config import PRICE_TRACKING_CONFIG, AMSTERDAM_TZ
import pytz

logger = logging.getLogger(__name__)

class HitDetector:
    """Detects and processes TP/SL/Breakeven hits on trading signals"""
    
    def __init__(self, db_pool=None):
        self.db_pool = db_pool

    async def process_price_update(self, message_id: str, trade_data: dict, current_price: float, bot) -> None:
        """
        Core hit detection logic - checks current price against TP/SL levels
        Routes to appropriate handler (TP/SL/Breakeven)
        """
        if trade_data.get('manual_tracking_only', False):
            return
            
        if not await self.check_message_still_exists(message_id, trade_data, bot):
            await self._remove_completed_trade(message_id, bot)
            return
            
        trade_data = await self.verify_trade_data_consistency(message_id, trade_data, bot)

        raw_action = trade_data.get('action')
        if not raw_action:
            return
        action = str(raw_action).upper()

        live_entry = trade_data.get('live_entry') or trade_data.get('entry', 0)
        tp_hits = trade_data.get('tp_hits', [])
        breakeven_active = trade_data.get('breakeven_active', False)

        # Check breakeven first (highest priority when active)
        if breakeven_active and live_entry:
            if (action == "BUY" and current_price <= live_entry) or \
               (action == "SELL" and current_price >= live_entry):
                await self.handle_breakeven_hit(message_id, trade_data, bot)
                return

        # Check Stop Loss
        if (action == "BUY" and current_price <= trade_data.get('sl_price', 0)) or \
           (action == "SELL" and current_price >= trade_data.get('sl_price', 0)):
            await self.handle_sl_hit(message_id, trade_data, current_price, bot)
            return

        # Check Take Profit levels
        tp_levels = ["tp1", "tp2", "tp3"]
        # Sort levels to ensure they are processed in order (TP1 -> TP2 -> TP3)
        for level in tp_levels:
            if level.upper() not in tp_hits:
                tp_price = trade_data.get(f'{level}_price')
                if tp_price and ((action == "BUY" and current_price >= tp_price) or \
                   (action == "SELL" and current_price <= tp_price)):
                    await self.handle_tp_hit(message_id, trade_data, level.upper(), current_price, bot)
                    # Break after first new TP hit to allow sequential processing in next cycle
                    # or continue if we want to allow multiple TP hits in one go
                    # We continue to allow catching multiple hits if price jumps
                    continue

    async def handle_tp_hit(self, message_id: str, trade_data: dict, tp_level: str, hit_price: float, bot) -> None:
        """Process Take Profit hit"""
        trade = PRICE_TRACKING_CONFIG['active_trades'].get(message_id)
        if not trade:
            return
        
        if tp_level in trade.get('tp_hits', []):
            return
        
        trade['tp_hits'] = trade.get('tp_hits', []) + [tp_level]
        
        await self._send_tp_notification(message_id, trade, tp_level, hit_price, bot)
        
        # Activate breakeven after TP2 hit
        if tp_level == 'TP2' and not trade.get('breakeven_active'):
            trade['breakeven_active'] = True
        
        # Complete trade after TP3 hit
        if tp_level == 'TP3':
            trade['status'] = 'completed'
            await self._remove_completed_trade(message_id, bot)
        else:
            # Update database with TP hit
            await self._update_trade_in_db(message_id, trade, bot)
        
        if hasattr(bot, 'log_to_debug'):
            await bot.log_to_debug(f"✅ **TP Hit Detected**: {trade['pair']} {trade['action']} hit {tp_level} @ {hit_price:.5f}")

    async def handle_sl_hit(self, message_id: str, trade_data: dict, hit_price: float, bot) -> None:
        """Process Stop Loss hit"""
        trade = PRICE_TRACKING_CONFIG['active_trades'].get(message_id)
        if not trade:
            return
        
        trade['status'] = 'sl_hit'
        await self._send_sl_notification(message_id, trade, hit_price, bot)
        
        await self._remove_completed_trade(message_id, bot)
        
        if hasattr(bot, 'log_to_debug'):
            await bot.log_to_debug(f"❌ **SL Hit Detected**: {trade['pair']} {trade['action']} hit SL @ {hit_price:.5f}")

    async def handle_breakeven_hit(self, message_id: str, trade_data: dict, bot) -> None:
        """Process Breakeven hit"""
        trade = PRICE_TRACKING_CONFIG['active_trades'].get(message_id)
        if not trade:
            return
        
        trade['status'] = 'breakeven'
        
        pair = trade.get('pair', 'Unknown')
        action = trade.get('action', 'Unknown')
        live_entry = trade.get('live_entry') or trade.get('entry') or trade.get('entry_price', 0)
        tp_hits = trade.get('tp_hits', [])
        
        tp_status = f"TPs hit: {', '.join(tp_hits)}" if tp_hits else ""
        
        notification = (f"**BREAKEVEN HIT** {pair} {action}\n\n"
                        f"Price returned to entry ({live_entry:.5f})\n"
                        f"{tp_status}\n"
                        f"Trade closed at breakeven.")
        
        await self._send_notification(message_id, trade, notification, bot)
        
        await self._remove_completed_trade(message_id, bot)
        
        if hasattr(bot, 'log_to_debug'):
            await bot.log_to_debug(f"🔄 **Breakeven Hit**: {trade['pair']} {trade['action']} @ {live_entry:.5f}")

    async def _send_tp_notification(self, message_id: str, trade_data: dict, tp_level: str, hit_price: float, bot) -> None:
        """Send Take Profit hit notification with randomized messages"""
        tp1_messages = [
            "TP1 has been hit. First target secured 📈🔥",
            "TP1 smashed. Secure profits and aim for TP2 🎯💪",
            "TP1 hit! Keep eyes on TP2 👀💸",
            "First target hit. Clean start 📉➡️📈",
            "TP1 locked in. Push for TP2 💰📍",
            "TP1 reached. Trade moving as planned 🔄📊"
        ]
        
        tp2_messages = [
            "TP1 & TP2 hit! Move SL to breakeven, get TP3 💸",
            "TP2 hit! Move SL to breakeven, let's get TP3 🚀",
            "TP2 secured. Shift SL to breakeven, target TP3 📈🔥",
            "TP2 hit. Lock in gains, push for TP3 🎯🔒",
            "Another level cleared! TP2 hit, eyes on TP3 🧠🎯"
        ]
        
        tp3_messages = [
            "TP3 hit. Full target smashed 🔥🔥🔥",
            "TP3 reached. Perfect execution 📊🚀",
            "TP3 hit. Lock in profits 💸🎯",
            "Final target hit. Huge win 🔥💸",
            "TP3 secured. Result of following plan 💼💎"
        ]
        
        if tp_level.lower() == "tp1":
            notification = random.choice(tp1_messages)
        elif tp_level.lower() == "tp2":
            notification = random.choice(tp2_messages)
        elif tp_level.lower() == "tp3":
            notification = random.choice(tp3_messages)
        else:
            notification = f"**{tp_level.upper()} HAS BEEN HIT!** 🎯"
        
        await self._send_notification(message_id, trade_data, notification, bot)

    async def _send_sl_notification(self, message_id: str, trade_data: dict, hit_price: float, bot) -> None:
        """Send Stop Loss hit notification with randomized messages"""
        sl_messages = [
            "This one hit SL. Stay focused 🔄🧠",
            "SL hit. Risk managed, we move on 💪📉",
            "SL hit. Part of the process 💼📚",
            "SL triggered. Risk management at work 💪⚡",
            "SL hit. We bounce back 📈⏭️"
        ]
        
        notification = random.choice(sl_messages)
        await self._send_notification(message_id, trade_data, notification, bot)

    async def _send_notification(self, message_id: str, trade_data: dict, notification: str, bot) -> None:
        """Send notification to trade's original channel"""
        chat_id = trade_data.get('chat_id') or trade_data.get('channel_id')
        if not chat_id:
            logger.error(f"No chat_id for trade {message_id}")
            return
        
        original_msg_id = trade_data.get('message_id', message_id)
        if '_' in str(original_msg_id):
            original_msg_id = str(original_msg_id).split('_', 1)[1]
        
        try:
            await bot.send_message(chat_id, notification, reply_to_message_id=int(original_msg_id))
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
            try:
                await bot.send_message(chat_id, notification)
            except Exception as e2:
                logger.error(f"Failed to send notification without reply: {e2}")

    async def check_message_still_exists(self, message_id: str, trade_data: dict, bot) -> bool:
        """Verify original signal message still exists in Telegram"""
        try:
            chat_id_int = trade_data.get('chat_id') or trade_data.get('channel_id')
            if not chat_id_int:
                return True
            
            if '_' in str(message_id):
                actual_msg_id = int(str(message_id).split('_', 1)[1])
            else:
                actual_msg_id = int(message_id) if str(message_id).isdigit() else None
            
            if actual_msg_id is None:
                return True
            
            try:
                message = await asyncio.wait_for(
                    bot.get_messages(chat_id_int, actual_msg_id),
                    timeout=10
                )
                return message is not None
            except asyncio.TimeoutError:
                logger.warning(f"Timeout checking message {message_id}. Assuming exists.")
                return True
                
        except Exception as e:
            error_str = str(e).lower()
            if "not found" in error_str or "message_id_invalid" in error_str or "message deleted" in error_str:
                logger.info(f"Message {message_id} verified deleted: {e}")
                return False
            logger.warning(f"Unexpected error checking message {message_id}: {e}. Assuming exists.")
            return True

    async def verify_trade_data_consistency(self, message_id: str, trade_data: dict, bot) -> dict:
        """Sync trade data between memory and database"""
        try:
            if not bot or not bot.db_pool:
                return trade_data
                
            async with bot.db_pool.acquire() as conn:
                db_trade = await conn.fetchrow(
                    'SELECT * FROM active_trades WHERE message_id = $1',
                    message_id)
                
                if not db_trade:
                    logger.warning(f"Trade {message_id} not in DB")
                    return trade_data
                
                synced_data = dict(trade_data)
                synced_data['status'] = db_trade.get('status', trade_data.get('status', 'active'))
                
                tp_hits_str = db_trade.get('tp_hits', '')
                synced_data['tp_hits'] = [h.strip() for h in tp_hits_str.split(',') if h.strip()]
                
                synced_data['breakeven_active'] = db_trade.get('breakeven_active', False)
                synced_data['manual_overrides'] = db_trade.get('manual_overrides', '')
                
                if synced_data.get('tp_hits') != trade_data.get('tp_hits', []):
                    logger.info(f"Trade {message_id}: TP hits synced from DB")
                    trade_data['tp_hits'] = synced_data['tp_hits']
                
                if synced_data.get('breakeven_active') != trade_data.get('breakeven_active'):
                    logger.info(f"Trade {message_id}: Breakeven synced from DB")
                    trade_data['breakeven_active'] = synced_data['breakeven_active']
                    
                if synced_data.get('status') != trade_data.get('status'):
                    logger.info(f"Trade {message_id}: Status synced from DB")
                    trade_data['status'] = synced_data['status']
                
                return trade_data
                
        except Exception as e:
            logger.error(f"Error verifying trade consistency {message_id}: {e}")
            return trade_data

    async def _update_trade_in_db(self, message_id: str, trade: dict, bot) -> None:
        """Update trade status in database"""
        if not bot or not bot.db_pool:
            return
        
        try:
            async with bot.db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE active_trades SET tp_hits = $1, breakeven_active = $2 WHERE message_id = $3", 
                    ",".join(trade.get('tp_hits', [])), 
                    trade.get('breakeven_active', False), 
                    message_id
                )
        except Exception as e:
            logger.error(f"Error updating trade in DB: {e}")

    async def _remove_completed_trade(self, message_id: str, bot) -> None:
        """Remove completed trade from tracking"""
        if message_id in PRICE_TRACKING_CONFIG['active_trades']:
            del PRICE_TRACKING_CONFIG['active_trades'][message_id]
        
        if not bot or not bot.db_pool:
            return
        
        try:
            async with bot.db_pool.acquire() as conn:
                await conn.execute("DELETE FROM active_trades WHERE message_id = $1", message_id)
        except Exception as e:
            logger.error(f"Error removing trade from DB: {e}")

    def validate_chronological_hits(self, hits: List[Dict]) -> List[Dict]:
        """Validate hits chronologically according to trading rules"""
        valid_hits = []
        sl_hit = False
        tp_levels_hit = set()
        
        for hit in sorted(hits, key=lambda x: x.get('hit_time', 0)):
            hit_type = hit.get('hit_type')
            hit_level = hit.get('hit_level')
            
            # SL prevents any subsequent TP hits
            if sl_hit and hit_type == 'tp':
                continue
            
            # SL can't happen after TP2
            if hit_type == 'sl' and 'TP2' in tp_levels_hit:
                continue
            
            # SL can't happen after TP3
            if hit_type == 'sl' and 'TP3' in tp_levels_hit:
                continue
            
            # TP3 can't happen after SL
            if hit_type == 'tp' and hit_level == 'TP3' and sl_hit:
                continue
            
            valid_hits.append(hit)
            
            if hit_type == 'sl':
                sl_hit = True
            elif hit_type == 'tp':
                tp_levels_hit.add(hit_level)
        
        return valid_hits
