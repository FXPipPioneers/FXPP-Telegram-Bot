import asyncio
import logging
from datetime import datetime, timedelta, timezone, timedelta
import pytz
from typing import Tuple, Optional
from src.features.core.config import AMSTERDAM_TZ, MESSAGE_TEMPLATES

logger = logging.getLogger(__name__)

class PeerIDEscalationLoop:
    """Background loop: escalate peer ID checks until established or 24 hours passed"""
    
    def __init__(self, app, db_pool, bot_instance):
        self.app = app
        self.db_pool = db_pool
        self.bot = bot_instance
    
    async def escalate_peer_id_check(self, delay_level: int) -> Tuple[int, int]:
        """Get next delay and interval based on escalation level.
        Level 0: 30 min delay, 3 min interval
        Level 1: 1 hour delay, 10 min interval  
        Level 2: 3 hour delay, 20 min interval
        Level 3+: 24 hour limit, then give up
        """
        escalation = [
            (30, 3),    # Level 0: 30 min delay, check every 3 min
            (60, 10),   # Level 1: 1 hour delay, check every 10 min
            (180, 20),  # Level 2: 3 hours delay, check every 20 min
        ]
        if delay_level < len(escalation):
            return escalation[delay_level]
        return (1440, 20)  # Level 3+: 24 hours (give up after this)
    
    async def check_peer_id_established(self, user_id: int) -> bool:
        """Attempt to verify peer ID is established by checking if we can interact with the user"""
        try:
            await self.app.get_users([user_id])
            return True
        except Exception:
            return False
    
    async def run(self):
        """Background loop: escalate peer ID checks until established or 24 hours passed"""
        await asyncio.sleep(30)
        
        while getattr(self.bot, 'running', True):
            try:
                if not self.db_pool:
                    await asyncio.sleep(60)
                    continue
                
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                
                async with self.db_pool.acquire() as conn:
                    # Get all pending peer ID checks
                    pending = await conn.fetch('''
                        SELECT user_id, joined_at, peer_id_established, current_delay_minutes, 
                               current_interval_minutes, next_check_at
                        FROM peer_id_checks 
                        WHERE NOT peer_id_established AND welcome_dm_sent = FALSE
                        ORDER BY next_check_at ASC
                    ''')
                    
                    # Only log if there are actually pending checks
                    if pending:
                        logger.info(f"Peer ID Escalation Loop: Found {len(pending)} pending checks")
                    
                    for row in pending:
                        user_id = row['user_id']
                        joined_at = row['joined_at']
                        if joined_at.tzinfo is None:
                            joined_at = AMSTERDAM_TZ.localize(joined_at)
                        next_check_at = row['next_check_at']
                        if next_check_at.tzinfo is None:
                            next_check_at = AMSTERDAM_TZ.localize(next_check_at)
                        
                        # Check if it's time to do a peer ID check
                        if current_time >= next_check_at:
                            time_elapsed = (current_time - joined_at).total_seconds() / 3600
                            
                            # Give up after 24 hours
                            if time_elapsed > 24:
                                await conn.execute('''
                                    UPDATE peer_id_checks SET peer_id_established = FALSE 
                                    WHERE user_id = $1
                                ''', user_id)
                                logger.warning(f"Peer ID check gave up for user {user_id} after 24 hours")
                                continue
                            
                            # Try peer ID check
                            if await self.check_peer_id_established(user_id):
                                # Peer ID established - update database and send welcome DM
                                await conn.execute('''
                                    UPDATE peer_id_checks SET peer_id_established = TRUE, established_at = $1
                                    WHERE user_id = $2
                                ''', current_time, user_id)
                                
                                try:
                                    user_data = await self.app.get_users([user_id])
                                    first_name = user_data[0].first_name if user_data else "Trader"
                                    welcome_dm = MESSAGE_TEMPLATES["Engagement & Offers"]["Welcome (Free Group)"]["message"]
                                    await self.app.send_message(user_id, welcome_dm)
                                    await conn.execute('UPDATE peer_id_checks SET welcome_dm_sent = TRUE WHERE user_id = $1', user_id)
                                    await self.bot.log_to_debug(
                                        f"✅ Welcome DM successfully sent to {first_name} (ID: {user_id}) - "
                                        f"Peer ID established after {time_elapsed:.1f} hours",
                                        user_id=user_id
                                    )
                                except Exception as e:
                                    error_msg = str(e)
                                    # Handle PEER_ID_INVALID: Reset peer_id_established so system can retry
                                    if "PEER_ID_INVALID" in error_msg:
                                        await conn.execute('''
                                            UPDATE peer_id_checks SET peer_id_established = FALSE
                                            WHERE user_id = $1
                                        ''', user_id)
                                        await self.bot.log_to_debug(
                                            f"⚠️ Peer ID became invalid for user {user_id}. Resetting to attempt recovery.",
                                            is_error=True,
                                            user_id=user_id
                                        )
                                    else:
                                        await self.bot.log_to_debug(
                                            f"❌ Peer ID established for user {user_id} but welcome DM failed: {e}",
                                            is_error=True,
                                            user_id=user_id
                                        )
                            else:
                                # Still not established - schedule next check based on delay progression
                                delay_mins = row['current_delay_minutes']
                                interval_mins = row['current_interval_minutes']
                                
                                # Log failure at each interval (internal only, don't spam Telegram)
                                logger.info(
                                    f"Peer ID check failed for user {user_id} (Joined {time_elapsed:.1f}h ago). "
                                    f"Next check in {interval_mins}m."
                                )
                                
                                # Calculate time since join
                                mins_since_join = (current_time - joined_at).total_seconds() / 60
                                
                                # Escalate if we've passed current delay threshold
                                if mins_since_join >= delay_mins:
                                    # Move to next escalation level
                                    next_delay, next_interval = await self.escalate_peer_id_check(
                                        [30, 60, 180].index(delay_mins) + 1 if delay_mins in [30, 60, 180] else 3
                                    )
                                    next_check = current_time + timedelta(minutes=next_interval)
                                    
                                    await conn.execute('''
                                        UPDATE peer_id_checks 
                                        SET current_delay_minutes = $1, current_interval_minutes = $2, next_check_at = $3
                                        WHERE user_id = $4
                                    ''', next_delay, next_interval, next_check, user_id)
                                    
                                    if next_delay == 1440:
                                        logger.warning(f"Peer ID check for user {user_id} escalated to 24-hour cycle (final attempt)")
                                else:
                                    # Same delay level, schedule next interval check
                                    next_check = current_time + timedelta(minutes=interval_mins)
                                    await conn.execute('''
                                        UPDATE peer_id_checks SET next_check_at = $1 WHERE user_id = $2
                                    ''', next_check, user_id)
                
                await asyncio.sleep(10)  # Check every 10 seconds for due checks
                
            except Exception as e:
                logger.error(f"Error in peer_id_escalation_loop: {e}")
                await asyncio.sleep(60)
