from datetime import datetime, timedelta
import pytz
import logging
from src.features.core.config import AMSTERDAM_TZ, AUTO_ROLE_CONFIG, VIP_GROUP_ID, MESSAGE_TEMPLATES

logger = logging.getLogger(__name__)

class TrialManager:
    def __init__(self, db_manager, bot_instance):
        self.db = db_manager
        self.bot = bot_instance

    def is_weekend_time(self, check_time: datetime) -> bool:
        """Check if given time falls on a weekend (Friday 22:00 to Sunday 22:00 Amsterdam)"""
        if check_time.tzinfo != AMSTERDAM_TZ:
            check_time = check_time.astimezone(AMSTERDAM_TZ)
            
        weekday = check_time.weekday()  # 0=Mon, 4=Fri, 5=Sat, 6=Sun
        hour = check_time.hour
        
        if weekday == 4 and hour >= 22:
            return True
        if weekday == 5:
            return True
        if weekday == 6 and hour < 22:
            return True
            
        return False

    def calculate_trial_expiry_time(self, join_time: datetime) -> datetime:
        """Calculate trial expiry guaranteeing 3 trading days (Mon-Fri)"""
        if join_time.tzinfo is None:
            join_time = AMSTERDAM_TZ.localize(join_time)
        elif join_time.tzinfo != AMSTERDAM_TZ:
            join_time = join_time.astimezone(AMSTERDAM_TZ)
            
        weekday = join_time.weekday()
        
        if self.is_weekend_time(join_time):
            # Friday 22:00 or later, or Saturday/Sunday
            # Trial starts Monday morning, ends Wednesday night
            days_to_monday = (0 - weekday) % 7
            start_monday = (join_time + timedelta(days=days_to_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
            expiry = (start_monday + timedelta(days=2)).replace(hour=22, minute=59, second=0, microsecond=0)
            return expiry
            
        # Normal weekday activation
        expiry = join_time + timedelta(days=3)
        # Adjust for weekend overlap
        # If it crosses Friday 22:00, add 2 days
        temp_expiry = join_time + timedelta(days=3)
        # Check if any part of the 3 days falls on weekend
        current = join_time
        added_days = 0
        while added_days < 3:
            current += timedelta(days=1)
            if current.weekday() >= 5: # Sat or Sun
                pass # Don't count weekend days
            else:
                added_days += 1
        
        expiry = current.replace(hour=22, minute=59, second=0, microsecond=0)
        return expiry

    async def register_trial(self, user, current_time: datetime):
        """Register a user for a VIP trial"""
        user_id_str = str(user.id)
        expiry_time = self.calculate_trial_expiry_time(current_time)
        is_weekend = self.is_weekend_time(current_time)
        
        AUTO_ROLE_CONFIG['active_members'][user_id_str] = {
            'joined_at': current_time.isoformat(),
            'expiry_time': expiry_time.isoformat(),
            'weekend_delayed': is_weekend,
            'chat_id': VIP_GROUP_ID
        }
        
        AUTO_ROLE_CONFIG['role_history'][user_id_str] = {
            'first_granted': current_time.isoformat(),
            'times_granted': 1,
            'last_expired': None
        }
        
        if self.db:
            try:
                async with self.db.pool.acquire() as conn:
                    await conn.execute(
                        '''INSERT INTO active_members (member_id, role_added_time, expiry_time, weekend_delayed)
                           VALUES ($1, $2, $3, $4)
                           ON CONFLICT (member_id) DO UPDATE SET 
                           role_added_time = $2, expiry_time = $3, weekend_delayed = $4''',
                        user.id, current_time, expiry_time, is_weekend)
                    
                    await conn.execute(
                        '''INSERT INTO role_history (member_id, first_granted, times_granted, last_expired)
                           VALUES ($1, $2, 1, NULL)
                           ON CONFLICT (member_id) DO UPDATE SET times_granted = role_history.times_granted + 1''',
                        user.id, current_time)
            except Exception as e:
                logger.error(f"Error saving trial registration to DB: {e}")
        
        return expiry_time, is_weekend
