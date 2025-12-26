import asyncio
import logging
import asyncpg
from typing import List, Dict, Any, Optional
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        try:
            # Add ssl requirement for external neon database
            self.pool = await asyncpg.create_pool(
                self.db_url,
                min_size=5,
                max_size=20,
                command_timeout=60,
                ssl='require'
            )
            if self.pool is None:
                raise Exception("Failed to initialize database pool")
            logger.info("Successfully connected to the database")
            await self.initialize_tables()
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")

    async def check_connection(self) -> bool:
        if self.pool is None: return False
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")
                return True
        except Exception:
            return False

    async def get_pending_peer_checks(self) -> List[Dict[str, Any]]:
        if self.pool is None: return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM peer_id_checks 
                WHERE peer_id_established = FALSE 
                AND next_check_at <= NOW()
                AND joined_at > NOW() - INTERVAL '24 hours'
            """)
            return [dict(row) for row in rows]

    async def mark_welcome_sent(self, user_id: int):
        if self.pool is None: return
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE peer_id_checks SET welcome_dm_sent = TRUE WHERE user_id = $1", user_id)

    async def add_peer_id_check(self, user_id: int):
        if self.pool is None: return
        async with self.pool.acquire() as conn:
            # Escalation logic: 30m -> 1h -> 3h -> 24h
            # Initial check at 30 minutes
            next_check = datetime.now(pytz.UTC) + timedelta(minutes=30)
            await conn.execute('''
                INSERT INTO peer_id_checks (user_id, joined_at, current_delay_minutes, next_check_at)
                VALUES ($1, $2, 30, $3)
                ON CONFLICT (user_id) DO NOTHING
            ''', user_id, datetime.now(pytz.UTC), next_check)

    async def update_next_check(self, user_id: int, next_delay: int, next_check: datetime):
        if self.pool is None: return
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE peer_id_checks 
                SET current_delay_minutes = $2, next_check_at = $3 
                WHERE user_id = $1
            ''', user_id, next_delay, next_check)

    async def initialize_tables(self):
        if self.pool is None:
            return
        async with self.pool.acquire() as conn:
            # Create all necessary tables
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS role_history (
                    member_id BIGINT PRIMARY KEY,
                    first_granted TIMESTAMP WITH TIME ZONE,
                    times_granted INTEGER DEFAULT 1,
                    last_expired TIMESTAMP WITH TIME ZONE,
                    guild_id BIGINT
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS active_members (
                    member_id BIGINT PRIMARY KEY,
                    role_added_time TIMESTAMP WITH TIME ZONE,
                    role_id BIGINT,
                    guild_id BIGINT,
                    weekend_delayed BOOLEAN DEFAULT FALSE,
                    expiry_time TIMESTAMP WITH TIME ZONE
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS active_trades (
                    message_id TEXT PRIMARY KEY,
                    trade_key TEXT,
                    chat_id BIGINT,
                    pair TEXT,
                    action TEXT,
                    entry_price FLOAT,
                    tp1_price FLOAT,
                    tp2_price FLOAT,
                    tp3_price FLOAT,
                    sl_price FLOAT,
                    status TEXT DEFAULT 'active',
                    assigned_api TEXT,
                    created_at TIMESTAMP WITH TIME ZONE,
                    group_name TEXT,
                    breakeven_active BOOLEAN DEFAULT FALSE,
                    manual_tracking_only BOOLEAN DEFAULT FALSE
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS peer_id_checks (
                    user_id BIGINT PRIMARY KEY,
                    joined_at TIMESTAMP WITH TIME ZONE,
                    current_delay_minutes INTEGER,
                    current_interval_minutes INTEGER,
                    next_check_at TIMESTAMP WITH TIME ZONE,
                    peer_id_established BOOLEAN DEFAULT FALSE,
                    established_at TIMESTAMP WITH TIME ZONE,
                    welcome_dm_sent BOOLEAN DEFAULT FALSE
                )
            ''')

    async def get_active_trades(self) -> List[Dict[str, Any]]:
        if self.pool is None:
            return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM active_trades WHERE status = 'active'")
            return [dict(row) for row in rows]

    async def update_trade_in_db(self, message_id: str, trade_data: Dict[str, Any]):
        if self.pool is None: return
        async with self.pool.acquire() as conn:
            tp_hits_str = ','.join(trade_data.get('tp_hits', []))
            manual_overrides_str = ','.join(trade_data.get('manual_overrides', []))
            await conn.execute('''
                UPDATE active_trades 
                SET status = $2, tp_hits = $3, breakeven_active = $4, 
                    manual_overrides = $5, live_entry = $6, last_updated = NOW()
                WHERE message_id = $1
            ''', message_id, trade_data.get('status', 'active'), tp_hits_str,
                trade_data.get('breakeven_active', False),
                manual_overrides_str, trade_data.get('live_entry'))

    async def archive_trade_to_completed(self, message_id: str, trade_data: Dict[str, Any], completion_reason: str):
        if self.pool is None: return
        async with self.pool.acquire() as conn:
            tp_hits_str = ','.join(trade_data.get('tp_hits', []))
            manual_overrides_str = ','.join(trade_data.get('manual_overrides', []))
            created_at = trade_data.get('created_at')
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            
            await conn.execute('''
                INSERT INTO completed_trades 
                (message_id, channel_id, guild_id, pair, action, entry_price, tp1_price, tp2_price, tp3_price, sl_price,
                 status, tp_hits, breakeven_active, entry_type, manual_overrides, created_at, completion_reason)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                ON CONFLICT (message_id) DO NOTHING
            ''', message_id, trade_data.get('chat_id', 0), trade_data.get('chat_id', 0), 
                trade_data.get('pair'), trade_data.get('action'),
                trade_data.get('entry_price') or trade_data.get('entry'),
                trade_data.get('tp1_price') or trade_data.get('tp1'),
                trade_data.get('tp2_price') or trade_data.get('tp2'),
                trade_data.get('tp3_price') or trade_data.get('tp3'),
                trade_data.get('sl_price') or trade_data.get('sl'),
                trade_data.get('status', 'completed'), tp_hits_str,
                trade_data.get('breakeven_active', False),
                trade_data.get('entry_type', 'execution'),
                manual_overrides_str, created_at, completion_reason)

    async def get_role_history(self, user_id: int) -> Optional[Dict[str, Any]]:
        if self.pool is None: return None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM role_history WHERE member_id = $1", user_id)
            return dict(row) if row else None

    async def log_engagement(self, user_id: int, message_id: int, emoji: str):
        if self.pool is None: return
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO emoji_reactions (user_id, message_id, emoji, reaction_time)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id, message_id, emoji) DO NOTHING
            ''', user_id, message_id, emoji, datetime.now(pytz.UTC))

    async def update_peer_id(self, user_id: int, established: bool):
        if self.pool is None: return
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE peer_id_checks 
                SET peer_id_established = $2, established_at = $3 
                WHERE user_id = $1
            ''', user_id, established, datetime.now(pytz.UTC) if established else None)

