import asyncpg
import logging
import os
import sys
from datetime import datetime, timezone

# Ensure project root is in PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from typing import Dict
from src.features.core.config import DATABASE_URL
from src.features.database.schema import SCHEMA

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, bot_instance=None):
        self.pool = None
        self.bot = bot_instance

    async def log_to_debug(self, message: str, is_error: bool = False, user_id: int | None = None):
        if hasattr(self, 'bot') and self.bot:
            await self.bot.log_to_debug(message, is_error, user_id)
        else:
            logger.info(f"DB Log (No bot): {message}")

    async def connect(self):
        db_url = os.environ.get("DATABASE_URL")
        
        if not db_url:
            user = os.environ.get("PGUSER")
            password = os.environ.get("PGPASSWORD")
            host = os.environ.get("PGHOST")
            port = os.environ.get("PGPORT")
            database = os.environ.get("PGDATABASE")
            
            if all([user, password, host, port, database]):
                db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        if not db_url:
            logger.error("DATABASE_URL is missing. Please ensure database is integrated.")
            db_url = "postgresql://localhost:5432/postgres"

        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)

        logger.info(f"Connecting to database...")
        try:
            # Added connection timeout and pooling optimizations for Render/Neon
            self.pool = await asyncpg.create_pool(
                db_url, 
                ssl='prefer',
                min_size=1,
                max_size=10,
                command_timeout=60,
                max_inactive_connection_lifetime=300
            )
            async with self.pool.acquire() as conn:
                # Initialize all tables from schema
                for table_name, create_sql in SCHEMA.items():
                    try:
                        await conn.execute(create_sql)
                    except Exception as e:
                        logger.error(f"Failed to create table {table_name}: {e}")
                
                # Verify migrations for trial_offer_history
                try:
                    # Check if offered_at exists (from schema) or offer_sent_date exists (from daily_trial_offers.py)
                    # The daily_trial_offers.py uses offer_sent_date, but schema.py says offered_at.
                    # Standardizing to offered_at and adding an index.
                    cols = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = 'trial_offer_history'")
                    col_names = [r['column_name'] for r in cols]
                    if 'offer_sent_date' in col_names and 'offered_at' not in col_names:
                        await conn.execute("ALTER TABLE trial_offer_history RENAME COLUMN offer_sent_date TO offered_at")
                except Exception as e:
                    logger.error(f"Migration error: {e}")

                # Fresh Start Reset (Per User Request)
                # This clears trial and peer ID tracking tables for a fresh start on Monday
                try:
                    await conn.execute("TRUNCATE TABLE vip_trial_activations, peer_id_checks, active_members, role_history, dm_schedule, trial_offer_history, free_group_joins, emoji_reactions, pending_welcome_dms CASCADE")
                    logger.info("Fresh Start: Database tables cleared successfully for fresh start.")
                except Exception as e:
                    logger.error(f"Fresh Start Reset failed: {e}")
                
            logger.info("Database connected and full schema verified.")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            
    async def get_active_trades(self):
        if not self.pool: return []
        async with self.pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM active_trades WHERE status = 'active'")

    async def activate_trial(self, user_id: int, activation_date: datetime, expiry_date: datetime):
        if not self.pool: return
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO vip_trial_activations (user_id, activation_date, expiry_date)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id) DO UPDATE SET
                activation_date = $2, expiry_date = $3
            ''', user_id, activation_date, expiry_date)
            
            # Also update role history
            await conn.execute('''
                INSERT INTO role_history (member_id, first_granted, times_granted)
                VALUES ($1, $2, 1)
                ON CONFLICT (member_id) DO UPDATE SET
                times_granted = role_history.times_granted + 1
            ''', user_id, activation_date)

    async def get_role_history(self, user_id: int):
        if not self.pool: return None
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM role_history WHERE member_id = $1", user_id)

    async def add_pending_welcome_dm(self, user_id: int, first_name: str, message_type: str, content: str):
        if not self.pool: return
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO pending_welcome_dms (user_id, first_name, message_type, message_content)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO UPDATE SET
                message_type = $3, message_content = $4, last_attempt = NULL, failed_attempts = 0
            ''', user_id, first_name, message_type, content)

    async def get_pending_dms(self):
        if not self.pool: return []
        async with self.pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM pending_welcome_dms WHERE failed_attempts < 5")

    async def save_active_trade(self, trade: Dict):
        if not self.pool: return
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO active_trades (
                    message_id, channel_id, pair, action, entry_price, 
                    tp1_price, tp2_price, tp3_price, sl_price, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ''', str(trade.get('message_id', 'manual')), trade.get('channel_id', 0), 
                trade['pair'], trade['action'], trade['entry_price'],
                trade['tp1_price'], trade['tp2_price'], trade['tp3_price'], 
                trade['sl_price'], trade['status'])
