import os
import asyncio
import logging
import sys
import warnings

# Suppress deprecated warnings for Python 3.14+
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Windows compatibility fix
if sys.platform == 'win32':
    if sys.version_info < (3, 12):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass

# Create the loop before importing pyrogram
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

import asyncpg
import ssl
from datetime import datetime, timedelta
import pytz
import random
from pyrogram.client import Client
from pyrogram.errors import FloodWait, PeerIdInvalid, UserPrivacyRestricted

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("UserbotService")

# Configuration from Environment Variables
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
DATABASE_URL_ENV = os.getenv("DATABASE_URL_OVERRIDE") or DATABASE_URL

DEBUG_GROUP_ID = int(os.getenv("DEBUG_GROUP_ID", "0"))
AMSTERDAM_TZ = pytz.timezone('Europe/Amsterdam')

class UserbotService:
    def __init__(self):
        self.client = None
        self.db_pool = None
        self.running = True

    async def init_db(self):
        try:
            url = DATABASE_URL_ENV
            if not url and os.getenv("DATABASE_URL"):
                url = os.getenv("DATABASE_URL")
                
            if not url:
                logger.error("No DATABASE_URL found in environment")
                return

            # Standard SSL configuration for Render PostgreSQL
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

            # Try up to 5 times to connect to the database
            for attempt in range(5):
                try:
                    pool = await asyncpg.create_pool(
                        url,
                        min_size=1,
                        max_size=5,
                        command_timeout=60,
                        ssl=ctx
                    )
                    self.db_pool = pool
                    logger.info("Database connected successfully")
                    break
                except Exception as e:
                    if attempt == 4:
                        raise
                    wait_time = (attempt + 1) * 5
                    logger.warning(f"Database connection attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
        
        if self.db_pool:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS bot_settings (
                        setting_key VARCHAR(100) PRIMARY KEY,
                        setting_value TEXT NOT NULL
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS userbot_dm_queue (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        message_text TEXT NOT NULL,
                        label TEXT NOT NULL,
                        status TEXT DEFAULT 'pending',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        sent_at TIMESTAMP WITH TIME ZONE
                    )
                """)

    async def log_to_debug(self, message: str):
        if not self.client or not self.client.is_connected:
            logger.info(f"DEBUG_LOG (Client not ready): {message}")
            return
            
        try:
            await self.client.send_message(DEBUG_GROUP_ID, f"🤖 **Userbot Service**: {message}")
        except Exception as e:
            logger.error(f"Failed to log to debug group via userbot: {e}")
            logger.info(f"DEBUG_LOG: {message}")

    async def start(self):
        await self.init_db()
        
        if not self.db_pool:
            logger.error("Database pool not initialized. Cannot start service.")
            return

        # Notify startup to debug group
        await self.log_to_debug("🚀 **Userbot Service**: Initiating startup sequence...")

        while self.running:
            async with self.db_pool.acquire() as conn:
                session_string = await conn.fetchval(
                    "SELECT setting_value FROM bot_settings WHERE setting_key = 'userbot_session_string'"
                )
            
            if session_string:
                break
                
            logger.info("Waiting for userbot session string in database...")
            await asyncio.sleep(30)
        
        if not self.running:
            return

        self.client = Client(
            "userbot_service",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            session_string=session_string,
            device_model="PC 64bit",
            system_version="Linux 6.8.0-1043-aws",
            app_version="2.1.0",
            lang_code="en",
            no_updates=False,
        )
        
        await self.client.start()
        logger.info("Userbot Service Started")
        await self.log_to_debug("✅ Userbot Service Started and Connected")
        
        await asyncio.gather(
            self.dm_loop(),
            self.peer_discovery_loop()
        )

    async def peer_discovery_loop(self):
        """Ensures the userbot 'sees' users to establish Peer IDs."""
        while self.running:
            try:
                if self.client and self.client.is_connected:
                    logger.info("Peer discovery heartbeat - checking for new members")
                    # Optionally fetch recent members from tracked groups
                    for group_id in [FREE_GROUP_ID, VIP_GROUP_ID]:
                        if group_id != 0:
                            async for member in self.client.get_chat_members(group_id, limit=20):
                                pass # This caches the peer in pyrogram's memory
                await asyncio.sleep(600) # Check every 10 mins instead of 1 hour
            except Exception as e:
                logger.error(f"Error in peer discovery: {e}")
                await asyncio.sleep(60)

    async def send_dm(self, user_id: int, message: str, label: str):
        if not self.client or not self.client.is_connected:
            logger.error(f"Cannot send DM {label} to {user_id}: Client not connected")
            return False
            
        try:
            await asyncio.sleep(random.randint(5, 15))
            await self.client.send_message(user_id, message)
            await self.log_to_debug(f"✅ Sent {label} to {user_id}")
            return True
        except UserPrivacyRestricted:
            await self.log_to_debug(f"❌ Privacy Violation: Cannot message {user_id} ({label})")
            return False
        except PeerIdInvalid:
            await self.log_to_debug(f"❌ Peer ID Invalid: {user_id} ({label})")
            return False
        except FloodWait as e:
            wait_time = float(e.value) if hasattr(e, 'value') and e.value else 60
            logger.warning(f"FloodWait: Waiting {wait_time}s")
            await asyncio.sleep(wait_time)
            return await self.send_dm(user_id, message, label)
        except Exception as e:
            await self.log_to_debug(f"❌ Failed to send {label} to {user_id}: {e}")
            return False

    async def dm_loop(self):
        while self.running:
            try:
                if not self.db_pool:
                    await asyncio.sleep(10)
                    continue
                    
                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)
                
                async with self.db_pool.acquire() as conn:
                    # 1. Check for pending Welcome DMs - DEPRECATED (Now handled via userbot_dm_queue)
                    # welcome_pending = await conn.fetch("""
                    #     SELECT user_id FROM peer_id_checks 
                    #     WHERE NOT welcome_dm_sent AND peer_id_established
                    # """)
                    # for row in welcome_pending:
                    #     ... (Logic removed to centralize in queue)

                    # 2. Handle Trial Expiry Warnings (24h and 3h)
                    active_members = await conn.fetch("SELECT member_id, expiry_time FROM active_members")
                    for member in active_members:
                        member_id = member['member_id']
                        expiry_time = member['expiry_time']
                        if expiry_time.tzinfo is None:
                            expiry_time = AMSTERDAM_TZ.localize(expiry_time)
                        
                        time_left = expiry_time - current_time
                        hours_left = time_left.total_seconds() / 3600

                        # 24h Warning
                        if 23.5 <= hours_left <= 24.5:
                            sent_check = await conn.fetchval("SELECT id FROM userbot_dm_queue WHERE user_id = $1 AND label = '24h Warning' AND created_at > $2", member_id, current_time - timedelta(days=1))
                            if not sent_check:
                                msg = "**REMINDER! Your 3-day free trial for our VIP Group will expire in 24 hours**.\n\nAfter that, you'll unfortunately lose access to the VIP Group. You've had great opportunities during these past 2 days. Don't let this last day slip away!"
                                await conn.execute("INSERT INTO userbot_dm_queue (user_id, message, label, status) VALUES ($1, $2, '24h Warning', 'pending')", member_id, msg)

                        # 3h Warning
                        if 2.5 <= hours_left <= 3.5:
                            sent_check = await conn.fetchval("SELECT id FROM userbot_dm_queue WHERE user_id = $1 AND label = '3h Warning' AND created_at > $2", member_id, current_time - timedelta(days=1))
                            if not sent_check:
                                msg = "**FINAL REMINDER! Your 3-day free trial for our VIP Group will expire in just 3 hours**.\n\nYou're about to lose access to our VIP Group and the 6+ daily trade signals and opportunities it comes with. Upgrade to VIP to keep your access: https://whop.com/gold-pioneer/gold-pioneer/"
                                await conn.execute("INSERT INTO userbot_dm_queue (user_id, message, label, status) VALUES ($1, $2, '3h Warning', 'pending')", member_id, msg)

                    # 2.1 Handle Trial Expiration (Userbot handles the DM)
                    expired_members = await conn.fetch("""
                        SELECT member_id FROM active_members 
                        WHERE expiry_time <= $1
                    """, current_time)
                    for member in expired_members:
                        member_id = member['member_id']
                        sent_check = await conn.fetchval("SELECT id FROM userbot_dm_queue WHERE user_id = $1 AND label = 'Trial Expired' AND created_at > $2", member_id, current_time - timedelta(hours=1))
                        if not sent_check:
                            msg = "Hey! Your **3-day free access** to the VIP Group has unfortunately **ran out**. We truly hope you were able to benefit with us & we hope to see you back soon!"
                            await conn.execute("INSERT INTO userbot_dm_queue (user_id, message, label, status) VALUES ($1, $2, 'Trial Expired', 'pending')", member_id, msg)

                    # 3. Handle Retention Follow-ups (3, 7, 14 days)
                    followups = await conn.fetch("SELECT member_id, role_expired, dm_3_sent, dm_7_sent, dm_14_sent FROM dm_schedule")
                    for f in followups:
                        m_id = f['member_id']
                        expired_at = f['role_expired']
                        if expired_at.tzinfo is None:
                            expired_at = AMSTERDAM_TZ.localize(expired_at)
                        
                        days_since = (current_time - expired_at).days
                        
                        if days_since >= 3 and not f['dm_3_sent']:
                            msg = "Hey! It's been 3 days since your trial ended. We'd love to invite you back! Join again: https://whop.com/gold-pioneer/gold-pioneer/"
                            if await self.send_dm(m_id, msg, "3-Day Follow-up"):
                                await conn.execute("UPDATE dm_schedule SET dm_3_sent = TRUE WHERE member_id = $1", m_id)
                        
                        if days_since >= 7 and not f['dm_7_sent']:
                            msg = "It's been a week since your trial ended. VIP members are catching setups daily! Join: https://whop.com/gold-pioneer/gold-pioneer/"
                            if await self.send_dm(m_id, msg, "7-Day Follow-up"):
                                await conn.execute("UPDATE dm_schedule SET dm_7_sent = TRUE WHERE member_id = $1", m_id)

                        if days_since >= 14 and not f['dm_14_sent']:
                            msg = "It's been two weeks! Don't lose your edge. Rejoin VIP: https://whop.com/gold-pioneer/gold-pioneer/"
                            if await self.send_dm(m_id, msg, "14-Day Follow-up"):
                                await conn.execute("UPDATE dm_schedule SET dm_14_sent = TRUE WHERE member_id = $1", m_id)

                    # 4. Handle Engagement Tracking (5+ reactions after 2 weeks)
                    joins = await conn.fetch('SELECT user_id, joined_at, discount_sent FROM free_group_joins WHERE discount_sent = FALSE')
                    for j in joins:
                        two_weeks_after = j['joined_at'] + timedelta(days=14)
                        if current_time >= two_weeks_after:
                            reaction_count = await conn.fetchval('SELECT COUNT(DISTINCT message_id) FROM emoji_reactions WHERE user_id = $1 AND reaction_time > $2', j['user_id'], two_weeks_after)
                            if reaction_count >= 5:
                                msg = "Hey! 👋 We noticed that you've been engaging with our signals in the Free Group. We want to say that we truly appreciate it!\n\n**Your exclusive discount code is:** `Thank_You!50!`"
                                if await self.send_dm(j['user_id'], msg, "Engagement Discount"):
                                    await conn.execute('UPDATE free_group_joins SET discount_sent = TRUE WHERE user_id = $1', j['user_id'])

                    # 5. Handle Monday Activations
                    if current_time.weekday() == 0 and current_time.hour <= 1:
                        delayed = await conn.fetch("SELECT member_id FROM active_members WHERE weekend_delayed = TRUE AND NOT monday_notification_sent")
                        for d in delayed:
                            msg = "Hey! The weekend is over, so the trading markets have been opened again. That means your 3-day welcome gift has officially started."
                            if await self.send_dm(d['member_id'], msg, "Monday Activation"):
                                await conn.execute("UPDATE active_members SET monday_notification_sent = TRUE WHERE member_id = $1", d['member_id'])

                    # 6. Check for queued DMs from main bot
                    queued_dms = await conn.fetch("""
                        SELECT id, user_id, message_text, label, created_at FROM userbot_dm_queue 
                        WHERE status = 'pending'
                        ORDER BY created_at ASC LIMIT 10
                    """)
                    for row in queued_dms:
                        # 10 minute delay specifically for Welcome DMs
                        if row['label'] == 'Welcome DM':
                            join_time = row['created_at']
                            if join_time.tzinfo is None:
                                join_time = AMSTERDAM_TZ.localize(join_time)
                            
                            if current_time < join_time + timedelta(minutes=10):
                                continue # Wait until 10 mins have passed

                        if await self.send_dm(row['user_id'], row['message_text'], row['label']):
                            await conn.execute("UPDATE userbot_dm_queue SET status = 'sent', sent_at = $1 WHERE id = $2", current_time, row['id'])
                        else:
                            await conn.execute("UPDATE userbot_dm_queue SET status = 'failed' WHERE id = $2", row['id'])
                
                await asyncio.sleep(60)
            except Exception as e:
                error_msg = f"❌ Error in DM loop: {e}"
                logger.error(error_msg)
                await self.log_to_debug(error_msg)
                await asyncio.sleep(30)

if __name__ == "__main__":
    service = UserbotService()
    try:
        asyncio.run(service.start())
    except KeyboardInterrupt:
        service.running = False
