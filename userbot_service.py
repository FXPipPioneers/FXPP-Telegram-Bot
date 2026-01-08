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
from pyrogram import filters
from pyrogram.raw import functions, types
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

BOT_OWNER_USER_ID = int(os.getenv("BOT_OWNER_USER_ID") or "6664440870")

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
            # Using ssl=True or a more robust context for asyncpg
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            
            # Ensure the URL is properly formatted for asyncpg (replacing postgres:// with postgresql:// if needed)
            if url.startswith("postgres://"):
                url = url.replace("postgres://", "postgresql://", 1)
            
            # For Render internal/pooled connections, it's often safer to use ssl=True directly 
            # or a more permissive context if the above still fails.
            
            # Try up to 10 times with increasing wait to handle Render network jitter
            for attempt in range(10):
                try:
                    # Removed connect_timeout as it is not a valid argument for create_pool in asyncpg
                    pool = await asyncpg.create_pool(
                        url,
                        min_size=1,
                        max_size=5,
                        command_timeout=60,
                        ssl=ctx,
                        server_settings={
                            'tcp_keepalives_idle': '60',
                            'tcp_keepalives_interval': '10',
                            'tcp_keepalives_count': '3',
                            'application_name': 'userbot_service'
                        }
                    )
                    self.db_pool = pool
                    logger.info("Database connected successfully")

                    # FORCE MIGRATION IMMEDIATELY AFTER SUCCESSFUL CONNECTION
                    async with self.db_pool.acquire() as conn:
                        logger.info("Running immediate database migration check...")
                        try:
                            # 1. Rename 'message' to 'message_text'
                            msg_exists = await conn.fetchval("""
                                SELECT count(*) FROM information_schema.columns 
                                WHERE table_name = 'userbot_dm_queue' AND column_name = 'message'
                            """)
                            if msg_exists > 0:
                                await conn.execute("ALTER TABLE userbot_dm_queue RENAME COLUMN message TO message_text")
                                logger.info("✅ Renamed 'message' to 'message_text'")

                            # 2. Rename 'message_content' to 'message_text'
                            try:
                                content_exists = await conn.fetchval("""
                                    SELECT count(*) FROM information_schema.columns 
                                    WHERE table_name = 'userbot_dm_queue' AND column_name = 'message_content'
                                """)
                                text_exists = await conn.fetchval("""
                                    SELECT count(*) FROM information_schema.columns 
                                    WHERE table_name = 'userbot_dm_queue' AND column_name = 'message_text'
                                """)
                                if content_exists > 0 and text_exists == 0:
                                    await conn.execute("ALTER TABLE userbot_dm_queue RENAME COLUMN message_content TO message_text")
                                    logger.info("✅ Renamed 'message_content' to 'message_text'")
                                elif content_exists > 0 and text_exists > 0:
                                    # Handle case where both exist (failed partial migration)
                                    await conn.execute("ALTER TABLE userbot_dm_queue DROP COLUMN message_content")
                                    logger.info("✅ Dropped redundant 'message_content'")
                            except Exception as e:
                                logger.error(f"Error migrating message_content: {e}")
                            
                            # 3. Rename 'message_type' to 'label'
                            try:
                                type_exists = await conn.fetchval("""
                                    SELECT count(*) FROM information_schema.columns 
                                    WHERE table_name = 'userbot_dm_queue' AND column_name = 'message_type'
                                """)
                                label_exists = await conn.fetchval("""
                                    SELECT count(*) FROM information_schema.columns 
                                    WHERE table_name = 'userbot_dm_queue' AND column_name = 'label'
                                """)
                                if type_exists > 0 and label_exists == 0:
                                    await conn.execute("ALTER TABLE userbot_dm_queue RENAME COLUMN message_type TO label")
                                    logger.info("✅ Renamed 'message_type' to 'label'")
                                elif type_exists > 0 and label_exists > 0:
                                    # Handle case where both exist
                                    await conn.execute("ALTER TABLE userbot_dm_queue DROP COLUMN message_type")
                                    logger.info("✅ Dropped redundant 'message_type'")
                            except Exception as e:
                                logger.error(f"Error migrating message_type: {e}")

                            # 5. Fix NOT NULL constraints on old columns that might be lingering
                            try:
                                await conn.execute("ALTER TABLE userbot_dm_queue ALTER COLUMN message_text SET NOT NULL")
                                await conn.execute("ALTER TABLE userbot_dm_queue ALTER COLUMN label SET NOT NULL")
                            except: pass

                            # 6. Ensure sent_at column exists
                            try:
                                has_sent_at = await conn.fetchval("""
                                    SELECT count(*) FROM information_schema.columns 
                                    WHERE table_name = 'userbot_dm_queue' AND column_name = 'sent_at'
                                """)
                                if has_sent_at == 0:
                                    await conn.execute("ALTER TABLE userbot_dm_queue ADD COLUMN sent_at TIMESTAMP WITH TIME ZONE")
                                    logger.info("✅ Added missing 'sent_at' column")
                            except Exception as e:
                                logger.error(f"Error adding sent_at column: {e}")

                            
                            # 4. FIX: Handle potential column naming variations and ensure 'label' or 'message_text' is large enough
                            # We use TRY blocks for each to be safe
                            try:
                                await conn.execute("ALTER TABLE userbot_dm_queue ALTER COLUMN label TYPE TEXT")
                                logger.info("✅ Ensured 'label' column is TEXT type")
                            except: pass

                            try:
                                await conn.execute("ALTER TABLE userbot_dm_queue ALTER COLUMN message_text TYPE TEXT")
                                logger.info("✅ Ensured 'message_text' column is TEXT type")
                            except: pass
                        except Exception as migration_err:
                            logger.error(f"❌ Migration during connection failed: {migration_err}")

                    break
                except Exception as e:
                    if attempt == 9:
                        raise
                    wait_time = min((attempt + 1) * 2, 20)
                    logger.warning(f"Database connection attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
        
        if self.db_pool:
            async with self.db_pool.acquire() as conn:
                # Forced migration: Rename 'message' or 'message_type' to 'message_text' if they exist
                try:
                    # Check for 'message'
                    message_exists = await conn.fetchval("""
                        SELECT count(*) FROM information_schema.columns 
                        WHERE table_name = 'userbot_dm_queue' AND column_name = 'message'
                    """)
                    if message_exists > 0:
                        logger.info("Migrating userbot_dm_queue: renaming 'message' to 'message_text'")
                        await conn.execute("ALTER TABLE userbot_dm_queue RENAME COLUMN message TO message_text")
                    
                    # Check for 'message_type' (which seems to be what it actually is on Render right now)
                    type_exists = await conn.fetchval("""
                        SELECT count(*) FROM information_schema.columns 
                        WHERE table_name = 'userbot_dm_queue' AND column_name = 'message_type'
                    """)
                    if type_exists > 0:
                        logger.info("Migrating userbot_dm_queue: renaming 'message_type' to 'message_text'")
                        await conn.execute("ALTER TABLE userbot_dm_queue RENAME COLUMN message_type TO message_text")

                    # Check for 'label' column
                    label_exists = await conn.fetchval("""
                        SELECT count(*) FROM information_schema.columns 
                        WHERE table_name = 'userbot_dm_queue' AND column_name = 'label'
                    """)
                    if label_exists == 0:
                        logger.info("Migrating userbot_dm_queue: adding missing 'label' column")
                        await conn.execute("ALTER TABLE userbot_dm_queue ADD COLUMN label TEXT DEFAULT 'manual'")
                except Exception as e:
                    logger.error(f"Migration error (updating columns): {e}")

                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS active_members (
                        member_id BIGINT PRIMARY KEY,
                        role_added_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        role_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        weekend_delayed BOOLEAN DEFAULT FALSE,
                        expiry_time TIMESTAMP WITH TIME ZONE,
                        custom_duration BOOLEAN DEFAULT FALSE,
                        monday_notification_sent BOOLEAN DEFAULT FALSE
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS dm_schedule (
                        member_id BIGINT PRIMARY KEY,
                        role_expired TIMESTAMP WITH TIME ZONE NOT NULL,
                        guild_id BIGINT NOT NULL,
                        dm_3_sent BOOLEAN DEFAULT FALSE,
                        dm_7_sent BOOLEAN DEFAULT FALSE,
                        dm_14_sent BOOLEAN DEFAULT FALSE
                    )
                """)
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

    async def log_to_debug(self, message: str, tag_owner: bool = False):
        try:
            if not self.client or not self.client.is_connected:
                logger.info(f"DEBUG_LOG (Client not ready): {message}")
                return
                
            # The user wants the Userbot account itself to send the message
            # without the "🤖 Userbot Service:" prefix if it's already identifying itself
            final_message = message
            if tag_owner:
                final_message += f"\n\n⚠️ Attention: [Owner](tg://user?id={BOT_OWNER_USER_ID})"
            
            try:
                # This call uses the Userbot account (Client) to send the message
                await self.client.send_message(DEBUG_GROUP_ID, final_message)
            except PeerIdInvalid:
                logger.info("Resolving Peer ID for Debug Group via dialogs...")
                async for dialog in self.client.get_dialogs():
                    if dialog.chat.id == DEBUG_GROUP_ID:
                        await self.client.send_message(DEBUG_GROUP_ID, final_message)
                        break
        except Exception as e:
            logger.error(f"Failed to log to debug group via userbot: {e}")
            logger.info(f"DEBUG_LOG: {message}")

    async def start(self):
        try:
            # Strictly use session string from environment variable (Secret)
            session_string = os.getenv("USERBOT_SESSION_STRING")
            
            if not session_string:
                logger.error("❌ USERBOT_SESSION_STRING NOT FOUND IN ENVIRONMENT.")
                logger.info("Please set the USERBOT_SESSION_STRING environment variable in Render.")
                return

            # Initialize DB pool if URL is available (needed for DM queue)
            try:
                await self.init_db()
            except Exception as e:
                logger.warning(f"Database initialization failed: {e}. Some features may be limited.")
            
            try:
                logger.info("🚀 Userbot Service: Starting Pyrogram client with environment session...")
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

                # Register private message handler for auto-replies
                @self.client.on_message(filters.private)
                async def on_private_message(client, message):
                    if not message.from_user or message.from_user.id == BOT_OWNER_USER_ID:
                        return
                    
                    # Log the incoming message
                    logger.info(f"Userbot received PM from {message.from_user.id}: {message.text[:50] if message.text else '[No text]'}")
                    
                    # Auto-reply message
                    auto_reply_text = (
                        "Hey! This is an automated trading account and I can't respond to messages here.\n\n"
                        f"If you have any questions, please feel free to message my owner directly: [Owner](tg://user?id={BOT_OWNER_USER_ID})"
                    )
                    
                    try:
                        # Add a small delay to seem more natural
                        await asyncio.sleep(2)
                        await message.reply(auto_reply_text)
                        await self.log_to_debug(f"🤖 Userbot auto-replied to {message.from_user.id}")
                    except Exception as e:
                        logger.error(f"Failed to send auto-reply: {e}")

                await self.client.start()
                logger.info("✅ Userbot Service: Connected successfully!")

                # Apply Privacy Settings (Hide Phone Number, Profile Photo, Last Seen)
                try:
                    logger.info("🔒 Applying privacy settings to Userbot...")
                    # Hide phone number from everyone
                    await self.client.invoke(
                        functions.account.SetPrivacy(
                            key=types.InputPrivacyKeyPhoneNumber(),
                            rules=[types.InputPrivacyValueDisallowAll()]
                        )
                    )
                    # Allow profile photo to be seen by everyone
                    await self.client.invoke(
                        functions.account.SetPrivacy(
                            key=types.InputPrivacyKeyProfilePhoto(),
                            rules=[types.InputPrivacyValueAllowAll()]
                        )
                    )
                    # Hide last seen from everyone
                    await self.client.invoke(
                        functions.account.SetPrivacy(
                            key=types.InputPrivacyKeyStatusTimestamp(),
                            rules=[types.InputPrivacyValueDisallowAll()]
                        )
                    )
                    logger.info("✅ Privacy settings applied successfully!")
                except Exception as priv_err:
                    logger.error(f"Failed to apply privacy settings: {priv_err}")
                
                # Resolve initial Peer IDs by fetching dialogs
                logger.info("🔍 Resolving initial Peer IDs (human-mimic mode)...")
                async for dialog in self.client.get_dialogs(limit=50):
                    pass
                logger.info("✅ Peer resolution complete.")
                        
            except Exception as e:
                logger.error(f"Failed to start with session string: {e}")
                return
            
            # Define group IDs locally if they are missing
            global FREE_GROUP_ID, VIP_GROUP_ID
            FREE_GROUP_ID = int(os.getenv("FREE_GROUP_ID", "0"))
            VIP_GROUP_ID = int(os.getenv("VIP_GROUP_ID", "0"))
            
            # NOW send the notifications since we are connected
            try:
                await self.log_to_debug("🚀 **Userbot Service**: Startup sequence complete.")
                await self.log_to_debug("✅ **Userbot Service**: Connected and monitoring for DMs.")
            except Exception as log_err:
                logger.error(f"Failed to send startup debug logs: {log_err}")
            
            # Start task loops
            await asyncio.gather(
                self.dm_loop(),
                self.peer_discovery_loop()
            )
            
        except Exception as e:
            # If the loop breaks or start fails, try to notify
            try:
                error_msg = f"‼️ **CRITICAL DISCONNECT**: Userbot service has crashed or stopped.\nError: {e}"
                await self.log_to_debug(error_msg, tag_owner=True)
            except:
                pass
            logger.error(f"Userbot Service encountered a fatal error: {e}")
            raise

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
            await asyncio.sleep(int(wait_time))
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
                    # 1. Daily 9AM Trial Offer for members who haven't accepted yet
                    if current_time.hour == 9 and current_time.minute < 10:
                        # Check if we already processed this hour today
                        last_global_offer = await conn.fetchval("SELECT setting_value FROM bot_settings WHERE setting_key = 'last_9am_offer_run'")
                        today_str = current_time.strftime('%Y-%m-%d')
                        
                        if last_global_offer != today_str:
                            # Ensure column exists
                            await conn.execute("ALTER TABLE peer_id_checks ADD COLUMN IF NOT EXISTS last_daily_offer_at TIMESTAMP WITH TIME ZONE")
                            
                            # Find users who joined Free group but aren't in active_members and haven't received the daily offer today or yesterday (skip 1 day logic)
                            pending_trial_users = await conn.fetch("""
                                SELECT p.user_id 
                                FROM peer_id_checks p
                                LEFT JOIN active_members a ON p.user_id = a.member_id
                                WHERE a.member_id IS NULL 
                                AND p.peer_id_established = TRUE
                                AND (p.last_daily_offer_at IS NULL OR p.last_daily_offer_at < $1)
                            """, current_time - timedelta(days=2))

                            for row in pending_trial_users:
                                user_id = row['user_id']
                                # Double check if they aren't already queued for the same offer recently
                                exists = await conn.fetchval("SELECT id FROM userbot_dm_queue WHERE user_id = $1 AND label = 'Daily Trial Offer' AND created_at > $2", user_id, current_time - timedelta(hours=23))
                                if not exists:
                                    msg = "Want to try our VIP Group for FREE?\n\nWe're offering a 3-day free trial of our VIP Group where you'll receive 6+ high-quality trade signals per day.\n\nYour free trial will automatically be activated once you join our VIP group through this link: https://t.me/+5X18tTjgM042ODU0"
                                    
                                    # Queue DM
                                    await conn.execute("INSERT INTO userbot_dm_queue (user_id, message_text, label, status) VALUES ($1, $2, 'Daily Trial Offer', 'pending')", user_id, msg)
                                    
                                    # Update last_daily_offer_at to current_time to ensure it skips tomorrow
                                    await conn.execute("UPDATE peer_id_checks SET last_daily_offer_at = $1 WHERE user_id = $2", current_time, user_id)
                                    await self.log_to_debug(f"📅 Queued Daily Trial Offer for {user_id} (Skipping tomorrow)")
                            
                            # Mark this day as run
                            await conn.execute("INSERT INTO bot_settings (setting_key, setting_value) VALUES ('last_9am_offer_run', $1) ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value", today_str)

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
                                await conn.execute("INSERT INTO userbot_dm_queue (user_id, message_text, label, status) VALUES ($1, $2, '24h Warning', 'pending')", member_id, msg)

                        # 3h Warning
                        if 2.5 <= hours_left <= 3.5:
                            sent_check = await conn.fetchval("SELECT id FROM userbot_dm_queue WHERE user_id = $1 AND label = '3h Warning' AND created_at > $2", member_id, current_time - timedelta(days=1))
                            if not sent_check:
                                msg = "**FINAL REMINDER! Your 3-day free trial for our VIP Group will expire in just 3 hours**.\n\nYou're about to lose access to our VIP Group and the 6+ daily trade signals and opportunities it comes with. Upgrade to VIP to keep your access: https://whop.com/gold-pioneer/gold-pioneer/"
                                await conn.execute("INSERT INTO userbot_dm_queue (user_id, message_text, label, status) VALUES ($1, $2, '3h Warning', 'pending')", member_id, msg)

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
                            await conn.execute("INSERT INTO userbot_dm_queue (user_id, message_text, label, status) VALUES ($1, $2, 'Trial Expired', 'pending')", member_id, msg)

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
