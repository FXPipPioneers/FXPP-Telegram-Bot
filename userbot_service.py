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
            asyncio.set_event_loop_policy(
                asyncio.WindowsSelectorEventLoopPolicy())
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
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
                        })
                    self.db_pool = pool
                    logger.info("Database connected successfully")

                    # FORCE MIGRATION IMMEDIATELY AFTER SUCCESSFUL CONNECTION
                    async with self.db_pool.acquire() as conn:
                        logger.info(
                            "Running immediate database migration check...")
                        try:
                            # 1. Rename 'message' to 'message_text'
                            msg_exists = await conn.fetchval("""
                                SELECT count(*) FROM information_schema.columns 
                                WHERE table_name = 'userbot_dm_queue' AND column_name = 'message'
                            """)
                            if msg_exists > 0:
                                await conn.execute(
                                    "ALTER TABLE userbot_dm_queue RENAME COLUMN message TO message_text"
                                )
                                logger.info(
                                    "✅ Renamed 'message' to 'message_text'")

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
                                    await conn.execute(
                                        "ALTER TABLE userbot_dm_queue RENAME COLUMN message_content TO message_text"
                                    )
                                    logger.info(
                                        "✅ Renamed 'message_content' to 'message_text'"
                                    )
                                elif content_exists > 0 and text_exists > 0:
                                    # Handle case where both exist (failed partial migration)
                                    await conn.execute(
                                        "ALTER TABLE userbot_dm_queue DROP COLUMN message_content"
                                    )
                                    logger.info(
                                        "✅ Dropped redundant 'message_content'"
                                    )
                            except Exception as e:
                                logger.error(
                                    f"Error migrating message_content: {e}")

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
                                    await conn.execute(
                                        "ALTER TABLE userbot_dm_queue RENAME COLUMN message_type TO label"
                                    )
                                    logger.info(
                                        "✅ Renamed 'message_type' to 'label'")
                                elif type_exists > 0 and label_exists > 0:
                                    # Handle case where both exist
                                    await conn.execute(
                                        "ALTER TABLE userbot_dm_queue DROP COLUMN message_type"
                                    )
                                    logger.info(
                                        "✅ Dropped redundant 'message_type'")
                            except Exception as e:
                                logger.error(
                                    f"Error migrating message_type: {e}")

                            # 5. Fix NOT NULL constraints on old columns that might be lingering
                            try:
                                await conn.execute(
                                    "ALTER TABLE userbot_dm_queue ALTER COLUMN message_text SET NOT NULL"
                                )
                                await conn.execute(
                                    "ALTER TABLE userbot_dm_queue ALTER COLUMN label SET NOT NULL"
                                )
                            except:
                                pass

                            # 6. Ensure sent_at column exists
                            try:
                                has_sent_at = await conn.fetchval("""
                                    SELECT count(*) FROM information_schema.columns 
                                    WHERE table_name = 'userbot_dm_queue' AND column_name = 'sent_at'
                                """)
                                if has_sent_at == 0:
                                    await conn.execute(
                                        "ALTER TABLE userbot_dm_queue ADD COLUMN sent_at TIMESTAMP WITH TIME ZONE"
                                    )
                                    logger.info(
                                        "✅ Added missing 'sent_at' column")
                            except Exception as e:
                                logger.error(
                                    f"Error adding sent_at column: {e}")

                            # 7. Ensure last_retry_at column exists
                            try:
                                has_last_retry = await conn.fetchval("""
                                    SELECT count(*) FROM information_schema.columns 
                                    WHERE table_name = 'userbot_dm_queue' AND column_name = 'last_retry_at'
                                """)
                                if has_last_retry == 0:
                                    await conn.execute(
                                        "ALTER TABLE userbot_dm_queue ADD COLUMN last_retry_at TIMESTAMP WITH TIME ZONE"
                                    )
                                    logger.info(
                                        "✅ Added missing 'last_retry_at' column"
                                    )
                            except Exception as e:
                                logger.error(
                                    f"Error adding last_retry_at column: {e}")

                            # 8. Ensure abandoned column exists
                            try:
                                has_abandoned = await conn.fetchval("""
                                    SELECT count(*) FROM information_schema.columns 
                                    WHERE table_name = 'userbot_dm_queue' AND column_name = 'abandoned'
                                """)
                                if has_abandoned == 0:
                                    await conn.execute(
                                        "ALTER TABLE userbot_dm_queue ADD COLUMN abandoned BOOLEAN DEFAULT FALSE"
                                    )
                                    logger.info(
                                        "✅ Added missing 'abandoned' column")
                            except Exception as e:
                                logger.error(
                                    f"Error adding abandoned column: {e}")

                            # 4. FIX: Handle potential column naming variations and ensure 'label' or 'message_text' is large enough
                            # We use TRY blocks for each to be safe
                            try:
                                await conn.execute(
                                    "ALTER TABLE userbot_dm_queue ALTER COLUMN label TYPE TEXT"
                                )
                                logger.info(
                                    "✅ Ensured 'label' column is TEXT type")
                            except:
                                pass

                            try:
                                await conn.execute(
                                    "ALTER TABLE userbot_dm_queue ALTER COLUMN message_text TYPE TEXT"
                                )
                                logger.info(
                                    "✅ Ensured 'message_text' column is TEXT type"
                                )
                            except:
                                pass
                        except Exception as migration_err:
                            logger.error(
                                f"❌ Migration during connection failed: {migration_err}"
                            )

                    break
                except Exception as e:
                    if attempt == 9:
                        raise
                    wait_time = min((attempt + 1) * 2, 20)
                    logger.warning(
                        f"Database connection attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s..."
                    )
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
                        logger.info(
                            "Migrating userbot_dm_queue: renaming 'message' to 'message_text'"
                        )
                        await conn.execute(
                            "ALTER TABLE userbot_dm_queue RENAME COLUMN message TO message_text"
                        )

                    # Check for 'message_type' (which seems to be what it actually is on Render right now)
                    type_exists = await conn.fetchval("""
                        SELECT count(*) FROM information_schema.columns 
                        WHERE table_name = 'userbot_dm_queue' AND column_name = 'message_type'
                    """)
                    if type_exists > 0:
                        logger.info(
                            "Migrating userbot_dm_queue: renaming 'message_type' to 'message_text'"
                        )
                        await conn.execute(
                            "ALTER TABLE userbot_dm_queue RENAME COLUMN message_type TO message_text"
                        )

                    # Check for 'label' column
                    label_exists = await conn.fetchval("""
                        SELECT count(*) FROM information_schema.columns 
                        WHERE table_name = 'userbot_dm_queue' AND column_name = 'label'
                    """)
                    if label_exists == 0:
                        logger.info(
                            "Migrating userbot_dm_queue: adding missing 'label' column"
                        )
                        await conn.execute(
                            "ALTER TABLE userbot_dm_queue ADD COLUMN label TEXT DEFAULT 'manual'"
                        )
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
                        sent_at TIMESTAMP WITH TIME ZONE,
                        retry_count INTEGER DEFAULT 0,
                        last_retry_at TIMESTAMP WITH TIME ZONE,
                        abandoned BOOLEAN DEFAULT FALSE
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
                        await self.client.send_message(DEBUG_GROUP_ID,
                                                       final_message)
                        break
        except Exception as e:
            logger.error(f"Failed to log to debug group via userbot: {e}")
            logger.info(f"DEBUG_LOG: {message}")

    async def start(self):
        try:
            # Strictly use session string from environment variable (Secret)
            session_string = os.getenv("USERBOT_SESSION_STRING")

            if not session_string:
                logger.error(
                    "❌ USERBOT_SESSION_STRING NOT FOUND IN ENVIRONMENT.")
                logger.info(
                    "Please set the USERBOT_SESSION_STRING environment variable in Render."
                )
                return

            # Initialize DB pool if URL is available (needed for DM queue)
            try:
                await self.init_db()
            except Exception as e:
                logger.warning(
                    f"Database initialization failed: {e}. Some features may be limited."
                )

            try:
                logger.info(
                    "🚀 Userbot Service: Starting Pyrogram client with environment session..."
                )
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
                    logger.info(
                        f"Userbot received PM from {message.from_user.id}: {message.text[:50] if message.text else '[No text]'}"
                    )

                    # Auto-reply message
                    auto_reply_text = (
                        "This is a private trading bot that can only be used by members of the FX Pip Pioneers team. \n\n"
                        "If you need support or have questions, please contact @fx_pippioneers."
                    )

                    try:
                        await message.reply(auto_reply_text)
                        await self.log_to_debug(
                            f"🤖 Userbot auto-replied to {message.from_user.id}"
                        )
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
                            rules=[types.InputPrivacyValueDisallowAll()]))
                    # Allow profile photo to be seen by everyone
                    await self.client.invoke(
                        functions.account.SetPrivacy(
                            key=types.InputPrivacyKeyProfilePhoto(),
                            rules=[types.InputPrivacyValueAllowAll()]))
                    # Hide last seen from everyone
                    await self.client.invoke(
                        functions.account.SetPrivacy(
                            key=types.InputPrivacyKeyStatusTimestamp(),
                            rules=[types.InputPrivacyValueDisallowAll()]))
                    logger.info("✅ Privacy settings applied successfully!")
                except Exception as priv_err:
                    logger.error(
                        f"Failed to apply privacy settings: {priv_err}")

                # Resolve initial Peer IDs by fetching dialogs
                logger.info(
                    "🔍 Resolving initial Peer IDs (human-mimic mode)...")
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
                await self.log_to_debug(
                    "🚀 **Userbot Service**: Startup sequence complete.")
                await self.log_to_debug(
                    "✅ **Userbot Service**: Connected and monitoring for DMs.")
            except Exception as log_err:
                logger.error(f"Failed to send startup debug logs: {log_err}")

            # Start task loops
            await asyncio.gather(self.dm_loop(), self.peer_discovery_loop())

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
                    logger.info(
                        "Peer discovery heartbeat - checking for new members")
                    # Optionally fetch recent members from tracked groups
                    for group_id in [FREE_GROUP_ID, VIP_GROUP_ID]:
                        if group_id != 0:
                            async for member in self.client.get_chat_members(
                                    group_id, limit=20):
                                pass  # This caches the peer in pyrogram's memory
                await asyncio.sleep(600
                                    )  # Check every 10 mins instead of 1 hour
            except Exception as e:
                logger.error(f"Error in peer discovery: {e}")
                await asyncio.sleep(60)

    async def send_dm(self, user_id: int, message: str, label: str):
        if not self.client or not self.client.is_connected:
            logger.error(
                f"Cannot send DM {label} to {user_id}: Client not connected")
            return False

        try:
            # Force a peer resolution if possible by checking the chat member status first
            # this helps with "Peer ID Invalid" for new members
            try:
                for group_id in [FREE_GROUP_ID, VIP_GROUP_ID]:
                    if group_id != 0:
                        try:
                            await self.client.get_chat_member(
                                group_id, user_id)
                            break  # Found them, peer is now cached
                        except:
                            continue
            except Exception as peer_err:
                logger.debug(
                    f"Peer pre-resolution attempt for {user_id} failed: {peer_err}"
                )

            await asyncio.sleep(random.randint(5, 15))
            await self.client.send_message(user_id, message)
            await self.log_to_debug(f"✅ Sent {label} to {user_id}")
            return True
        except UserPrivacyRestricted:
            await self.log_to_debug(
                f"❌ Privacy Violation: Cannot message {user_id} ({label})")
            return False
        except PeerIdInvalid:
            logger.warning(
                f"Peer ID Invalid for {user_id} ({label}) - will retry if queued"
            )
            return False
        except FloodWait as e:
            wait_time = float(
                e.value) if hasattr(e, 'value') and e.value else 60
            logger.warning(f"FloodWait: Waiting {wait_time}s")
            await asyncio.sleep(int(wait_time))
            return await self.send_dm(user_id, message, label)
        except Exception as e:
            err_msg = str(e)
            if "PEER_FLOOD" in err_msg:
                # Account limited - stop loop briefly and log once
                logger.error(
                    f"❌ Account Limited (PEER_FLOOD): {user_id} ({label})")
                return False

            await self.log_to_debug(
                f"❌ Failed to send {label} to {user_id}: {e}")
            return False

    async def dm_loop(self):
        while self.running:
            try:
                if not self.db_pool:
                    await asyncio.sleep(10)
                    continue

                current_time = datetime.now(pytz.UTC).astimezone(AMSTERDAM_TZ)

                # 1. Daily 9AM Trial Offer
                try:
                    async with self.db_pool.acquire() as conn:
                        if 8 <= current_time.hour < 12:
                            last_global_offer = await conn.fetchval(
                                "SELECT setting_value FROM bot_settings WHERE setting_key = 'last_9am_offer_run'"
                            )
                            today_str = current_time.strftime('%Y-%m-%d')

                            if last_global_offer != today_str:
                                await conn.execute(
                                    "ALTER TABLE peer_id_checks ADD COLUMN IF NOT EXISTS last_daily_offer_at TIMESTAMP WITH TIME ZONE"
                                )
                                await conn.execute(
                                    "ALTER TABLE peer_id_checks ADD COLUMN IF NOT EXISTS ever_in_vip BOOLEAN DEFAULT FALSE"
                                )
                                await conn.execute(
                                    "ALTER TABLE peer_id_checks ADD COLUMN IF NOT EXISTS daily_offer_count INTEGER DEFAULT 0"
                                )

                                pending_trial_users = await conn.fetch(
                                    """
                                    SELECT p.user_id 
                                    FROM peer_id_checks p
                                    LEFT JOIN active_members a ON p.user_id = a.member_id
                                    WHERE p.welcome_dm_sent = TRUE
                                    AND a.member_id IS NULL 
                                    AND p.ever_in_vip = FALSE
                                    AND p.daily_offer_count < 3
                                    AND p.peer_id_established = TRUE
                                    AND (p.last_daily_offer_at IS NULL OR p.last_daily_offer_at < $1)
                                """, current_time - timedelta(days=2))

                                for row in pending_trial_users:
                                    user_id = row['user_id']
                                    exists = await conn.fetchval(
                                        "SELECT id FROM userbot_dm_queue WHERE user_id = $1 AND label = 'Daily Trial Offer' AND created_at > $2",
                                        user_id,
                                        current_time - timedelta(hours=23))
                                    if not exists:
                                        msg = "Want to try our VIP Group for FREE?\n\nWe're offering a 3-day free trial (excluding the weekend) of our VIP Group where you'll receive 6+ high-quality trade signals per day.\n\nYour free trial will automatically be activated once you join our VIP group through this link: https://t.me/+5X18tTjgM042ODU0"
                                        start_time = max(
                                            current_time,
                                            current_time.replace(hour=8,
                                                                 minute=0,
                                                                 second=0))
                                        end_time = current_time.replace(
                                            hour=12, minute=0, second=0)

                                        if end_time > start_time + timedelta(
                                                minutes=30):
                                            total_seconds = (
                                                end_time -
                                                start_time).total_seconds()
                                            scheduled_time = start_time + timedelta(
                                                seconds=random.randint(
                                                    0, int(total_seconds)))
                                        else:
                                            scheduled_time = current_time

                                        await conn.execute(
                                            """
                                            INSERT INTO userbot_dm_queue 
                                            (user_id, message_text, label, status, created_at) 
                                            VALUES ($1, $2, 'Daily Trial Offer', 'pending', $3)
                                        """, user_id, msg, scheduled_time)

                                        await conn.execute(
                                            """
                                            UPDATE peer_id_checks 
                                            SET last_daily_offer_at = $1, 
                                                daily_offer_count = daily_offer_count + 1 
                                            WHERE user_id = $2
                                        """, current_time, user_id)
                                        await self.log_to_debug(
                                            f"📅 Scheduled Daily Trial Offer for {user_id} at {scheduled_time.strftime('%H:%M')}"
                                        )

                                await conn.execute(
                                    "INSERT INTO bot_settings (setting_key, setting_value) VALUES ('last_9am_offer_run', $1) ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value",
                                    today_str)
                except Exception as e:
                    logger.error(f"Error in Daily Offer block: {e}")

                # 2. Handle Trial Expiry Warnings (Queueing only)
                try:
                    async with self.db_pool.acquire() as conn:
                        active_members = await conn.fetch(
                            "SELECT member_id, expiry_time FROM active_members"
                        )
                        for member in active_members:
                            member_id = member['member_id']
                            expiry_time = member['expiry_time']
                            if expiry_time.tzinfo is None:
                                expiry_time = AMSTERDAM_TZ.localize(
                                    expiry_time)

                            time_left = expiry_time - current_time

                            # 24h Warning
                            if timedelta(hours=23) <= time_left <= timedelta(
                                    hours=25):
                                exists = await conn.fetchval(
                                    "SELECT id FROM userbot_dm_queue WHERE user_id = $1 AND label = '24h_warning' AND created_at > $2",
                                    member_id,
                                    current_time - timedelta(hours=24))
                                if not exists:
                                    msg = "⏰ **REMINDER! Your 3-day free trial (excluding the weekend) for our VIP Group will expire in 24 hours**.\n\nAfter that, you'll unfortunately lose access to the VIP Group. You've had great opportunities during these past 2 days. Don't let this last day slip away!"
                                    await conn.execute(
                                        "INSERT INTO userbot_dm_queue (user_id, message_text, label, status) VALUES ($1, $2, '24h_warning', 'pending')",
                                        member_id, msg)

                            # 3h Warning
                            if timedelta(hours=2) <= time_left <= timedelta(
                                    hours=4):
                                exists = await conn.fetchval(
                                    "SELECT id FROM userbot_dm_queue WHERE user_id = $1 AND label = '3h_warning' AND created_at > $2",
                                    member_id,
                                    current_time - timedelta(hours=24))
                                if not exists:
                                    msg = "⏰ **REMINDER! Your 3-day free trial (excluding the weekend) for our VIP Group will expire in just 3 hours**.\n\nYou're about to lose access to our VIP Group and the 6+ daily trade signals and opportunities it comes with. However, you can also keep your access! Upgrade from FREE to VIP through our website and instantly regain your access to our VIP Group.\n\n**Upgrade to VIP to keep your access:** https://whop.com/gold-pioneer/gold-pioneer/"
                                    await conn.execute(
                                        "INSERT INTO userbot_dm_queue (user_id, message_text, label, status) VALUES ($1, $2, '3h_warning', 'pending')",
                                        member_id, msg)

                        # Handle Trial Expiration queueing
                        expired = await conn.fetch(
                            "SELECT member_id FROM active_members WHERE expiry_time <= $1",
                            current_time)
                        for member in expired:
                            member_id = member['member_id']
                            exists = await conn.fetchval(
                                "SELECT id FROM userbot_dm_queue WHERE user_id = $1 AND label = 'Trial Expired' AND created_at > $2",
                                member_id, current_time - timedelta(hours=1))
                            if not exists:
                                msg = "Hey! Your **3-day free trial (excluding the weekend)** to the VIP Group has unfortunately **ran out**. We truly hope you were able to benefit with us & we hope to see you back soon! For now, feel free to continue following our trade signals in our Free Group: https://t.me/fxpippioneers\n\n**Want to rejoin the VIP Group? You can regain access through this link:** https://whop.com/gold-pioneer/gold-pioneer/"
                                await conn.execute(
                                    "INSERT INTO userbot_dm_queue (user_id, message_text, label, status) VALUES ($1, $2, 'Trial Expired', 'pending')",
                                    member_id, msg)
                except Exception as e:
                    logger.error(f"Error in Expiry Warning block: {e}")

                # 3. Handle Retention Follow-ups (Direct Sending - using short connections)
                try:
                    followups = []
                    async with self.db_pool.acquire() as conn:
                        followups = await conn.fetch(
                            "SELECT member_id, role_expired, dm_3_sent, dm_7_sent, dm_14_sent FROM dm_schedule"
                        )

                    for f in followups:
                        m_id = f['member_id']
                        expired_at = f['role_expired']
                        if expired_at.tzinfo is None:
                            expired_at = AMSTERDAM_TZ.localize(expired_at)
                        days_since = (current_time - expired_at).days

                        for days, flag in [(3, 'dm_3_sent'), (7, 'dm_7_sent'),
                                           (14, 'dm_14_sent')]:
                            if days_since >= days and not f[flag]:
                                msg_templates = {
                                    3:
                                    "Hey! It's been 3 days since your **3-day free trial (excluding the weekend)** ended. We truly hope you got value from the **20+ trading signals** you received during that time.\n\nAs you've probably seen, our free signals channel gets **1 free signal per day**, while our **VIP members** in the VIP Group receive **6+ high-quality signals per day**. That means that our VIP Group offers way more chances to profit and grow consistently.\n\nWe'd love to **invite you back to the VIP Group,** so you don't miss out on more solid opportunities.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer/gold-pioneer/",
                                    7:
                                    "It's been a week since your **3-day free trial (excluding the weekend)** ended. Since then, our **VIP members have been catching trade setups daily in the VIP Group**.\n\nIf you found value in just 3 days, imagine what results you could've been seeing by now with full access. It's all about **consistency and staying connected to the right information**.\n\nWe'd like to **personally invite you to rejoin the VIP Group** and get back into the rhythm.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer/gold-pioneer/",
                                    14:
                                    "Hey! It's been two weeks since your **3-day free trial (excluding the weekend)** ended. We hope you've stayed active since then.\n\nIf you've been trading solo or passively following the free channel, you might be feeling the difference. In the VIP Group, it's not just about more signals. It's about the **structure, support, and smarter decision-making**. That edge can make all the difference over time.\n\nWe'd love to **invite you back into the VIP Group** and help you start compounding results again.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer/gold-pioneer/"
                                }
                                success = await self.send_dm(
                                    m_id, msg_templates[days],
                                    f"{days}-Day Follow-up")
                                if success:
                                    async with self.db_pool.acquire() as conn:
                                        await conn.execute(
                                            f"UPDATE dm_schedule SET {flag} = TRUE WHERE member_id = $1",
                                            m_id)
                except Exception as e:
                    logger.error(f"Error in Retention Follow-up block: {e}")

                # 4. Handle Engagement Tracking
                try:
                    joins = []
                    async with self.db_pool.acquire() as conn:
                        joins = await conn.fetch(
                            'SELECT user_id, joined_at FROM free_group_joins WHERE discount_sent = FALSE'
                        )

                    for j in joins:
                        if current_time >= j['joined_at'] + timedelta(days=30):
                            async with self.db_pool.acquire() as conn:
                                reaction_count = await conn.fetchval(
                                    'SELECT COUNT(DISTINCT message_id) FROM emoji_reactions WHERE user_id = $1 AND reaction_time > $2',
                                    j['user_id'], j['joined_at'])
                            if reaction_count >= 5:
                                msg = "Hey! 👋 We noticed that you've been engaging with our signals in the Free Group. We want to say that we truly appreciate it!\n\nAs a form of appreciation for your loyalty and engagement, we want to give you something special: **an exclusive 50% discount for access to our VIP Group.**\n\n**Your exclusive discount code is:** `Thank_You!50!`\n\n**You can upgrade to VIP and apply your discount code here:** https://whop.com/gold-pioneer/gold-pioneer/"
                                if await self.send_dm(j['user_id'], msg,
                                                      "Engagement Discount"):
                                    async with self.db_pool.acquire() as conn:
                                        await conn.execute(
                                            'UPDATE free_group_joins SET discount_sent = TRUE WHERE user_id = $1',
                                            j['user_id'])
                except Exception as e:
                    logger.error(f"Error in Engagement Tracking block: {e}")

                # 5. Handle Monday Activations
                try:
                    if current_time.weekday() == 0 and current_time.hour <= 1:
                        delayed = []
                        async with self.db_pool.acquire() as conn:
                            delayed = await conn.fetch(
                                "SELECT member_id FROM active_members WHERE weekend_delayed = TRUE AND NOT monday_notification_sent"
                            )
                        for d in delayed:
                            msg = "Hey! The weekend is over, so the trading markets have been opened again. That means your **3-day free trial (excluding the weekend)** has officially started."
                            if await self.send_dm(d['member_id'], msg,
                                                  "Monday Activation"):
                                async with self.db_pool.acquire() as conn:
                                    await conn.execute(
                                        "UPDATE active_members SET monday_notification_sent = TRUE WHERE member_id = $1",
                                        d['member_id'])
                except Exception as e:
                    logger.error(f"Error in Monday Activation block: {e}")

                # 6. Process Queued DMs (Tiered Retry Logic)
                try:
                    queued_dms = []
                    async with self.db_pool.acquire() as conn:
                        # Fetch items that are pending and not abandoned
                        queued_dms = await conn.fetch("""
                            SELECT id, user_id, message_text, label, created_at, retry_count, last_retry_at 
                            FROM userbot_dm_queue 
                            WHERE status = 'pending' AND abandoned = FALSE 
                            ORDER BY created_at ASC LIMIT 10
                        """)

                    for row in queued_dms:
                        row_id = row['id']
                        u_id = row['user_id']
                        label = row['label']
                        retries = row['retry_count']
                        last_retry = row['last_retry_at']

                        # 1. 10-minute initial delay for Welcome DMs
                        if label == 'Welcome DM':
                            join_time = row['created_at']
                            if join_time.tzinfo is None:
                                join_time = AMSTERDAM_TZ.localize(join_time)
                            if current_time < join_time + timedelta(
                                    minutes=10):
                                continue

                        # 2. Tiered Retry Logic
                        # Phase 1: First 3 tries (Immediate/Sequential)
                        # Phase 2: After 3 tries, wait 30 minutes, then 3 more tries
                        # Phase 3: After 6 tries, wait 3.5 hours, then 3 final tries
                        # Total max tries: 9

                        can_retry = False
                        if retries == 0:
                            can_retry = True
                        elif retries < 3:
                            # Try every minute (default loop interval)
                            can_retry = True
                        elif 3 <= retries < 6:
                            # 30-minute pause after 3rd try
                            if last_retry and current_time >= last_retry + timedelta(
                                    minutes=30):
                                can_retry = True
                        elif 6 <= retries < 9:
                            # 3.5-hour pause after 6th try
                            if last_retry and current_time >= last_retry + timedelta(
                                    hours=3.5):
                                can_retry = True
                        else:
                            # Abandon after 9 tries
                            async with self.db_pool.acquire() as conn:
                                await conn.execute(
                                    "UPDATE userbot_dm_queue SET abandoned = TRUE, status = 'abandoned' WHERE id = $1",
                                    row_id)
                            continue

                        if not can_retry:
                            continue

                        # Execute the DM attempt
                        msg_to_send = row['message_text']

                        # Dynamic hour calculation for 'Trial Started'
                        if label == 'Trial Started':
                            try:
                                async with self.db_pool.acquire() as conn:
                                    member_row = await conn.fetchrow(
                                        "SELECT expiry_time FROM active_members WHERE member_id = $1",
                                        u_id)
                                    if member_row and member_row['expiry_time']:
                                        expiry = member_row['expiry_time']
                                        if expiry.tzinfo is None:
                                            expiry = AMSTERDAM_TZ.localize(expiry)
                                        
                                        time_left = expiry - current_time
                                        hours_left = max(0, int(time_left.total_seconds() / 3600))
                                        
                                        # Update the message with actual hours remaining
                                        # Use regex to replace whatever hours value was there
                                        import re
                                        msg_to_send = re.sub(r'\*\*\d+ hours\*\*', f'**{hours_left} hours**', msg_to_send)
                                        logger.info(f"🕒 Recalculated hours for {u_id}: {hours_left}h remaining")
                            except Exception as e:
                                logger.error(f"Error recalculating trial hours: {e}")

                        success = await self.send_dm(u_id, msg_to_send,
                                                     label)

                        async with self.db_pool.acquire() as conn:
                            if success:
                                await conn.execute(
                                    """
                                    UPDATE userbot_dm_queue 
                                    SET status = 'sent', sent_at = $1::timestamptz 
                                    WHERE id = $2::integer
                                """, current_time, row_id)

                                # Update onboarding widget for Welcome DM success
                                if label == 'Welcome DM':
                                    try:
                                        # Main Bot manages this; we just update the status text in DB or log it
                                        # Since we're in dual-service, we use the debug log which Main Bot can see or just update widget
                                        await conn.execute(
                                            "INSERT INTO bot_settings (setting_key, setting_value) VALUES ($1, $2) ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value",
                                            f"widget_status_{u_id}",
                                            "✅ Welcome DM Sent Successfully!")
                                    except Exception:
                                        pass

                                # Quietly log success to console, skip debug group spam
                                logger.info(f"✅ Sent {label} to {u_id}")
                            else:
                                await conn.execute(
                                    """
                                    UPDATE userbot_dm_queue 
                                    SET retry_count = retry_count + 1, 
                                        last_retry_at = $1::timestamptz 
                                    WHERE id = $2::integer
                                """, current_time, row_id)

                                # Update onboarding widget for Welcome DM failure
                                if label == 'Welcome DM':
                                    try:
                                        await conn.execute(
                                            "INSERT INTO bot_settings (setting_key, setting_value) VALUES ($1, $2) ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value",
                                            f"widget_status_{u_id}",
                                            f"❌ Welcome DM Failed (Attempt {retries + 1})"
                                        )
                                    except Exception:
                                        pass
                except Exception as e:
                    logger.error(f"Error in Queue processing block: {e}")

                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"❌ Critical Error in DM loop: {e}")
                await asyncio.sleep(30)


if __name__ == "__main__":
    service = UserbotService()
    try:
        asyncio.run(service.start())
    except KeyboardInterrupt:
        service.running = False
