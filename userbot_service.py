import os
import asyncio
import logging
import asyncpg
from pyrogram import Client
from pyrogram.errors import FloodWait

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("UserbotService")

# Configuration from Environment Variables
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

class UserbotService:
    def __init__(self):
        self.client = None
        self.db_pool = None

    async def init_db(self):
        self.db_pool = await asyncpg.create_pool(DATABASE_URL)
        logger.info("Database connected")

    async def start(self):
        await self.init_db()
        
        # Fetch session string from DB
        async with self.db_pool.acquire() as conn:
            session_string = await conn.fetchval(
                "SELECT setting_value FROM bot_settings WHERE setting_key = 'userbot_session_string'"
            )
        
        if not session_string:
            logger.error("No userbot session string found in database. Please login via main bot first.")
            return

        self.client = Client(
            "userbot_service",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            session_string=session_string,
            no_updates=True
        )
        
        await self.client.start()
        logger.info("Userbot Service Started")
        
        # Run background loop for DMs
        await self.dm_loop()

    async def dm_loop(self):
        """Main loop for processing automated DMs"""
        while True:
            try:
                # Logic for sending DMs would go here, 
                # querying the database for pending trials/follow-ups
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Error in DM loop: {e}")
                await asyncio.sleep(30)

if __name__ == "__main__":
    service = UserbotService()
    asyncio.run(service.start())
