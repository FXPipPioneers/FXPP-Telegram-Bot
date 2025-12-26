import os
import sys
import logging
import asyncio

# Ensure 'src' is importable by adding the parent directory to sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(BASE_DIR))

from pyrogram import Client as PyClient
from pyrogram import filters
from src.core.config import TELEGRAM_BOT_TOKEN, TELEGRAM_API_ID, TELEGRAM_API_HASH, DATABASE_URL
from src.database.manager import DatabaseManager
from src.core.engine import PriceEngine
from src.handlers.trading import register_trading_handlers
from src.handlers.community import register_community_handlers
from src.handlers.admin import register_admin_handlers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    if not DATABASE_URL:
        logger.error("DATABASE_URL not set!")
        return

    db_manager = DatabaseManager(DATABASE_URL)
    await db_manager.connect()
    
    app = PyClient(
        "trading_bot_v2",
        api_id=TELEGRAM_API_ID,
        api_hash=TELEGRAM_API_HASH,
        bot_token=TELEGRAM_BOT_TOKEN
    )
    
    engine = PriceEngine(db_manager)
    engine.app = app
    
    # Register all modular handlers
    register_trading_handlers(app, engine, db_manager)
    register_community_handlers(app, db_manager)
    register_admin_handlers(app, db_manager)
    
    logger.info(f"🔑 BOT_OWNER_USER_ID = {BOT_OWNER_USER_ID}")
    
    logger.info("Bot starting...")
    await app.start()
    
    await engine.start_all_loops()
    
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
