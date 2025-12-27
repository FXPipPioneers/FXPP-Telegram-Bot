from pyrogram.client import Client
from pyrogram.types import Message
from src.features.core.config import PRICE_TRACKING_CONFIG

async def handle_db_status(bot_instance, client: Client, message: Message):
    if not await bot_instance.is_owner(message.from_user.id):
        return

    status = "**Database Status**\n\n"

    if bot_instance.db_pool:
        try:
            async with bot_instance.db_pool.acquire() as conn:
                active_trades = await conn.fetchval(
                    "SELECT COUNT(*) FROM active_trades")
                active_members = await conn.fetchval(
                    "SELECT COUNT(*) FROM active_members")
                dm_scheduled = await conn.fetchval(
                    "SELECT COUNT(*) FROM dm_schedule")
                role_history = await conn.fetchval(
                    "SELECT COUNT(*) FROM role_history")

            status += (f"**Connection:** Connected\n"
                        f"**Active Trades:** {active_trades}\n"
                        f"**Trial Members:** {active_members}\n"
                        f"**DM Scheduled:** {dm_scheduled}\n"
                        f"**Anti-abuse Records:** {role_history}\n")
        except Exception as e:
            status += f"**Connection:** Error - {str(e)}\n"
    else:
        status += "**Connection:** Not connected\n"

    status += f"\n**In-Memory Trades:** {len(PRICE_TRACKING_CONFIG['active_trades'])}"

    await message.reply(status)

async def handle_dm_status(bot_instance, client: Client, message: Message):
    if not await bot_instance.is_owner(message.from_user.id):
        return

    if not bot_instance.db_pool:
        await message.reply("Database not connected.")
        return

    try:
        async with bot_instance.db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM dm_schedule")
            dm_3_sent = await conn.fetchval(
                "SELECT COUNT(*) FROM dm_schedule WHERE dm_3_sent = TRUE")
            dm_7_sent = await conn.fetchval(
                "SELECT COUNT(*) FROM dm_schedule WHERE dm_7_sent = TRUE")
            dm_14_sent = await conn.fetchval(
                "SELECT COUNT(*) FROM dm_schedule WHERE dm_14_sent = TRUE")
            pending_3 = await conn.fetchval(
                "SELECT COUNT(*) FROM dm_schedule WHERE dm_3_sent = FALSE")
            pending_7 = await conn.fetchval(
                "SELECT COUNT(*) FROM dm_schedule WHERE dm_7_sent = FALSE AND dm_3_sent = TRUE"
            )
            pending_14 = await conn.fetchval(
                "SELECT COUNT(*) FROM dm_schedule WHERE dm_14_sent = FALSE AND dm_7_sent = TRUE"
            )

        status = (f"**DM Status Report**\n\n"
                    f"**Total Tracked Users:** {total}\n\n"
                    f"**3-Day Follow-up:**\n"
                    f"  Sent: {dm_3_sent} | Pending: {pending_3}\n\n"
                    f"**7-Day Follow-up:**\n"
                    f"  Sent: {dm_7_sent} | Pending: {pending_7}\n\n"
                    f"**14-Day Follow-up:**\n"
                    f"  Sent: {dm_14_sent} | Pending: {pending_14}\n")

        await message.reply(status)
    except Exception as e:
        await message.reply(f"Error fetching DM status: {str(e)}")
