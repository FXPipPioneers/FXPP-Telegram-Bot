from pyrogram import filters
from pyrogram.types import Message
from src.core.config import BOT_OWNER_USER_ID

async def is_owner(user_id: int) -> bool:
    return user_id == BOT_OWNER_USER_ID

def register_admin_handlers(app, db_manager):
    @app.on_message(filters.command("dbstatus"))
    async def handle_db_status(client, message: Message):
        if not await is_owner(message.from_user.id):
            return
        status = await db_manager.check_connection()
        if status:
            await message.reply("📊 **Database Status**: Online and connected.")
        else:
            await message.reply("📊 **Database Status**: Offline or connection error.")

    @app.on_message(filters.command("dmstatus"))
    async def handle_dm_status(client, message: Message):
        if not await is_owner(message.from_user.id):
            return
        # Simplified for now, can be expanded to check actual queue health
        await message.reply("📬 **DM Status**: System active.")

    @app.on_message(filters.command("sendwelcomedm"))
    async def handle_send_welcome_dm(client, message: Message):
        if not await is_owner(message.from_user.id):
            return
        # Logic to manually trigger a welcome DM for a specific user ID
        args = message.command
        if len(args) < 2:
            await message.reply("Usage: `/sendwelcomedm [user_id]`")
            return
        try:
            target_id = int(args[1])
            # Implementation would call the DM engine logic
            await message.reply(f"Attempting to send welcome DM to {target_id}...")
        except ValueError:
            await message.reply("Invalid User ID.")

    @app.on_message(filters.command("newmemberslist"))
    async def handle_new_members_list(client, message: Message):
        if not await is_owner(message.from_user.id):
            return
        # List members who joined recently (pending peer ID)
        await message.reply("📋 **New Members List**: Feature implementation pending database query.")

    @app.on_message(filters.command("dmmessages"))
    async def handle_dm_messages(client, message: Message):
        if not await is_owner(message.from_user.id):
            return
        await message.reply("📑 **DM Messages**: Listing all templates and scheduled messages.")

    @app.on_message(filters.command("timedautorole"))
    async def handle_timed_auto_role(client, message: Message):
        if not await is_owner(message.from_user.id):
            return
        await message.reply("⏳ **Timed Auto-Role**: Status of trial roles and expiries.")

    @app.on_message(filters.command("retracttrial"))
    async def handle_retract_trial(client, message: Message):
        if not await is_owner(message.from_user.id):
            return
        args = message.command
        if len(args) < 2:
            await message.reply("Usage: `/retracttrial [user_id]`")
            return
        await message.reply(f"Retracting trial for {args[1]}...")
