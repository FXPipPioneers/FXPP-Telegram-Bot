from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from src.features.core.config import PAIR_CONFIG

async def handle_price_test(bot_instance, client: Client, message: Message):
    if not await bot_instance.is_owner(message.from_user.id):
        return

    args = message.text.split()[1:] if len(
        message.text.split()) > 1 else []

    if not args:
        popular_pairs = [['EURUSD', 'GBPUSD', 'USDJPY'],
                            ['XAUUSD', 'GBPJPY', 'AUDUSD'],
                            ['BTCUSD', 'US100', 'GER40'],
                            ['EURGBP', 'USDCAD', 'NZDUSD']]

        buttons = []
        for row in popular_pairs:
            button_row = [
                InlineKeyboardButton(pair,
                                        callback_data=f"pricetest_{pair}")
                for pair in row
            ]
            buttons.append(button_row)

        buttons.append([
            InlineKeyboardButton("Custom Pair",
                                    callback_data="pricetest_custom")
        ])
        buttons.append([
            InlineKeyboardButton("Cancel",
                                    callback_data="pricetest_cancel")
        ])

        keyboard = InlineKeyboardMarkup(buttons)

        await message.reply(
            "**Price Test Menu**\n\n"
            "Select a trading pair to check its live price:",
            reply_markup=keyboard)
        return

    pair = args[0].upper()
    await execute_price_test(bot_instance, message, pair)

async def execute_price_test(bot_instance, message: Message, pair: str):
    pair_name = PAIR_CONFIG.get(pair, {}).get('name', pair)

    price = await bot_instance.get_live_price(pair)

    if price:
        decimals = PAIR_CONFIG.get(pair, {}).get('decimals', 5)
        await message.reply(
            f"**Price Test: {pair_name}**\n\nLive Price: **{price:.{decimals}f}**"
        )
    else:
        await message.reply(
            f"Could not retrieve price for **{pair}**. The pair may not be supported or APIs are unavailable."
        )

async def handle_pricetest_callback(bot_instance, client: Client, callback_query: CallbackQuery):
    data = callback_query.data

    if data == "pricetest_cancel":
        await callback_query.message.edit_text("Price test cancelled.")
        await callback_query.answer()
        return

    if data == "pricetest_custom":
        bot_instance.awaiting_custom_pair[callback_query.from_user.id] = {
            'type': 'pricetest',
            'message_id': callback_query.message.id
        }
        await callback_query.message.edit_text(
            "**Price Test - Custom Pair**\n\n"
            "Type the trading pair you want to check (e.g., EURUSD, GBPJPY):"
        )
        await callback_query.answer()
        return

    if data.startswith("pricetest_"):
        pair = data.replace("pricetest_", "").upper()
        await callback_query.message.edit_text(
            f"Fetching live price for **{pair}**...")

        price = await bot_instance.get_live_price(pair)

        if price:
            pair_name = PAIR_CONFIG.get(pair, {}).get('name', pair)
            decimals = PAIR_CONFIG.get(pair, {}).get('decimals', 5)
            await callback_query.message.edit_text(
                f"**Price Test: {pair_name}**\n\nLive Price: **{price:.{decimals}f}**"
            )
        else:
            await callback_query.message.edit_text(
                f"Could not retrieve price for **{pair}**. The pair may not be supported or APIs are unavailable."
            )

    await callback_query.answer()
