import time
import logging
from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from src.features.core.config import PAIR_CONFIG, PRICE_TRACKING_CONFIG

logger = logging.getLogger(__name__)

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
    await _execute_price_test(bot_instance, message, pair)

async def _execute_price_test(bot_instance, message: Message, pair: str):
    start_time = time.time()
    pair_name = PAIR_CONFIG.get(pair, {}).get('name', pair)
    
    # Priority order from config
    results = []
    final_price = None
    
    for api in PRICE_TRACKING_CONFIG["api_priority_order"]:
        try:
            price = await bot_instance.tracker.get_price_from_api(api, pair)
            if price:
                results.append(f"✅ **{api.capitalize()}**: {price}")
                if not final_price:
                    final_price = price
            else:
                results.append(f"❌ **{api.capitalize()}**: Failed")
        except Exception as e:
            results.append(f"❌ **{api.capitalize()}**: Error: {str(e)}")

    time_taken = time.time() - start_time
    result_text = "\n".join(results)

    if final_price:
        decimals = PAIR_CONFIG.get(pair, {}).get('decimals', 5)
        await message.reply(
            f"**PRICE TEST RESULT**\n\n"
            f"Pair: **{pair_name}**\n"
            f"Current Price: **{final_price:.{decimals}f}**\n"
            f"Time Taken: **{time_taken:.2f} seconds**\n\n"
            f"**API Status:**\n{result_text}\n\n"
            f"Status: Successfully fetched ✅"
        )
    else:
        await message.reply(
            f"❌ **Price Test Failed**\n\n"
            f"Could not retrieve price for **{pair}**.\n"
            f"**API Status:**\n{result_text}\n\n"
            f"Check API keys and network connection."
        )

async def handle_pricetest_callback(bot_instance, client: Client, callback_query: CallbackQuery):
    data = str(callback_query.data or "")

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
        
        await _execute_price_test(bot_instance, callback_query.message, pair)
        await callback_query.answer()
