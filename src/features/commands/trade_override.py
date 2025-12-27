import logging
from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from src.features.core.config import PRICE_TRACKING_CONFIG
import asyncio

logger = logging.getLogger(__name__)

async def handle_trade_override(bot_instance, client: Client, message: Message):
    if not await bot_instance.is_owner(message.from_user.id):
        return

    active_trades = PRICE_TRACKING_CONFIG['active_trades']

    if not active_trades:
        await message.reply("**Trade Override System**\n\n"
                            "No active trades found.\n\n"
                            "Use /activetrades to confirm.")
        return

    menu_id = str(message.id)[-8:]
    trade_mapping = {}
    bot_instance.pending_multi_select = bot_instance.pending_multi_select if hasattr(
        bot_instance, 'pending_multi_select') else {}
    bot_instance.pending_multi_select[menu_id] = []
    buttons = []

    for idx, (msg_id,
              trade_data) in enumerate(list(active_trades.items())[:25]):
        pair = trade_data.get('pair', 'Unknown')
        action = trade_data.get('action', 'Unknown')
        tp_hits = trade_data.get('tp_hits', [])
        tp_status = f" ({', '.join(tp_hits)})" if tp_hits else ""
        group_name = trade_data.get('group_name', '')
        group_label = f" [{group_name}]" if group_name else ""

        trade_mapping[str(idx)] = msg_id

        buttons.append([
            InlineKeyboardButton(
                f"☐ {pair} - {action}{tp_status}{group_label}",
                callback_data=f"ovr_{menu_id}_sel_{idx}")
        ])

    bot_instance.override_trade_mappings[menu_id] = trade_mapping
    buttons.append([
        InlineKeyboardButton("✅ Confirm Selection",
                             callback_data=f"ovr_{menu_id}_confirm")
    ])
    buttons.append([
        InlineKeyboardButton("Cancel",
                             callback_data=f"ovr_{menu_id}_cancel")
    ])

    keyboard = InlineKeyboardMarkup(buttons)

    await message.reply(
        f"**Trade Override System**\n\n"
        f"Found **{len(active_trades)}** active trades.\n\n"
        f"Select up to 5 trades to modify (tap to toggle), then press Confirm:",
        reply_markup=keyboard)

async def handle_override_callback(bot_instance, client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id

    if not await bot_instance.is_owner(user_id):
        await callback_query.answer("This is restricted to the bot owner.",
                                    show_alert=True)
        return

    data = callback_query.data

    if not data.startswith("ovr_"):
        return

    parts = data.split("_")
    if len(parts) < 3:
        await callback_query.answer("Invalid callback data.",
                                    show_alert=True)
        return

    menu_id = parts[1]
    action_type = parts[2]

    if action_type == "cancel":
        bot_instance.override_trade_mappings.pop(menu_id, None)
        if hasattr(bot_instance, 'pending_multi_select'):
            bot_instance.pending_multi_select.pop(menu_id, None)
        await callback_query.message.edit_text("Trade override cancelled.")
        return

    trade_mapping = bot_instance.override_trade_mappings.get(menu_id, {})
    if not hasattr(bot_instance, 'pending_multi_select'):
        bot_instance.pending_multi_select = {}

    if action_type == "sel":
        idx = parts[3] if len(parts) > 3 else None
        if not idx:
            await callback_query.answer("Invalid selection.",
                                        show_alert=True)
            return

        selected = bot_instance.pending_multi_select.get(menu_id, [])
        if idx in selected:
            selected.remove(idx)
        else:
            if len(selected) >= 5:
                await callback_query.answer(
                    "Maximum 5 trades can be selected.", show_alert=True)
                return
            selected.append(idx)
        bot_instance.pending_multi_select[menu_id] = selected

        active_trades = PRICE_TRACKING_CONFIG['active_trades']
        buttons = []
        for i, (msg_id,
                trade_data) in enumerate(list(active_trades.items())[:25]):
            pair = trade_data.get('pair', 'Unknown')
            action = trade_data.get('action', 'Unknown')
            tp_hits = trade_data.get('tp_hits', [])
            tp_status = f" ({', '.join(tp_hits)})" if tp_hits else ""
            group_name = trade_data.get('group_name', '')
            group_label = f" [{group_name}]" if group_name else ""
            checkbox = "☑" if str(i) in selected else "☐"
            buttons.append([
                InlineKeyboardButton(
                    f"{checkbox} {pair} - {action}{tp_status}{group_label}",
                    callback_data=f"ovr_{menu_id}_sel_{i}")
            ])
        buttons.append([
            InlineKeyboardButton("✅ Confirm Selection",
                                 callback_data=f"ovr_{menu_id}_confirm")
        ])
        buttons.append([
            InlineKeyboardButton("Cancel",
                                 callback_data=f"ovr_{menu_id}_cancel")
        ])
        keyboard = InlineKeyboardMarkup(buttons)
        selected_count = len(selected)
        await callback_query.message.edit_text(
            f"**Trade Override System**\n\n"
            f"**Selected: {selected_count}/5** trades\n\n"
            f"Tap to toggle, then press Confirm:",
            reply_markup=keyboard)
        await callback_query.answer()
        return

    if action_type == "confirm":
        selected = bot_instance.pending_multi_select.get(menu_id, [])
        if not selected:
            await callback_query.answer(
                "Please select at least one trade.", show_alert=True)
            return

        trade_list = []
        for idx in selected:
            full_msg_id = trade_mapping.get(idx)
            if full_msg_id and full_msg_id in PRICE_TRACKING_CONFIG[
                    'active_trades']:
                trade = PRICE_TRACKING_CONFIG['active_trades'][full_msg_id]
                pair = trade.get('pair', 'Unknown')
                action = trade.get('action', 'Unknown')
                group_name = trade.get('group_name', '')
                group_label = f" [{group_name}]" if group_name else ""
                trade_list.append(f"• {pair} - {action}{group_label}")

        if len(selected) == 1:
            description = f"**Selected Trade:**\n{trade_list[0]}"
        else:
            description = f"**Selected {len(selected)} Trades:**\n" + "\n".join(
                trade_list)

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔴 SL Hit",
                                     callback_data=f"ovr_{menu_id}_slhit")
            ],
            [
                InlineKeyboardButton("🟢 TP1 Hit",
                                     callback_data=f"ovr_{menu_id}_tp1hit")
            ],
            [
                InlineKeyboardButton("🟢 TP2 Hit",
                                     callback_data=f"ovr_{menu_id}_tp2hit")
            ],
            [
                InlineKeyboardButton("🚀 TP3 Hit",
                                     callback_data=f"ovr_{menu_id}_tp3hit")
            ],
            [
                InlineKeyboardButton("🟡 Breakeven Hit After TP2",
                                     callback_data=f"ovr_{menu_id}_behit")
            ],
            [
                InlineKeyboardButton("⏹️ End Tracking",
                                     callback_data=f"ovr_{menu_id}_endhit")
            ],
            [
                InlineKeyboardButton("Cancel",
                                     callback_data=f"ovr_{menu_id}_cancel")
            ]
        ])

        await callback_query.message.edit_text(
            f"**Trade Override**\n\n"
            f"{description}\n\n"
            f"Select the status to apply:",
            reply_markup=keyboard)
        await callback_query.answer()
        return

    if action_type in [
            'slhit', 'tp1hit', 'tp2hit', 'tp3hit', 'behit', 'endhit'
    ]:
        selected = bot_instance.pending_multi_select.get(menu_id, [])
        if not selected:
            await callback_query.answer("No trades selected.",
                                        show_alert=True)
            return

        successful_trades = []
        failed_trades = []

        for idx in selected:
            full_msg_id = trade_mapping.get(idx)
            if not full_msg_id or full_msg_id not in PRICE_TRACKING_CONFIG[
                    'active_trades']:
                failed_trades.append(f"ID {idx}: Not found")
                continue

            trade = PRICE_TRACKING_CONFIG['active_trades'][full_msg_id]
            pair = trade.get('pair', 'Unknown')
            action = trade.get('action', 'Unknown')
            group_name = trade.get('group_name', '')
            group_label = f" [{group_name}]" if group_name else ""

            try:
                if action_type == 'slhit':
                    trade['status'] = 'closed (sl hit - manual override)'
                    await bot_instance.remove_trade_from_db(full_msg_id, 'manual_sl_hit')
                    await bot_instance.send_sl_notification(full_msg_id, trade, trade.get('sl_price', 0))
                    successful_trades.append(f"{pair} {action}{group_label} - SL Hit")

                elif action_type == 'tp1hit':
                    if 'TP1' not in trade.get('tp_hits', []):
                        trade['tp_hits'] = trade.get('tp_hits', []) + ['TP1']
                    trade['status'] = 'active (tp1 hit - manual override)'
                    await bot_instance.update_trade_in_db(full_msg_id, trade)
                    await bot_instance.send_tp_notification(full_msg_id, trade, 'TP1', trade.get('tp1_price', 0))
                    successful_trades.append(f"{pair} {action}{group_label} - TP1 Hit")

                elif action_type == 'tp2hit':
                    current_tp_hits = trade.get('tp_hits', [])
                    if 'TP1' not in current_tp_hits:
                        trade['tp_hits'] = trade.get('tp_hits', []) + ['TP1']
                        await bot_instance.update_trade_in_db(full_msg_id, trade)
                        await bot_instance.send_tp_notification(full_msg_id, trade, 'TP1', trade.get('tp1_price', 0))
                        await asyncio.sleep(1)
                    
                    if 'TP2' not in trade.get('tp_hits', []):
                        trade['tp_hits'] = trade.get('tp_hits', []) + ['TP2']
                    trade['breakeven_active'] = True
                    trade['status'] = 'active (tp2 hit - manual override - breakeven active)'
                    await bot_instance.update_trade_in_db(full_msg_id, trade)
                    await bot_instance.send_tp_notification(full_msg_id, trade, 'TP2', trade.get('tp2_price', 0))
                    successful_trades.append(f"{pair} {action}{group_label} - TP2 Hit")

                elif action_type == 'tp3hit':
                    current_tp_hits = trade.get('tp_hits', [])
                    if 'TP1' not in current_tp_hits:
                        trade['tp_hits'] = trade.get('tp_hits', []) + ['TP1']
                        await bot_instance.update_trade_in_db(full_msg_id, trade)
                        await bot_instance.send_tp_notification(full_msg_id, trade, 'TP1', trade.get('tp1_price', 0))
                        await asyncio.sleep(1)
                    
                    if 'TP2' not in current_tp_hits:
                        trade['tp_hits'] = trade.get('tp_hits', []) + ['TP2']
                        trade['breakeven_active'] = True
                        await bot_instance.update_trade_in_db(full_msg_id, trade)
                        await bot_instance.send_tp_notification(full_msg_id, trade, 'TP2', trade.get('tp2_price', 0))
                        await asyncio.sleep(1)
                    
                    if 'TP3' not in trade.get('tp_hits', []):
                        trade['tp_hits'] = trade.get('tp_hits', []) + ['TP3']
                    trade['status'] = 'completed (tp3 hit - manual override)'
                    await bot_instance.remove_trade_from_db(full_msg_id, 'manual_tp3_hit')
                    await bot_instance.send_tp_notification(full_msg_id, trade, 'TP3', trade.get('tp3_price', 0))
                    successful_trades.append(f"{pair} {action}{group_label} - TP3 Hit")

                elif action_type == 'behit':
                    trade['status'] = 'closed (breakeven after tp2 - manual override)'
                    await bot_instance.remove_trade_from_db(full_msg_id, 'manual_breakeven_hit')
                    await bot_instance.send_breakeven_notification(full_msg_id, trade)
                    successful_trades.append(f"{pair} {action}{group_label} - Breakeven After TP2")

                elif action_type == 'endhit':
                    trade['status'] = 'closed (ended by manual override)'
                    await bot_instance.remove_trade_from_db(full_msg_id, 'manual_end_tracking')
                    successful_trades.append(f"{pair} {action}{group_label} - Tracking Ended")

            except Exception as e:
                failed_trades.append(f"{pair}: {str(e)[:30]}")

        bot_instance.override_trade_mappings.pop(menu_id, None)
        bot_instance.pending_multi_select.pop(menu_id, None)

        total = len(successful_trades) + len(failed_trades)
        if len(successful_trades) == total:
            title = "✅ All Trades Updated Successfully"
        elif len(successful_trades) > 0:
            title = "⚠️ Partial Success"
        else:
            title = "❌ All Trades Failed"

        description = f"**Processed {total} trade(s)**\n\n"
        if successful_trades:
            description += "**✅ Successful:**\n" + "\n".join(
                f"• {t}" for t in successful_trades) + "\n\n"
        if failed_trades:
            description += "**❌ Failed:**\n" + "\n".join(
                f"• {t}" for t in failed_trades)

        await callback_query.message.edit_text(
            f"**{title}**\n\n{description}")
        await bot_instance.log_to_debug(
            f"Manual override completed: {len(successful_trades)} success, {len(failed_trades)} failed"
        )

    await callback_query.answer()
