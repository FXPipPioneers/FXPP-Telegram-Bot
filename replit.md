# FX Pip Pioneers Trading Bot

## Overview
A professional Telegram trading bot for distributing forex/crypto trading signals, automatically calculating Take Profit (TP) and Stop Loss (SL) levels, and managing VIP trial memberships.

## Current Architecture
The project is maintained as a single-file application (`telegram_bot.py`) for simplified deployment on Render.

## Core Functionality
- Trading signal creation and distribution to VIP/Free Telegram groups.
- Real-time price tracking with automatic TP/SL hit detection.
- VIP trial membership system with exactly 3 trading days duration (Mon-Fri).
- Automated DM sequences (welcome, warnings, follow-ups).
- Engagement tracking rewarding active free members with discounts.

## Environment Variables
- `TELEGRAM_BOT_TOKEN`: Bot token from @BotFather
- `TELEGRAM_API_ID`: From my.telegram.org
- `TELEGRAM_API_HASH`: From my.telegram.org
- `BOT_OWNER_USER_ID`: Telegram user ID of bot owner
- `DATABASE_URL`: PostgreSQL connection string
- `VIP_GROUP_ID`: Telegram group ID for VIP members
- `FREE_GROUP_ID`: Telegram group ID for free members
- `DEBUG_GROUP_ID`: Telegram group ID for system logs
- `CURRENCYBEACON_KEY`: Price API Key
- `EXCHANGERATE_API_KEY`: Price API Key
- `CURRENCYLAYER_KEY`: Price API Key
- `ABSTRACTAPI_KEY`: Price API Key
- `FXAPI_KEY`: Price API Key
