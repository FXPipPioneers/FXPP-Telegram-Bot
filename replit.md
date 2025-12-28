# FX Pip Pioneers Trading Bot

## Overview

A professional Telegram trading bot for distributing forex/crypto trading signals, automatically calculating Take Profit (TP) and Stop Loss (SL) levels, and managing VIP trial memberships. The bot tracks live prices, detects TP/SL hits, and handles community engagement through automated DMs and trial management.

**Core Functionality:**
- Trading signal creation and distribution to VIP/Free Telegram groups
- Real-time price tracking with automatic TP/SL hit detection
- VIP trial membership system with 3 trading day duration
- Automated DM sequences (welcome, warnings, follow-ups)
- Peer ID escalation for reliable message delivery

## User Preferences

Preferred communication style: Simple, everyday language.

**Working Style:** Always work task by task. Do NOT ask to switch modes or suggest using Autonomous Mode. Always find the best way to work within current constraints and take the time needed to complete each task properly.

**Important Configuration:**
- Bot owner restricted to Telegram user ID via `BOT_OWNER_USER_ID` environment variable
- TP/SL pip values: TP1=20, TP2=40, TP3=70, SL=50 pips
- Trial duration: Exactly 3 trading days (Mon-Fri only), expires at 22:59 Amsterdam time
- Welcome DM uses escalating peer ID verification (30min → 1hr → 3hr → 24hr)

## System Architecture

### Modular Structure
The codebase follows a clean modular architecture in `src/`:

```
src/
├── main.py                    # Entry point, bot initialization
├── features/
│   ├── core/                  # Config, logging, background engine
│   ├── database/              # PostgreSQL manager and schema
│   ├── trading/               # Signal parsing, price tracking, hit detection
│   ├── community/             # Member handlers, DM manager, trial manager
│   ├── commands/              # 12 individual command modules
│   └── loops/                 # 9 background task loops
```

### Bot Framework
- **Pyrogram** client for Telegram API interaction
- Async/await patterns throughout for non-blocking operations
- Owner-only command restriction via `BOT_OWNER_USER_ID` check

### Background Loops (9 total)
1. **Price Tracking** - Monitors active trades every 4-8 minutes
2. **Peer ID Escalation** - Retries DM delivery with increasing delays
3. **Trial Expiry** - Removes expired VIP trial members
4. **Pre-expiration Warnings** - Sends 24hr and 3hr warnings
5. **Follow-up DMs** - Sends 3/7/14-day post-trial messages
6. **Retry Welcome DMs** - Retries failed welcome messages
7. **Monday Activation** - Activates weekend-delayed trials
8. **Engagement Tracking** - Rewards engaged free group members
9. **Daily Trial Offers** - Offers VIP trials at 09:00 Amsterdam time

### Trading Engine
- **SignalParser** - Extracts trade data from formatted messages
- **HitDetector** - Processes price updates, validates TP/SL hits chronologically
- **TradingEngine** - Orchestrates signal creation, price updates, and notifications
- Breakeven logic activates after TP2 hit
- Excluded pairs for manual tracking: XAUUSD, BTCUSD, GER40, US100

### Data Flow
1. Owner creates signal via `/entry` command
2. Bot calculates TP/SL levels based on pip configuration
3. Signal posted to VIP/Free groups
4. Price tracking loop monitors live prices
5. Hit detection triggers notifications and updates database

## External Dependencies

### Database
- **PostgreSQL** via asyncpg connection pooling
- Hosted on Render PostgreSQL (managed instance)
- Connection string via `DATABASE_URL` environment variable
- Key tables: `active_trades`, `completed_trades`, `active_members`, `peer_id_checks`, `dm_schedule`, `vip_trial_activations`

### Telegram API
- **Pyrogram 2.0.106** with TgCrypto for MTProto
- Requires: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`
- Group IDs: `VIP_GROUP_ID`, `FREE_GROUP_ID`, `DEBUG_GROUP_ID`

### Price Data APIs
- Multiple forex/crypto price APIs with automatic fallback mechanism
- If primary API fails, cycles through alternatives before giving up

### Environment Variables Required
```
TELEGRAM_BOT_TOKEN      # Bot token from @BotFather
TELEGRAM_API_ID         # From my.telegram.org
TELEGRAM_API_HASH       # From my.telegram.org
BOT_OWNER_USER_ID       # Telegram user ID of bot owner
DATABASE_URL            # PostgreSQL connection string
VIP_GROUP_ID            # Telegram group ID for VIP members
FREE_GROUP_ID           # Telegram group ID for free members
DEBUG_GROUP_ID          # Telegram group ID for system logs
```

### Timezone
- **pytz** for Amsterdam timezone handling (`Europe/Amsterdam`)
- All trial calculations and scheduled tasks use Amsterdam time