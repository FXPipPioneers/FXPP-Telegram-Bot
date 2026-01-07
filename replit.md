# FX Pip Pioneers Trading Bot

## Overview
FX Pip Pioneers is a professional Telegram ecosystem designed for high-performance forex and crypto trading signal distribution. The bot manages a complex workflow including signal broadcasting, real-time price tracking, automated risk management (TP/SL), and a comprehensive VIP membership system.

## Project Purpose
The primary goal is to provide a seamless, automated experience for both free and VIP members. The bot acts as the central hub for:
- **Signal Distribution**: Broadcasting high-quality trading setups to both Free and VIP Telegram groups.
- **Trial Management**: Automatically onboarding new members with a 72-hour VIP trial and managing their transition to paid tiers.
- **Engagement Tracking**: Monitoring member activity to reward active community members with exclusive discounts.
- **Retention**: Automated DM sequences (welcome, follow-up, and expiration warnings) to maximize user lifetime value.

## Core Features & Technical Architecture
The system is divided into two main components optimized for 24/7 deployment on Render.com:
1.  **Main Bot (`telegram_bot.py`)**: Handles signals, group management, and administrative control.
2.  **Userbot Service (`userbot_service.py`)**: Centralized engine that exclusively handles all private messaging and member discovery.

### Task Delegation: Main Bot vs. Userbot
To ensure high performance and bypass Telegram API limitations, tasks are strictly divided:

**Main Bot (`telegram_bot.py`) Tasks:**
- **Signal Engine**: Tracks prices, handles TP/SL, and broadcasts signals to groups.
- **Admin Control**: Handles owner commands (`/entry`, `/activetrades`, `/memberdatabase`).
- **Event Monitoring**: Watches for join requests and new members entering groups.
- **Logging**: Sends system-wide status updates to the Debug Group.
- **Database Entry**: Creates the initial records for new members and queues DM tasks.

**Userbot Service (`userbot_service.py`) Tasks:**
- **Centralized DM Engine**: Exclusively handles ALL private messaging (Direct Messages) to users.
- **Welcome Sequence**: Sends onboarding messages with a deliberate 10-minute delay.
- **Trial Management DMs**: Instantly sends trial start confirmations and expiration warnings (24h and 3h).
- **Automated Retention DMs**: Triggers follow-up messages (3, 7, 14 days post-expiration) instantly.
- **Engagement Rewards**: Sends discount codes to active community members.
- **Peer Discovery**: Periodically scans group members to establish the "Peer IDs" required for messaging.
- **Health Checks**: Sends its own startup and connection status to the Debug Group, including critical disconnect alerts.

### 1. Trading Signal Engine
- **Multi-Pair Support**: Tracks Gold (XAUUSD), major Forex pairs, and Crypto.
- **Automated TP/SL**: Real-time price monitoring using multiple APIs to detect when profit targets or stop losses are hit.
- **Instant Alerts**: Notifies groups immediately upon price action events.

### 2. VIP & Trial System
- **72-Hour VIP Trial**: New members receive exactly 3 trading days (Mon-Fri) of VIP access.
- **Userbot Onboarding**: New members receive a Welcome DM exactly 10 minutes after joining the Free group.
- **Automated Expiration**: Automatically removes users from VIP groups and sends expiration warnings (24h and 3h before).

### 3. CRM & Automated Messaging
- **Centralized DM Queue**: All private messages are handled by the Userbot to ensure high delivery rates.
- **Retention DMs**: Scheduled follow-up messages at 3, 7, and 14 days post-expiration.
- **Admin Controls**: A widget-based `/memberdatabase` command allows admins to toggle member tracking (ON/OFF) during high-traffic promotions to prevent spam filters.

## Development & Deployment Guidelines

**CRITICAL NOTE**: This project is developed on Replit but **runs exclusively on Render.com**. 

### Deployment Process (GitHub -> Render)
1. Make code edits in Replit.
2. **Update the Zip**: Recreate `telegram_bot_github.zip` containing `telegram_bot.py`, `userbot_service.py`, `requirements.txt`, `render.yaml`, `generate_session.py`, and `login_webapp.py`.
3. Push the updated files to your GitHub repository.
4. Render will automatically detect the push and redeploy both the Bot and Userbot services.

### Monitoring & Safety
- **Debug Group**: All status updates, errors, and admin actions are logged to a dedicated Debug Group.
- **Disconnect Alerts**: The Userbot service automatically tags the owner in the Debug Group if a fatal disconnect occurs.
- **Self-Healing DB**: The system automatically manages and repairs database schema mismatches (e.g., column renames) on the fly.

## File Structure
- `telegram_bot.py`: Main bot logic and group management.
- `userbot_service.py`: Background DM engine and peer discovery.
- `requirements.txt`: Python dependencies.
- `render.yaml`: Infrastructure configuration for Render.
- `generate_session.py`: Tool for generating Pyrogram session strings locally.
- `login_webapp.py`: Web interface for userbot authentication.
- `replit.md`: This project documentation.
- `telegram_bot_github.zip`: The ready-to-deploy package.
