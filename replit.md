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
The bot is implemented as a high-performance, single-file asynchronous Python application (`telegram_bot.py`) optimized for 24/7 deployment on Render.com.

### 1. Trading Signal Engine
- **Multi-Pair Support**: Tracks Gold (XAUUSD), major Forex pairs, and Crypto.
- **Automated TP/SL**: Real-time price monitoring using multiple APIs (CurrencyBeacon, FXAPI, etc.) to detect when profit targets or stop losses are hit.
- **Instant Alerts**: Notifies groups immediately upon price action events.

### 2. VIP & Trial System
- **72-Hour VIP Trial**: New members receive exactly 3 trading days (Mon-Fri) of VIP access.
- **Peer ID Verification**: A sophisticated background process that ensures the bot can send DMs to new members by verifying Telegram Peer IDs after a 30-minute delay.
- **Automated Expiration**: Automatically removes users from VIP groups and sends expiration warnings (24h and 3h before).

### 3. CRM & Automated Messaging
- **Welcome DMs**: Personalized onboarding messages sent to every new member.
- **Retention DMs**: Scheduled follow-up messages at 3, 7, and 14 days post-expiration.
- **Engagement Rewards**: Tracks messages in free groups; members reaching 50+ messages qualify for a 15% discount code.

## Development & Deployment Guidelines

### Environment Variables (Managed on Render)
- `TELEGRAM_BOT_TOKEN`: Core bot authentication.
- `TELEGRAM_API_ID` / `TELEGRAM_API_HASH`: Telegram API credentials.
- `DATABASE_URL`: PostgreSQL connection string.
- `VIP_GROUP_ID` / `FREE_GROUP_ID`: Target distribution channels.
- `BOT_OWNER_USER_ID`: Master control ID.
- `PRICE_API_KEYS`: Multiple keys for redundant price tracking.

### Deployment Process (GitHub -> Render)
**CRITICAL**: Every time a code change is made in Replit, the deployment package must be updated.
1. Make code edits in `telegram_bot.py`.
2. **Update the Zip**: Run the command to recreate `telegram_bot_github.zip`.
   - **IMPORTANT**: Ensure only necessary files (`telegram_bot.py`, `requirements.txt`, `render.yaml`, `replit.md`, and `userbot_service.py`) are included in the zip. Do not include documentation or asset folders.
3. Download the zip, extract, and push the updated files to your GitHub repository.
4. Render will automatically detect the GitHub push and redeploy the bot.

## File Structure
- `telegram_bot.py`: The entire bot logic, handlers, and background tasks.
- `requirements.txt`: Python dependencies.
- `render.yaml`: Infrastructure-as-code for Render deployment.
- `replit.md`: This project documentation.
- `telegram_bot_github.zip`: The ready-to-deploy package.
