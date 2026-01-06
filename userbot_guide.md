# FX Pip Pioneers Userbot Documentation

## Overview
The Userbot is a specialized Pyrogram-based client that runs alongside the main Telegram bot. It operates as a real user account rather than a bot, allowing it to bypass specific limitations of the Telegram Bot API—most notably the ability to initiate Direct Messages (DMs) with users who have not previously started a conversation with the bot.

## Core Purpose & Migration
Previously, the main bot handled all user interactions. However, many new members joined the Telegram group but forgot to "start" the bot, making it impossible for the bot to send them welcome messages or trial expiration warnings.

The Userbot was introduced to handle:
1.  **Direct Onboarding**: Messaging new members as soon as they join the VIP or Free groups.
2.  **Reliable Notifications**: Ensuring trial expiration warnings and follow-ups are delivered even if the user hasn't interacted with the main bot.
3.  **Peer ID Discovery**: Automatically discovering and caching user "Peer IDs" which are required for Telegram to allow private messaging.

## Features Handled by Userbot (vs. Main Bot)
Based on the transition from `OLD_telegram_bot.py`:

| Feature | Previously (Main Bot) | Now (Userbot) |
| :--- | :--- | :--- |
| **Welcome DMs** | Could only message users who clicked /start. | Can message any user immediately upon joining. |
| **Trial Warnings** | Frequently failed if the user was "inactive" to the bot. | High delivery rate to all trial members. |
| **Follow-up Sequences** | Restricted to bot-interactive users. | Sent to all expired members (3, 7, 14 days). |
| **Peer ID Verification** | Handled via message tracking in groups. | Actively checks group participants to "see" users. |

## DM Sequences
The Userbot manages the following automated message flows:
1.  **Welcome Message**: Sent within 30 minutes of a user joining the community.
2.  **Trial Start**: Confirmation of the 72-hour VIP trial activation.
3.  **Expiration Warnings**:
    *   **24-Hour Warning**: Sent one day before VIP access ends.
    *   **3-Hour Warning**: Final reminder with a purchase link.
4.  **Retention DMs**: Follow-ups sent at specific intervals (3, 7, and 14 days) after a subscription expires to encourage re-subscription.

## Setup & Environment Variables
The Userbot requires its own set of credentials to run as a user account:
- `USERBOT_PHONE`: The phone number associated with the Telegram account (international format).
- `USERBOT_API_ID` & `USERBOT_API_HASH`: Obtained from [my.telegram.org](https://my.telegram.org).
- `userbot_session_string`: (Managed internally) Stored in the PostgreSQL database under the `bot_settings` table to maintain login state between restarts.

### Login Flow
1.  Owner triggers `/login` in the main bot.
2.  The bot presents a **Widget-style Interface** using inline buttons:
    *   **"🔑 Start Login"**: Initiates the session request.
    *   **"📊 Check Status"**: A subcommand shortcut to verify if the Userbot is currently online and healthy.
3.  If starting a login, the bot requests a code from Telegram.
4.  Owner receives the code on their Telegram app and sends it to the bot.
5.  Userbot authenticates and saves its session to the database.

### Subcommands & Interactions
The `/login` command now acts as a central hub with two primary subcommands:
1.  **`/login setup`**: This is the main setup command that triggers the login process, requests the session from Telegram, and handles the 2FA code input.
2.  **`/login status`**: A quick command to check the current connection status and health of the Userbot.

## Debugging & Logging
The Userbot sends real-time status updates to the `DEBUG_GROUP_ID`:
- **Login Status**: Notifies when the userbot connects or fails.
- **Message Success/Failure**: Logs whether a DM was successfully delivered or blocked (e.g., if a user has "Everyone" disabled for DMs).
- **Task completion**: Signals when a scheduled DM sequence (like a 3-day follow-up) has been processed.

## Important Limitations & Safety
- **Anti-Spam Compliance**: The Userbot includes built-in delays (randomized between 5-15 seconds) between messages to avoid Telegram's spam filters.
- **Privacy Settings**: If a user has "Everyone" disabled for private messages, the Userbot will log a "Privacy Violation" error in the debug group. This is normal and prevents the account from being flagged.
- **Session Persistence**: The session is tied to the `userbot_session_string`. If you manually log out from your Telegram app settings, this string will become invalid and you will need to run `/login setup` again.
- **Human-Like Behavior**: The Userbot is programmed to avoid sending more than 20-30 messages per hour to maintain account safety.

## Technical Maintenance
The Userbot runs in the same event loop as the main bot but maintains its own connection. On Render.com, if the service restarts, the bot automatically retrieves the `userbot_session_string` from the database to log back in without needing a new code from the owner.

## Current Technical Issues & Troubleshooting (Jan 2026)

### 1. Current Progress & Working Features
*   **Command Structure**: The `/login` command and its subcommands (`setup`, `status`) are fully implemented and functional.
*   **Database Integration**: The system correctly checks for existing sessions in the PostgreSQL database.
*   **State Machine**: A robust login state machine is in place to handle the transition between requesting a code and verifying it.
*   **Owner Validation**: The bot correctly identifies the owner using environment variables, and now includes a hardcoded safety fallback (ID: `6664440870`) to ensure commands are always processed for you.
*   **Official Fingerprinting & Location Spoofing**: The login client is configured with a trusted Linux/PC-based configuration: Device `PC 64bit`, System `Linux 6.8.0-1043-aws`, and App Version `2.1.0`. This profile has proven to be the most reliable for triggering code delivery.
*   **Verification Interception**: The bot now prioritizes the capture of any 5-digit code sent by any user currently in a login state, bypassing strict owner checks to avoid ID mismatch issues.
*   **Tactical Logging**: A numbered step-by-step logging system (Steps 1-11) is active in the code to pinpoint the exact moment of failure during the final handshake.

### 2. Ongoing Maintenance Protocol
**Note for Developer**: Every time a new fix is attempted for the login/verification issue, it must be documented here immediately.
- If a fix **fails**: Add it to the "Attempted Fixes" list below.
- If a fix **succeeds**: Move the feature to "Current Progress & Working Features" and clear relevant items from the "Attempted Fixes" list.

### 3. The Current Blocker: Handshake Finalization
While the **Telegram verification code is now being delivered and captured** by the bot, the challenge is ensuring the `sign_in` process completes successfully once the code is provided.

*   **Symptoms**: The owner receives the code and sends it to the bot. The bot captures it (confirmed by Step-by-Step logs), but previously it might have timed out or ignored the code due to ID mismatch.
*   **Status**: The latest fix addresses the ID mismatch and adds a 3-second stability delay before the final sign-in call.

### 4. Attempted Fixes (What hasn't worked yet)
*   **Standard Client Initialization**: Initial attempts used default Pyrogram client settings, which Telegram flagged as suspicious.
*   **Mobile Device Fingerprinting**: Attempting to spoof a Samsung Galaxy device caused Telegram to withhold the verification code. Reverted to a stable Linux/PC profile.
*   **Immediate Sign-in**: Calling `sign_in` the instant the message was received was causing handshake drops. Now using a 3-second delay and explicit connection checks.

### 7. CURRENT METHOD: Local Session Generation (Jan 6, 2026)
To bypass Render's IP reputation issues and Telegram's silent code blocking, we are now using a **Local Session Generator**. This allows you to perform the initial handshake on your trusted home internet and provide the final "session string" to the bot.

**Why this works:**
*   **Trusted IP**: Telegram sees a login from your home PC, which is much more trusted than a data center.
*   **Bypasses Interceptor**: No more worrying about the bot "catching" the code correctly; you enter it directly into the local script.
*   **Permanent**: Once the string is generated, it's saved to the database and survives all future Render restarts.

**Instructions:**
1. Use `generate_session.py` on your computer.
2. Provide the result via the `/setsession` command in Telegram.
3. Detailed steps are found in `Userbot_activation_guide.md`.

### 8. FUTURE: Login Web App (Jan 6, 2026)
We have also prepared `login_webapp.py` as a web interface for login. 
*   **Status**: Built but not yet active in the main workflow.
*   **Purpose**: To provide a simple web form for entering your phone and code.
*   **Note**: Using the **Local Generator (Step 7)** is currently recommended over the web app for maximum reliability.
