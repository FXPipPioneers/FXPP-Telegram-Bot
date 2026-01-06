# Userbot Activation Guide (Windows)

Follow these steps to activate your userbot using your own computer's trusted internet connection.

### Step 1: Install Required Libraries
Open your Windows PowerShell and run this command:
```powershell
python -m pip install pyrogram
```

### Step 2: Update your GitHub & Render
1. **Download** the latest `telegram_bot_github.zip` from Replit.
2. **Extract and Push** all files to your GitHub.
3. Wait for Render to show your service as **"Live"**.

### Step 3: Generate your Session String
In your PowerShell, run the generator script:
```powershell
python "C:\Users\zedmi\Desktop\FX Pip Pioneers\FXPP BOT\Telegram bot python files\TG bot met userbot - web app versi\generate_session.py"
```
*   **Follow the prompts**: Enter API ID, Hash, and Phone.
*   **Enter the code**: From your Telegram app.
*   **Copy the result**: Copy the very long "Session String" it gives you.

### Step 4: Activate the Bot
1. Go to your main bot on Telegram.
2. Send this command:
   ```
   /setsession YOUR_LONG_STRING_HERE
   ```
3. The bot will reply: **"✅ Success! Userbot session has been manually updated."**

### Step 5: Automated Sync
* The **Userbot Service** on Render will detect the session in the database within 30 seconds and log in automatically.
* It is now permanently synced and will survive future restarts or updates.
