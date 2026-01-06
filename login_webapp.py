from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import os
import asyncio
import sys
import warnings

# Suppress deprecated warnings for Python 3.14+
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Windows compatibility fix
if sys.platform == 'win32':
    if sys.version_info < (3, 12):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass

# Create the loop before importing pyrogram
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

from pyrogram import Client
import asyncpg
import ssl

app = Flask(__name__)
CORS(app)

# Use same credentials as main bot
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Template for the login page
LOGIN_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>FX Pip Pioneers Userbot Login</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: #f0f2f5; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
        .card { background: white; padding: 2rem; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); width: 100%; max-width: 400px; }
        h2 { margin-top: 0; color: #1c1e21; }
        input { width: 100%; padding: 12px; margin: 8px 0; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
        button { width: 100%; padding: 12px; background: #0088cc; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; margin-top: 10px; }
        button:hover { background: #0077b5; }
        #status { margin-top: 15px; font-size: 14px; color: #666; }
        .hidden { display: none; }
    </style>
</head>
<body>
    <div class="card">
        <h2>Userbot Login</h2>
        <div id="step-phone">
            <input type="text" id="phone" placeholder="Phone (e.g. +1234567890)">
            <button onclick="sendCode()">Send Code</button>
        </div>
        <div id="step-code" class="hidden">
            <input type="text" id="code" placeholder="5-digit code">
            <button onclick="verifyCode()">Verify & Login</button>
        </div>
        <div id="step-password" class="hidden">
            <input type="password" id="password" placeholder="2FA Password">
            <button onclick="verifyPassword()">Submit Password</button>
        </div>
        <div id="status"></div>
    </div>

    <script>
        let phoneCodeHash = '';
        let phoneNumber = '';

        async function sendCode() {
            phoneNumber = document.getElementById('phone').value;
            const status = document.getElementById('status');
            status.innerText = 'Requesting code...';
            
            try {
                const response = await fetch('/api/send-code', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ phone: phoneNumber })
                });
                const data = await response.json();
                if (data.success) {
                    phoneCodeHash = data.hash;
                    document.getElementById('step-phone').classList.add('hidden');
                    document.getElementById('step-code').classList.remove('hidden');
                    status.innerText = 'Code sent to your Telegram!';
                } else {
                    status.innerText = 'Error: ' + data.error;
                }
            } catch (e) {
                status.innerText = 'Failed to connect to server.';
            }
        }

        async function verifyCode() {
            const code = document.getElementById('code').value;
            const status = document.getElementById('status');
            status.innerText = 'Verifying...';
            
            try {
                const response = await fetch('/api/verify-code', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ phone: phoneNumber, hash: phoneCodeHash, code: code })
                });
                const data = await response.json();
                if (data.success) {
                    status.innerText = '✅ Success! You can close this window.';
                } else if (data.needs_password) {
                    document.getElementById('step-code').classList.add('hidden');
                    document.getElementById('step-password').classList.remove('hidden');
                    status.innerText = '2FA Password required.';
                } else {
                    status.innerText = 'Error: ' + data.error;
                }
            } catch (e) {
                status.innerText = 'Failed to connect to server.';
            }
        }

        async function verifyPassword() {
            const password = document.getElementById('password').value;
            const status = document.getElementById('status');
            status.innerText = 'Checking password...';
            
            try {
                const response = await fetch('/api/verify-password', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ phone: phoneNumber, password: password })
                });
                const data = await response.json();
                if (data.success) {
                    status.innerText = '✅ Success! You can close this window.';
                } else {
                    status.innerText = 'Error: ' + data.error;
                }
            } catch (e) {
                status.innerText = 'Failed to connect to server.';
            }
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(LOGIN_HTML)

# Store active clients in memory during the login process
active_clients = {}

@app.post('/api/send-code')
async def api_send_code():
    data = request.json
    phone = data.get('phone')
    
    try:
        client = Client(
            f"web_login_{phone}",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            in_memory=True,
            device_model="PC 64bit",
            system_version="Linux 6.8.0-1043-aws",
            app_version="2.1.0",
            lang_code="en",
            system_lang_code="en-US"
        )
        await client.connect()
        sent_code = await client.send_code(phone)
        active_clients[phone] = client
        return jsonify({"success": True, "hash": sent_code.phone_code_hash})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.post('/api/verify-code')
async def api_verify_code():
    data = request.json
    phone = data.get('phone')
    hash = data.get('hash')
    code = data.get('code')
    
    client = active_clients.get(phone)
    if not client:
        return jsonify({"success": False, "error": "Session expired. Please restart."})
    
    try:
        try:
            await client.sign_in(phone, hash, code)
        except Exception as e:
            if "SESSION_PASSWORD_NEEDED" in str(e):
                return jsonify({"success": True, "needs_password": True})
            raise e
        
        # If success, export session and save to DB
        session_string = await client.export_session_string()
        await save_session(session_string)
        await client.disconnect()
        del active_clients[phone]
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.post('/api/verify-password')
async def api_verify_password():
    data = request.json
    phone = data.get('phone')
    password = data.get('password')
    
    client = active_clients.get(phone)
    if not client:
        return jsonify({"success": False, "error": "Session expired."})
    
    try:
        await client.check_password(password)
        session_string = await client.export_session_string()
        await save_session(session_string)
        await client.disconnect()
        del active_clients[phone]
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

async def save_session(session_string):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    pool = await asyncpg.create_pool(DATABASE_URL, ssl=ctx)
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO bot_settings (setting_key, setting_value)
            VALUES ('userbot_session_string', $1)
            ON CONFLICT (setting_key) DO UPDATE SET setting_value = $1
        """, session_string)
    await pool.close()

if __name__ == '__main__':
    # Flask app will run on port 5001 to not conflict with main bot if run locally
    # But on Render, we would run it as a separate process or service
    app.run(host='0.0.0.0', port=5001)
