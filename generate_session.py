import asyncio
import os
import sys

# Windows compatibility fix for Python 3.8+
if sys.platform == 'win32':
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

from pyrogram import Client

async def generate_session():
    print("FX Pip Pioneers - Userbot Session Generator")
    print("------------------------------------------")
    
    api_id = input("Enter your API_ID: ")
    api_hash = input("Enter your API_HASH: ")
    phone = input("Enter your phone number (with + and country code): ")

    # Use the same fingerprint as the main bot for consistency
    client = Client(
        "temp_session",
        api_id=int(api_id),
        api_hash=api_hash,
        device_model="PC 64bit",
        system_version="Linux 6.8.0-1043-aws",
        app_version="2.1.0",
        lang_code="en",
        system_lang_code="en-US",
        in_memory=True,
        # Avoid tgcrypto requirement for local generation if it fails to build
        workers=1
    )

    await client.connect()
    
    try:
        code_hash = await client.send_code(phone)
        phone_code = input("Enter the 5-digit code you received: ")
        
        try:
            await client.sign_in(phone, code_hash.phone_code_hash, phone_code)
        except Exception as e:
            if "SESSION_PASSWORD_NEEDED" in str(e):
                password = input("Enter your 2FA password: ")
                await client.check_password(password)
            else:
                raise e
                
        session_string = await client.export_session_string()
        print("\n------------------------------------------")
        print("✅ SUCCESS! Copy the session string below:")
        print("------------------------------------------\n")
        print(session_string)
        print("\n------------------------------------------")
        print("Now use the /setsession command in your bot to save it.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    # Explicitly create and set the event loop for Python 3.12+ / 3.14
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(generate_session())
    finally:
        loop.close()
