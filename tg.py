import os
import asyncio
import hashlib
import sqlite3
from datetime import datetime
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from tqdm import tqdm

# ================= CONFIG =================

MAX_PARALLEL_UPLOADS = 4      # 3–5 safe for most bots
MAX_RETRIES = 5
UPLOAD_AS_DOCUMENT = True     # MUCH faster for videos
DB_FILE = "upload_state.db"

# ================= FILE TYPE MAP =================

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.bmp', '.webp', '.tiff'}
VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm'}
AUDIO_EXTENSIONS = {'.mp3', '.wav', '.ogg', '.flac', '.m4a'}
GIF_EXTENSIONS = {'.gif'}

# ================= DATABASE =================

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS uploaded (
            hash TEXT PRIMARY KEY,
            filename TEXT,
            uploaded_at TEXT
        )
    """)
    conn.commit()
    return conn


def file_hash(filepath):
    h = hashlib.sha1()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def already_uploaded(conn, hash_value):
    c = conn.cursor()
    c.execute("SELECT 1 FROM uploaded WHERE hash=?", (hash_value,))
    return c.fetchone() is not None


def mark_uploaded(conn, hash_value, filename):
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO uploaded VALUES (?, ?, ?)",
        (hash_value, filename, datetime.utcnow().isoformat())
    )
    conn.commit()


# ================= ULTRA UPLOADER =================

async def ultra_upload(folder_path, api_id, api_hash, bot_token, channel_id):

    conn = init_db()

    app = Client("ultra_uploader", api_id, api_hash, bot_token=bot_token)

    files = []
    for filename in os.listdir(folder_path):
        path = os.path.join(folder_path, filename)
        if os.path.isfile(path):
            if os.path.getsize(path) <= 2000 * 1024 * 1024:
                files.append((path, filename))

    if not files:
        print("No files to upload.")
        return

    semaphore = asyncio.Semaphore(MAX_PARALLEL_UPLOADS)

    sent = 0
    skipped = 0
    failed = 0

    async def upload_single(path, filename):
        nonlocal sent, skipped, failed

        file_sha = file_hash(path)

        if already_uploaded(conn, file_sha):
            skipped += 1
            return

        retries = 0

        while retries <= MAX_RETRIES:
            try:
                async with semaphore:

                    if UPLOAD_AS_DOCUMENT:
                        await app.send_document(
                            chat_id=channel_id,
                            document=path,
                            caption=filename
                        )
                    else:
                        ext = os.path.splitext(filename)[1].lower()

                        if ext in IMAGE_EXTENSIONS:
                            await app.send_photo(channel_id, path, caption=filename)
                        elif ext in VIDEO_EXTENSIONS:
                            await app.send_video(channel_id, path, caption=filename)
                        elif ext in AUDIO_EXTENSIONS:
                            await app.send_audio(channel_id, path, caption=filename)
                        elif ext in GIF_EXTENSIONS:
                            await app.send_animation(channel_id, path, caption=filename)
                        else:
                            await app.send_document(channel_id, path, caption=filename)

                mark_uploaded(conn, file_sha, filename)
                sent += 1
                return

            except FloodWait as e:
                retries += 1
                await asyncio.sleep(e.value + 1)

            except RPCError:
                failed += 1
                return

            except Exception:
                failed += 1
                return

        failed += 1

    async with app:

        tasks = [upload_single(path, filename) for path, filename in files]

        with tqdm(total=len(tasks), desc="🚀 Uploading", unit="file") as pbar:
            for coro in asyncio.as_completed(tasks):
                await coro
                pbar.update(1)
                pbar.set_postfix({
                    "Sent": sent,
                    "Skipped": skipped,
                    "Failed": failed
                })

    conn.close()

    print("\n🔥 ULTRA UPLOAD COMPLETE")
    print("Sent:", sent)
    print("Skipped (already uploaded):", skipped)
    print("Failed:", failed)


# ================= RUN =================

if __name__ == "__main__":

    API_ID = 21347898                        # Your Telegram API ID (integer)
    API_HASH = "98caf2e4f0c25e142c3cbb2e36e683ef"       # Your Telegram API Hash (string)
    BOT_TOKEN = "8424607885:AAHSWoyIiwTsc3gwhkcNJVTQTgFtGn0ca3w"     # Get from @BotFather
    CHANNEL_ID = -1002965517245         # or numeric ID like -1001234567890
    
    asyncio.run(
        ultra_upload(
            folder_path="downloads",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            channel_id=CHANNEL_ID
        )
    )
