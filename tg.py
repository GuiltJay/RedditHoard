import os
import asyncio
import hashlib
import sqlite3
from datetime import datetime
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from tqdm import tqdm

# ================= CONFIG =================


API_ID = 21347898                        # Your Telegram API ID (integer)
API_HASH = "98caf2e4f0c25e142c3cbb2e36e683ef"       # Your Telegram API Hash (string)
BOT_TOKENS = ["8424607885:AAHSWoyIiwTsc3gwhkcNJVTQTgFtGn0ca3w","8338190991:AAENGv0u9fH6bicMxUxOnK1I0qhsSmpB1pk"]     # Get from @BotFather
CHANNEL_ID = 7589472315 # -1002965517245     

FOLDER_PATH = "downloads"

MAX_PARALLEL_PER_BOT = 4     # 2–4 safe per bot
MAX_RETRIES = 5
DB_FILE = "upload_state.db"

# ================= FILE TYPES =================

IMAGE_EXT = {'.jpg', '.jpeg', '.png', '.webp', '.bmp'}
VIDEO_EXT = {'.mp4', '.mkv', '.mov', '.webm'}
GIF_EXT = {'.gif'}
AUDIO_EXT = {'.mp3', '.m4a', '.aac', '.wav'}
VOICE_EXT = {'.ogg'}

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


def calculate_hash(filepath):
    sha1 = hashlib.sha1()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024 * 1024):
            sha1.update(chunk)
    return sha1.hexdigest()


def already_uploaded(conn, file_hash):
    c = conn.cursor()
    c.execute("SELECT 1 FROM uploaded WHERE hash=?", (file_hash,))
    return c.fetchone() is not None


def mark_uploaded(conn, file_hash, filename):
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO uploaded VALUES (?, ?, ?)",
        (file_hash, filename, datetime.utcnow().isoformat())
    )
    conn.commit()

# ================= UPLOADER =================

async def multi_bot_media_uploader():

    conn = init_db()

    files = []
    for filename in os.listdir(FOLDER_PATH):
        path = os.path.join(FOLDER_PATH, filename)
        if os.path.isfile(path):
            files.append((path, filename))

    if not files:
        print("No files found.")
        return

    total_files = len(files)
    queue = asyncio.Queue()

    for file in files:
        await queue.put(file)

    sent = 0
    skipped = 0
    failed = 0

    counter_lock = asyncio.Lock()

    async def send_media(app, file_path, filename):

        ext = os.path.splitext(filename)[1].lower()

        if ext in IMAGE_EXT:
            await app.send_photo(
                CHANNEL_ID,
                file_path,
                caption=filename
            )

        elif ext in VIDEO_EXT:
            await app.send_video(
                CHANNEL_ID,
                file_path,
                caption=filename,
                supports_streaming=True
            )

        elif ext in GIF_EXT:
            await app.send_animation(
                CHANNEL_ID,
                file_path,
                caption=filename
            )

        elif ext in AUDIO_EXT:
            await app.send_audio(
                CHANNEL_ID,
                file_path,
                caption=filename
            )

        elif ext in VOICE_EXT:
            await app.send_voice(
                CHANNEL_ID,
                file_path,
                caption=filename
            )

        else:
            await app.send_document(
                CHANNEL_ID,
                file_path,
                caption=filename
            )

    async def bot_worker(bot_token, bot_index):

        nonlocal sent, skipped, failed

        app = Client(
            name=f"media_bot_{bot_index}",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=bot_token
        )

        semaphore = asyncio.Semaphore(MAX_PARALLEL_PER_BOT)

        async with app:

            while True:
                try:
                    file_path, filename = await queue.get()
                except:
                    break

                file_hash = calculate_hash(file_path)

                if already_uploaded(conn, file_hash):
                    async with counter_lock:
                        skipped += 1
                    queue.task_done()
                    continue

                retries = 0
                success = False

                while retries <= MAX_RETRIES:
                    try:
                        async with semaphore:
                            await send_media(app, file_path, filename)

                        mark_uploaded(conn, file_hash, filename)

                        async with counter_lock:
                            sent += 1

                        success = True
                        break

                    except FloodWait as e:
                        retries += 1
                        await asyncio.sleep(e.value + 1)

                    except RPCError:
                        async with counter_lock:
                            failed += 1
                        break

                    except Exception:
                        async with counter_lock:
                            failed += 1
                        break

                if not success:
                    async with counter_lock:
                        failed += 1

                queue.task_done()

    workers = [
        asyncio.create_task(bot_worker(token, idx))
        for idx, token in enumerate(BOT_TOKENS)
    ]

    with tqdm(total=total_files, desc="🎬 Multi-Bot Media Upload", unit="file") as pbar:

        last_count = 0

        while any(not w.done() for w in workers):
            await asyncio.sleep(0.5)

            current = sent + skipped + failed
            delta = current - last_count

            if delta > 0:
                pbar.update(delta)
                last_count = current

            pbar.set_postfix({
                "Sent": sent,
                "Skipped": skipped,
                "Failed": failed
            })

        final_count = sent + skipped + failed
        pbar.update(final_count - last_count)

    await asyncio.gather(*workers)

    conn.close()

    print("\n🔥 MEDIA UPLOAD COMPLETE")
    print("Sent:", sent)
    print("Skipped:", skipped)
    print("Failed:", failed)


if __name__ == "__main__":
    asyncio.run(multi_bot_media_uploader())
