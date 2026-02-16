import os
import asyncio
import hashlib
import sqlite3
import subprocess
from datetime import datetime
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from tqdm import tqdm

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

FOLDER_PATH = "downloads"
MAX_PARALLEL = 3
MAX_RETRIES = 5
DB_FILE = "upload_state.db"

VIDEO_EXT = {'.mp4', '.mkv', '.webm', '.mov'}
IMAGE_EXT = {'.jpg', '.jpeg', '.png', '.webp'}
GIF_EXT = {'.gif'}

# ================= DB =================

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


def file_hash(path):
    h = hashlib.sha1()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def mark_uploaded(conn, hash_value, filename):
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO uploaded VALUES (?, ?, ?)",
        (hash_value, filename, datetime.utcnow().isoformat())
    )
    conn.commit()


def already_uploaded(conn, hash_value):
    c = conn.cursor()
    c.execute("SELECT 1 FROM uploaded WHERE hash=?", (hash_value,))
    return c.fetchone() is not None


# ================= VIDEO PROCESSING =================

def convert_to_streamable(path):
    if path.endswith(".mp4"):
        return path

    output = path + "_stream.mp4"

    cmd = [
        "ffmpeg", "-y",
        "-i", path,
        "-c:v", "libx264",
        "-preset", "fast",
        "-movflags", "+faststart",
        "-c:a", "aac",
        output
    ]

    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return output if os.path.exists(output) else path


def generate_thumbnail(video_path):
    thumb = video_path + ".jpg"

    cmd = [
        "ffmpeg", "-y",
        "-ss", "00:00:01",
        "-i", video_path,
        "-vframes", "1",
        thumb
    ]

    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return thumb if os.path.exists(thumb) else None


# ================= UPLOADER =================

async def run_uploader():

    conn = init_db()
    semaphore = asyncio.Semaphore(MAX_PARALLEL)

    files = [
        (os.path.join(FOLDER_PATH, f), f)
        for f in os.listdir(FOLDER_PATH)
        if os.path.isfile(os.path.join(FOLDER_PATH, f))
    ]

    sent = 0
    skipped = 0
    failed = 0

    app = Client("single_bot", API_ID, API_HASH, bot_token=BOT_TOKEN)

    async def upload_one(path, filename):
        nonlocal sent, skipped, failed

        hash_val = file_hash(path)

        if already_uploaded(conn, hash_val):
            skipped += 1
            return

        retries = 0

        while retries <= MAX_RETRIES:
            try:
                async with semaphore:

                    ext = os.path.splitext(filename)[1].lower()

                    if ext in VIDEO_EXT:
                        path = convert_to_streamable(path)
                        thumb = generate_thumbnail(path)

                        await app.send_video(
                            CHANNEL_ID,
                            path,
                            caption=filename,
                            supports_streaming=True,
                            thumb=thumb
                        )

                    elif ext in IMAGE_EXT:
                        await app.send_photo(CHANNEL_ID, path, caption=filename)

                    elif ext in GIF_EXT:
                        await app.send_animation(CHANNEL_ID, path, caption=filename)

                    else:
                        await app.send_document(CHANNEL_ID, path, caption=filename)

                mark_uploaded(conn, hash_val, filename)
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

    async with app:
        tasks = [upload_one(p, f) for p, f in files]

        with tqdm(total=len(tasks), desc="Uploading") as pbar:
            for coro in asyncio.as_completed(tasks):
                await coro
                pbar.update(1)

    print("Sent:", sent)
    print("Skipped:", skipped)
    print("Failed:", failed)

    conn.close()


if __name__ == "__main__":
    asyncio.run(run_uploader())
