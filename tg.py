import os
import sys
import asyncio
import hashlib
import sqlite3
import logging
import subprocess
from datetime import datetime
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from tqdm import tqdm

# ================= CONFIG =================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def get_env_or_exit(name):
    val = os.getenv(name)
    if val is None:
        logger.error(f"Environment variable '{name}' is not set.")
        sys.exit(1)
    return val

API_ID = int(get_env_or_exit("API_ID"))
API_HASH = get_env_or_exit("API_HASH")
BOT_TOKEN = get_env_or_exit("BOT_TOKEN")

_channel_raw = get_env_or_exit("CHANNEL_ID")
try:
    CHANNEL_ID = int(_channel_raw)
except ValueError:
    # Support @channel_username format
    CHANNEL_ID = _channel_raw

FOLDER_PATH = "downloads"
MAX_PARALLEL = 3
MAX_RETRIES = 5
DB_FILE = "upload_state.db"

VIDEO_EXT = {'.mp4', '.mkv', '.webm', '.mov', '.avi', '.flv'}
IMAGE_EXT = {'.jpg', '.jpeg', '.png', '.webp'}
GIF_EXT = {'.gif'}

# ================= DB =================

def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
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
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            while chunk := f.read(1024 * 1024):
                h.update(chunk)
    except OSError as e:
        logger.error(f"Failed to hash file {path}: {e}")
        return None
    return h.hexdigest()

def mark_uploaded(conn, lock, hash_value, filename):
    with lock:
        c = conn.cursor()
        c.execute(
            "INSERT OR IGNORE INTO uploaded VALUES (?, ?, ?)",
            (hash_value, filename, datetime.utcnow().isoformat())
        )
        conn.commit()

def already_uploaded(conn, lock, hash_value):
    with lock:
        c = conn.cursor()
        c.execute("SELECT 1 FROM uploaded WHERE hash=?", (hash_value,))
        return c.fetchone() is not None

# ================= VIDEO PROCESSING =================

def convert_to_streamable(path):
    """Convert non-mp4 videos to streamable mp4. Returns (output_path, is_temp)."""
    if path.lower().endswith(".mp4"):
        return path, False

    output = os.path.splitext(path)[0] + "_stream.mp4"

    cmd = [
        "ffmpeg", "-y",
        "-i", path,
        "-c:v", "libx264",
        "-preset", "fast",
        "-movflags", "+faststart",
        "-c:a", "aac",
        output
    ]

    result = subprocess.run(
        cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
    )

    if result.returncode != 0:
        logger.warning(
            f"ffmpeg conversion failed for {path}: "
            f"{result.stderr.decode(errors='replace')[-200:]}"
        )
        return path, False

    if os.path.exists(output) and os.path.getsize(output) > 0:
        return output, True

    return path, False

def generate_thumbnail(video_path):
    """Generate a thumbnail from a video. Returns path or None."""
    thumb = os.path.splitext(video_path)[0] + "_thumb.jpg"

    cmd = [
        "ffmpeg", "-y",
        "-ss", "00:00:01",
        "-i", video_path,
        "-vframes", "1",
        "-q:v", "5",
        thumb
    ]

    result = subprocess.run(
        cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
    )

    if result.returncode != 0:
        logger.warning(f"Thumbnail generation failed for {video_path}")
        return None

    if os.path.exists(thumb) and os.path.getsize(thumb) > 0:
        return thumb

    return None

def cleanup_temp_files(*paths):
    """Remove temporary files safely."""
    for p in paths:
        if p and os.path.exists(p):
            try:
                os.remove(p)
            except OSError as e:
                logger.warning(f"Failed to remove temp file {p}: {e}")

# ================= UPLOADER =================

async def run_uploader():
    if not os.path.isdir(FOLDER_PATH):
        logger.error(f"Folder '{FOLDER_PATH}' does not exist.")
        sys.exit(1)

    conn = init_db()
    db_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(MAX_PARALLEL)

    files = sorted([
        (os.path.join(FOLDER_PATH, f), f)
        for f in os.listdir(FOLDER_PATH)
        if os.path.isfile(os.path.join(FOLDER_PATH, f))
    ])

    if not files:
        logger.info("No files found to upload.")
        conn.close()
        return

    sent = 0
    skipped = 0
    failed = 0

    app = Client("single_bot", API_ID, API_HASH, bot_token=BOT_TOKEN)

    async def upload_one(path, filename):
        nonlocal sent, skipped, failed

        hash_val = await asyncio.to_thread(file_hash, path)
        if hash_val is None:
            failed += 1
            return

        if already_uploaded(conn, db_lock, hash_val):
            logger.debug(f"Skipping (already uploaded): {filename}")
            skipped += 1
            return

        temp_video = None
        thumb_path = None
        retries = 0

        while retries <= MAX_RETRIES:
            try:
                async with semaphore:
                    ext = os.path.splitext(filename)[1].lower()

                    if ext in VIDEO_EXT:
                        converted, is_temp = await asyncio.to_thread(
                            convert_to_streamable, path
                        )
                        if is_temp:
                            temp_video = converted
                        thumb_path = await asyncio.to_thread(
                            generate_thumbnail, converted
                        )

                        await app.send_video(
                            CHANNEL_ID,
                            converted,
                            caption=filename,
                            supports_streaming=True,
                            thumb=thumb_path
                        )

                    elif ext in IMAGE_EXT:
                        await app.send_photo(
                            CHANNEL_ID, path, caption=filename
                        )

                    elif ext in GIF_EXT:
                        await app.send_animation(
                            CHANNEL_ID, path, caption=filename
                        )

                    else:
                        await app.send_document(
                            CHANNEL_ID, path, caption=filename
                        )

                mark_uploaded(conn, db_lock, hash_val, filename)
                sent += 1
                logger.info(f"Uploaded: {filename}")
                return

            except FloodWait as e:
                retries += 1
                wait_time = e.value + 1
                logger.warning(
                    f"FloodWait {e.value}s for {filename} "
                    f"(retry {retries}/{MAX_RETRIES})"
                )
                await asyncio.sleep(wait_time)

            except RPCError as e:
                logger.error(f"RPCError uploading {filename}: {e}")
                failed += 1
                return

            except Exception as e:
                logger.exception(f"Unexpected error uploading {filename}: {e}")
                failed += 1
                return

            finally:
                # Cleanup temp files only when we're done (success or give up)
                pass

        # Exhausted all retries
        logger.error(f"Max retries exceeded for {filename}")
        failed += 1

        # Cleanup happens after the loop regardless of outcome
        cleanup_temp_files(temp_video, thumb_path)

    async with app:
        tasks = [upload_one(p, f) for p, f in files]

        with tqdm(total=len(tasks), desc="Uploading", unit="file") as pbar:
            for coro in asyncio.as_completed(tasks):
                await coro
                pbar.update(1)

    logger.info(f"Done — Sent: {sent} | Skipped: {skipped} | Failed: {failed}")
    conn.close()

if __name__ == "__main__":
    asyncio.run(run_uploader())
