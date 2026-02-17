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
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("uploader.log"),
        logging.StreamHandler()
    ]
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

_channel_raw = get_env_or_exit("CHANNEL_ID").strip()
try:
    CHANNEL_ID = int(_channel_raw)
except ValueError:
    if _channel_raw.startswith("@"):
        CHANNEL_ID = _channel_raw
    else:
        CHANNEL_ID = f"@{_channel_raw}"

FOLDER_PATH = "downloads"
MAX_PARALLEL = 3
MAX_RETRIES = 5
DB_FILE = "upload_state.db"

VIDEO_EXT = {'.mp4', '.mkv', '.webm', '.mov', '.avi', '.flv', '.ts', '.m4v'}
IMAGE_EXT = {'.jpg', '.jpeg', '.png', '.webp', '.bmp'}
GIF_EXT = {'.gif'}

# ================= DB =================

def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
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
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
    except OSError as e:
        logger.error(f"Failed to hash file {path}: {e}")
        return None
    return h.hexdigest()

def _mark_uploaded(conn, hash_value, filename):
    try:
        c = conn.cursor()
        c.execute(
            "INSERT OR IGNORE INTO uploaded VALUES (?, ?, ?)",
            (hash_value, filename, datetime.utcnow().isoformat())
        )
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"DB error marking {filename}: {e}")

def _already_uploaded(conn, hash_value):
    try:
        c = conn.cursor()
        c.execute("SELECT 1 FROM uploaded WHERE hash=?", (hash_value,))
        return c.fetchone() is not None
    except sqlite3.Error as e:
        logger.error(f"DB error checking hash: {e}")
        return False

# ================= VIDEO PROCESSING =================

def get_video_duration(path):
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            path
        ]
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30
        )
        if result.returncode == 0:
            return int(float(result.stdout.decode().strip()))
    except Exception:
        pass
    return 0

def get_video_dimensions(path):
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "csv=s=x:p=0",
            path
        ]
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30
        )
        if result.returncode == 0:
            parts = result.stdout.decode().strip().split("x")
            if len(parts) == 2:
                return int(parts[0]), int(parts[1])
    except Exception:
        pass
    return 0, 0

def convert_to_streamable(path):
    ext = os.path.splitext(path)[1].lower()
    if ext == ".mp4":
        return path, False

    output = os.path.splitext(path)[0] + "_stream.mp4"

    try:
        cmd = [
            "ffmpeg", "-y",
            "-i", path,
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-movflags", "+faststart",
            "-c:a", "aac",
            "-b:a", "128k",
            output
        ]
        result = subprocess.run(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=600
        )
        if result.returncode != 0:
            stderr_text = result.stderr.decode(errors="replace")[-300:]
            logger.warning(f"ffmpeg conversion failed for {path}: {stderr_text}")
            return path, False

        if os.path.exists(output) and os.path.getsize(output) > 0:
            return output, True

    except subprocess.TimeoutExpired:
        logger.warning(f"ffmpeg conversion timed out for {path}")
    except Exception as e:
        logger.warning(f"ffmpeg conversion error for {path}: {e}")

    return path, False

def generate_thumbnail(video_path):
    thumb = os.path.splitext(video_path)[0] + "_thumb.jpg"

    try:
        cmd = [
            "ffmpeg", "-y",
            "-ss", "00:00:01",
            "-i", video_path,
            "-vframes", "1",
            "-vf", "scale=320:-2",
            "-q:v", "5",
            thumb
        ]
        result = subprocess.run(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=30
        )
        if result.returncode == 0 and os.path.exists(thumb) and os.path.getsize(thumb) > 0:
            return thumb

    except subprocess.TimeoutExpired:
        logger.warning(f"Thumbnail generation timed out for {video_path}")
    except Exception as e:
        logger.warning(f"Thumbnail generation error for {video_path}: {e}")

    return None

def cleanup_temp_files(*paths):
    for p in paths:
        if p and os.path.exists(p):
            try:
                os.remove(p)
            except OSError as e:
                logger.warning(f"Failed to remove temp file {p}: {e}")

# ================= FILE COLLECTOR =================

def collect_files(folder):
    all_files = []
    for root, dirs, files in os.walk(folder):
        for f in sorted(files):
            full_path = os.path.join(root, f)
            if os.path.isfile(full_path) and os.path.getsize(full_path) > 0:
                if f.endswith("_stream.mp4") or f.endswith("_thumb.jpg"):
                    continue
                all_files.append((full_path, f))
    return all_files

# ================= UPLOADER =================

async def run_uploader():
    if not os.path.isdir(FOLDER_PATH):
        logger.error(f"Folder '{FOLDER_PATH}' does not exist.")
        sys.exit(1)

    conn = init_db()
    db_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(MAX_PARALLEL)

    files = collect_files(FOLDER_PATH)

    if not files:
        logger.info("No files found to upload.")
        conn.close()
        return

    logger.info(f"Found {len(files)} files to process")

    sent = 0
    skipped = 0
    failed = 0
    errors_list = []

    app = Client("single_bot", API_ID, API_HASH, bot_token=BOT_TOKEN)

    # Mutable container so nested function can read the resolved ID
    target = {"chat_id": CHANNEL_ID}

    async def safe_db_check(hash_val):
        async with db_lock:
            return _already_uploaded(conn, hash_val)

    async def safe_db_mark(hash_val, filename):
        async with db_lock:
            _mark_uploaded(conn, hash_val, filename)

    async def upload_one(path, filename):
        nonlocal sent, skipped, failed

        chat_id = target["chat_id"]
        temp_video = None
        thumb_path = None

        try:
            hash_val = await asyncio.to_thread(file_hash, path)
            if hash_val is None:
                logger.error(f"Hash failed: {filename}")
                failed += 1
                errors_list.append((filename, "hash failed"))
                return

            if await safe_db_check(hash_val):
                skipped += 1
                return

            ext = os.path.splitext(filename)[1].lower()
            retries = 0

            while retries <= MAX_RETRIES:
                try:
                    async with semaphore:

                        if ext in VIDEO_EXT:
                            converted, is_temp = await asyncio.to_thread(
                                convert_to_streamable, path
                            )
                            if is_temp:
                                temp_video = converted

                            thumb_path = await asyncio.to_thread(
                                generate_thumbnail, converted
                            )

                            duration = await asyncio.to_thread(
                                get_video_duration, converted
                            )
                            width, height = await asyncio.to_thread(
                                get_video_dimensions, converted
                            )

                            await app.send_video(
                                chat_id=chat_id,
                                video=converted,
                                caption=filename,
                                supports_streaming=True,
                                thumb=thumb_path,
                                duration=duration,
                                width=width,
                                height=height
                            )

                        elif ext in IMAGE_EXT:
                            await app.send_photo(
                                chat_id=chat_id,
                                photo=path,
                                caption=filename
                            )

                        elif ext in GIF_EXT:
                            await app.send_animation(
                                chat_id=chat_id,
                                animation=path,
                                caption=filename
                            )

                        else:
                            await app.send_document(
                                chat_id=chat_id,
                                document=path,
                                caption=filename,
                                force_document=True
                            )

                    await safe_db_mark(hash_val, filename)
                    sent += 1
                    return

                except FloodWait as e:
                    retries += 1
                    wait_time = e.value + 2
                    logger.warning(
                        f"FloodWait {e.value}s for {filename} "
                        f"(retry {retries}/{MAX_RETRIES})"
                    )
                    if retries > MAX_RETRIES:
                        break
                    await asyncio.sleep(wait_time)

                except RPCError as e:
                    retries += 1
                    err_msg = str(e)
                    logger.error(
                        f"RPCError uploading {filename}: {err_msg} "
                        f"(retry {retries}/{MAX_RETRIES})"
                    )
                    if retries > MAX_RETRIES:
                        break
                    await asyncio.sleep(5)

                except (ConnectionError, TimeoutError, OSError) as e:
                    retries += 1
                    logger.error(
                        f"Connection error uploading {filename}: {e} "
                        f"(retry {retries}/{MAX_RETRIES})"
                    )
                    if retries > MAX_RETRIES:
                        break
                    await asyncio.sleep(10)

                except Exception as e:
                    logger.exception(f"Unexpected error uploading {filename}: {e}")
                    failed += 1
                    errors_list.append((filename, str(e)))
                    return

            # Exhausted all retries
            logger.error(f"Max retries exceeded for {filename}")
            failed += 1
            errors_list.append((filename, "max retries exceeded"))

        finally:
            cleanup_temp_files(temp_video, thumb_path)

    async with app:
        # Verify bot can access the channel and resolve numeric ID
        try:
            chat = await app.get_chat(CHANNEL_ID)
            logger.info(f"Connected to channel: {chat.title} (ID: {chat.id})")
            target["chat_id"] = chat.id
        except Exception as e:
            logger.error(f"Cannot access channel {CHANNEL_ID}: {e}")
            conn.close()
            return

        tasks = [upload_one(p, f) for p, f in files]

        with tqdm(total=len(tasks), desc="Uploading", unit="file") as pbar:
            for coro in asyncio.as_completed(tasks):
                await coro
                pbar.update(1)

    # Final report
    print()
    print("=" * 40)
    print("🔥 FINISHED")
    print(f"✅ Sent:    {sent}")
    print(f"⏭️  Skipped: {skipped}")
    print(f"❌ Failed:  {failed}")
    print(f"📁 Total:   {len(files)}")
    print("=" * 40)

    if errors_list:
        print(f"\n❌ Error details ({len(errors_list)}):")
        for fname, err in errors_list:
            print(f"  {fname}: {err}")

    conn.close()

if __name__ == "__main__":
    asyncio.run(run_uploader())
