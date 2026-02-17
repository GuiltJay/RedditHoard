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
from pyrogram.session import Session
from tqdm import tqdm

# ================= LOGGING =================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("uploader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logging.getLogger("pyrogram").setLevel(logging.WARNING)

# ================= CONFIG =================

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
    CHANNEL_ID = _channel_raw

FOLDER_PATH = "downloads"
DB_FILE = "upload_state.db"
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
LARGE_FILE_THRESHOLD = 30 * 1024 * 1024  # 30MB

VIDEO_EXT = {'.mp4', '.mkv', '.webm', '.mov', '.avi', '.flv', '.wmv', '.m4v'}
IMAGE_EXT = {'.jpg', '.jpeg', '.png', '.webp', '.bmp'}
GIF_EXT = {'.gif'}

# ================= PYROGRAM TUNING =================

# Increase internal Pyrogram timeout to prevent premature "Request timed out"
Session.START_TIMEOUT = 30
Session.WAIT_TIMEOUT = 30
Session.SLEEP_THRESHOLD = 30
Session.MAX_RETRIES = 10
Session.PING_INTERVAL = 5

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
            while chunk := f.read(1024 * 1024):
                h.update(chunk)
    except OSError as e:
        logger.error(f"Failed to hash file {path}: {e}")
        return None
    return h.hexdigest()

def mark_uploaded(conn, db_lock, hash_value, filename):
    with db_lock:
        try:
            c = conn.cursor()
            c.execute(
                "INSERT OR IGNORE INTO uploaded VALUES (?, ?, ?)",
                (hash_value, filename, datetime.utcnow().isoformat())
            )
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB error marking {filename}: {e}")

def already_uploaded(conn, db_lock, hash_value):
    with db_lock:
        try:
            c = conn.cursor()
            c.execute("SELECT 1 FROM uploaded WHERE hash=?", (hash_value,))
            return c.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"DB error checking hash: {e}")
            return False

# ================= VIDEO PROCESSING =================

def convert_to_streamable(path):
    if path.lower().endswith(".mp4"):
        return path, False

    base = os.path.splitext(path)[0]
    output = base + "_stream.mp4"

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

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=600
        )
        if result.returncode != 0:
            stderr_text = result.stderr.decode(errors='replace')[-300:]
            logger.warning(f"ffmpeg failed for {path}: {stderr_text}")
            return path, False
    except subprocess.TimeoutExpired:
        logger.warning(f"ffmpeg timed out for {path}")
        if os.path.exists(output):
            try:
                os.remove(output)
            except OSError:
                pass
        return path, False
    except FileNotFoundError:
        logger.error("ffmpeg not found in PATH")
        return path, False

    if os.path.exists(output) and os.path.getsize(output) > 0:
        return output, True

    return path, False

def generate_thumbnail(video_path):
    base = os.path.splitext(video_path)[0]
    thumb = base + "_thumb.jpg"

    cmd = [
        "ffmpeg", "-y",
        "-ss", "00:00:01",
        "-i", video_path,
        "-vframes", "1",
        "-q:v", "5",
        "-vf", "scale='min(320,iw)':-1",
        thumb
    ]

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=30
        )
        if result.returncode != 0:
            return None
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None

    if os.path.exists(thumb) and os.path.getsize(thumb) > 0:
        return thumb

    return None

def cleanup_temp_files(*paths):
    for p in paths:
        if p and os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass

# ================= FILE DISCOVERY =================

def discover_files(folder):
    all_files = []
    for root, _dirs, filenames in os.walk(folder):
        for f in sorted(filenames):
            full_path = os.path.join(root, f)
            if not os.path.isfile(full_path):
                continue
            file_size = os.path.getsize(full_path)
            if file_size == 0:
                logger.warning(f"Skipping empty file: {f}")
                continue
            if file_size > MAX_FILE_SIZE:
                logger.warning(
                    f"Skipping oversized ({file_size // (1024 * 1024)}MB): {f}"
                )
                continue
            all_files.append((full_path, f, file_size))

    all_files.sort(key=lambda x: x[2])
    return all_files

# ================= UPLOADER =================

async def resolve_channel(app, channel_id):
    try:
        chat = await app.get_chat(channel_id)
        logger.info(f"Resolved channel: {chat.title} (ID: {chat.id})")
        return chat.id
    except Exception as e:
        logger.error(f"Cannot resolve channel '{channel_id}': {e}")
        return None

async def run_uploader():
    if not os.path.isdir(FOLDER_PATH):
        logger.error(f"Folder '{FOLDER_PATH}' does not exist.")
        sys.exit(1)

    conn = init_db()
    db_lock = asyncio.Lock()
    counter_lock = asyncio.Lock()

    files = discover_files(FOLDER_PATH)

    if not files:
        logger.info("No files found to upload.")
        conn.close()
        return

    logger.info(f"Found {len(files)} files to process")

    sent = 0
    skipped = 0
    failed = 0
    errors_list = []

    app = Client(
        "single_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        max_concurrent_transmissions=1,
    )

    async def do_send(app, path, filename, resolved_id):
        """Perform the actual Telegram send call."""
        ext = os.path.splitext(filename)[1].lower()

        temp_video = None
        thumb_path = None
        upload_path = path

        try:
            if ext in VIDEO_EXT:
                converted, is_temp = await asyncio.to_thread(
                    convert_to_streamable, path
                )
                if is_temp:
                    temp_video = converted
                upload_path = converted

                thumb_path = await asyncio.to_thread(
                    generate_thumbnail, upload_path
                )

                await app.send_video(
                    chat_id=resolved_id,
                    video=upload_path,
                    caption=filename,
                    supports_streaming=True,
                    thumb=thumb_path
                )

            elif ext in IMAGE_EXT:
                await app.send_photo(
                    chat_id=resolved_id,
                    photo=upload_path,
                    caption=filename
                )

            elif ext in GIF_EXT:
                await app.send_animation(
                    chat_id=resolved_id,
                    animation=upload_path,
                    caption=filename
                )

            else:
                await app.send_document(
                    chat_id=resolved_id,
                    document=upload_path,
                    caption=filename,
                    force_document=True
                )

        finally:
            cleanup_temp_files(temp_video, thumb_path)

    async def upload_one(path, filename, file_size, resolved_id):
        nonlocal sent, skipped, failed

        try:
            hash_val = await asyncio.to_thread(file_hash, path)
            if hash_val is None:
                async with counter_lock:
                    failed += 1
                    errors_list.append((filename, "hash_failed"))
                return

            if await asyncio.to_thread(
                lambda: already_uploaded(conn, db_lock, hash_val)
            ):
                async with counter_lock:
                    skipped += 1
                return

            max_retries = 10 if file_size > LARGE_FILE_THRESHOLD else 5
            retries = 0

            while retries <= max_retries:
                try:
                    await do_send(app, path, filename, resolved_id)

                    await asyncio.to_thread(
                        lambda: mark_uploaded(
                            conn, db_lock, hash_val, filename
                        )
                    )
                    async with counter_lock:
                        sent += 1
                    logger.info(
                        f"Uploaded: {filename} "
                        f"({file_size // 1024}KB)"
                    )
                    return

                except FloodWait as e:
                    retries += 1
                    wait_time = e.value + 5
                    logger.warning(
                        f"FloodWait {e.value}s for {filename} "
                        f"(retry {retries}/{max_retries})"
                    )
                    await asyncio.sleep(wait_time)

                except (
                    ConnectionError,
                    BrokenPipeError,
                    ConnectionResetError,
                    ConnectionAbortedError,
                    TimeoutError,
                    OSError,
                ) as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(
                            f"Connection failed for {filename} "
                            f"after {max_retries} retries: {e}"
                        )
                        async with counter_lock:
                            failed += 1
                            errors_list.append((filename, f"conn: {e}"))
                        return

                    delay = min(5 * (2 ** (retries - 1)), 120)
                    logger.warning(
                        f"Pipe/connection error for {filename}: {e} — "
                        f"retry {retries}/{max_retries} in {delay}s"
                    )
                    await asyncio.sleep(delay)

                except RPCError as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(
                            f"RPCError for {filename}: {e}"
                        )
                        async with counter_lock:
                            failed += 1
                            errors_list.append((filename, str(e)))
                        return

                    delay = min(5 * retries, 60)
                    logger.warning(
                        f"RPCError for {filename}: {e} — "
                        f"retry {retries}/{max_retries} in {delay}s"
                    )
                    await asyncio.sleep(delay)

            # Exhausted
            logger.error(f"Max retries exceeded for {filename}")
            async with counter_lock:
                failed += 1
                errors_list.append((filename, "max_retries"))

        except Exception as e:
            logger.exception(f"Fatal error processing {filename}: {e}")
            async with counter_lock:
                failed += 1
                errors_list.append((filename, str(e)))

    async with app:
        resolved_id = await resolve_channel(app, CHANNEL_ID)
        if resolved_id is None:
            logger.error("Failed to resolve channel. Exiting.")
            conn.close()
            return

        await asyncio.sleep(2)

        # === SEQUENTIAL UPLOAD — ONE FILE AT A TIME ===
        with tqdm(total=len(files), desc="Uploading", unit="file") as pbar:
            for path, filename, file_size in files:
                await upload_one(path, filename, file_size, resolved_id)
                pbar.update(1)

                # Small breathing room between uploads
                await asyncio.sleep(0.5)

    # ================= SUMMARY =================

    print("\n" + "=" * 50)
    print("🔥 FINISHED")
    print(f"📤 Sent:    {sent}")
    print(f"⏭️  Skipped: {skipped}")
    print(f"❌ Failed:  {failed}")
    print(f"📁 Total:   {len(files)}")
    print("=" * 50)

    if errors_list:
        print(f"\n❌ Errors ({len(errors_list)}):")
        for fname, err in errors_list:
            print(f"  • {fname}: {err}")

    logger.info(
        f"Done — Sent: {sent} | Skipped: {skipped} | "
        f"Failed: {failed} | Total: {len(files)}"
    )

    conn.close()

if __name__ == "__main__":
    try:
        asyncio.run(run_uploader())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)
