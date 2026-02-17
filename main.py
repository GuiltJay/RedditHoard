import os
import random
import time
import sqlite3
import threading
import traceback
import requests
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import PurePosixPath

import praw
from dotenv import load_dotenv
from tqdm import tqdm

# ================= CONFIG =================

FETCH_HOME = True
FETCH_SAVED = True
FETCH_SUBS = False

POST_LIMIT_HOME = 200
POST_LIMIT_SAVED = 50
POST_LIMIT_PER_SUB = 35
MAX_RANDOM_SUBS = 50
MAX_WORKERS = 12

DOWNLOAD_DIR = "downloads"
DB_FILE = "reddit_stats.db"

CUTOFF = time.time() - 24 * 3600
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ================= SETUP =================

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    user_agent=os.getenv("USER_AGENT"),
    username=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
)

# ================= THREAD-SAFE DATABASE =================

db_lock = threading.Lock()

_db_conn = sqlite3.connect(DB_FILE, check_same_thread=False)

with _db_conn:
    _db_conn.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        post_id TEXT PRIMARY KEY,
        subreddit TEXT,
        created_utc INTEGER,
        fetched_date TEXT,
        downloaded_count INTEGER
    )
    """)

    _db_conn.execute("""
    CREATE TABLE IF NOT EXISTS daily_stats (
        date TEXT,
        source TEXT,
        subreddit TEXT,
        posts_fetched INTEGER,
        files_downloaded INTEGER
    )
    """)

def post_exists(post_id):
    with db_lock:
        cur = _db_conn.cursor()
        cur.execute("SELECT 1 FROM posts WHERE post_id=?", (post_id,))
        return cur.fetchone() is not None

def save_post(post_id, subreddit, created_utc, downloaded):
    with db_lock:
        with _db_conn:
            _db_conn.execute("""
                INSERT OR IGNORE INTO posts
                (post_id, subreddit, created_utc, fetched_date, downloaded_count)
                VALUES (?, ?, ?, ?, ?)
            """, (post_id, subreddit, created_utc, TODAY, downloaded))

def update_daily_stat(source, subreddit, files_downloaded):
    with db_lock:
        with _db_conn:
            _db_conn.execute("""
                INSERT INTO daily_stats
                (date, source, subreddit, posts_fetched, files_downloaded)
                VALUES (?, ?, ?, ?, ?)
            """, (TODAY, source, subreddit, 1, files_downloaded))

# ================= DOWNLOAD =================

_files_lock = threading.Lock()
_existing_files = set(os.listdir(DOWNLOAD_DIR))

def download_file(url, filename):
    if not url:
        return 0

    with _files_lock:
        if filename in _existing_files:
            return 0
        _existing_files.add(filename)

    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            path = os.path.join(DOWNLOAD_DIR, filename)
            with open(path, "wb") as f:
                f.write(r.content)
            return 1
        else:
            with _files_lock:
                _existing_files.discard(filename)
            return 0
    except Exception:
        with _files_lock:
            _existing_files.discard(filename)
        return 0

# ================= HELPERS =================

def get_url_extension(url):
    """Extract file extension from a URL, ignoring query parameters."""
    parsed = urlparse(url)
    path = unquote(parsed.path)
    ext = PurePosixPath(path).suffix
    return ext if ext else ".jpg"

# ================= PROCESS =================

def process_post(submission, source):
    # Skip comments that may appear in saved items
    if not isinstance(submission, praw.models.Submission):
        return 0

    try:
        created_utc = submission.created_utc
    except Exception:
        return 0

    if created_utc < CUTOFF:
        return 0

    post_id = submission.id

    if post_exists(post_id):
        return 0

    try:
        sub = submission.subreddit.display_name
    except Exception:
        sub = "unknown"

    created = datetime.fromtimestamp(
        created_utc, tz=timezone.utc
    ).strftime("%Y%m%d_%H%M%S")

    url = getattr(submission, "url", None)
    if not url:
        save_post(post_id, sub, created_utc, 0)
        update_daily_stat(source, sub, 0)
        return 0

    dom = urlparse(url).netloc.lower()

    downloaded = 0

    try:
        # Gallery posts
        gallery_data = getattr(submission, "gallery_data", None)
        media_metadata = getattr(submission, "media_metadata", None)

        if gallery_data and isinstance(gallery_data, dict):
            items = gallery_data.get("items", [])
            if items and media_metadata and isinstance(media_metadata, dict):
                for item in items:
                    media_id = item.get("media_id")
                    if not media_id:
                        continue
                    meta = media_metadata.get(media_id)
                    if meta and isinstance(meta, dict) and meta.get("status") == "valid":
                        s_data = meta.get("s")
                        if s_data and isinstance(s_data, dict) and "u" in s_data:
                            img_url = s_data["u"].replace("&amp;", "&")
                            fname = f"{sub}-{post_id}-{media_id}.jpg"
                            downloaded += download_file(img_url, fname)

        # Reddit-hosted video
        elif hasattr(submission, "media") and submission.media:
            media = submission.media
            if isinstance(media, dict):
                video_data = media.get("reddit_video")
                if video_data and isinstance(video_data, dict):
                    video_url = video_data.get("fallback_url")
                    if video_url:
                        fname = f"{sub}-{post_id}-{created}.mp4"
                        downloaded += download_file(video_url, fname)

        # Direct image links
        elif "i.redd.it" in dom or "preview.redd.it" in dom:
            ext = get_url_extension(url)
            fname = f"{sub}-{post_id}-{created}{ext}"
            downloaded += download_file(url, fname)

    except Exception as e:
        print(f"[ERROR] {post_id}: {e}")

    save_post(post_id, sub, created_utc, downloaded)
    update_daily_stat(source, sub, downloaded)

    return downloaded

# ================= MAIN =================

def main():
    print("🔥 Reddit Hoarder + Analytics Engine")

    submissions = []

    if FETCH_HOME:
        print("➡️ Fetching home feed")
        for s in reddit.front.new(limit=POST_LIMIT_HOME):
            submissions.append((s, "home"))

    if FETCH_SAVED:
        print("➡️ Fetching saved posts")
        user = reddit.user.me()
        for s in user.saved(limit=POST_LIMIT_SAVED):
            submissions.append((s, "saved"))

    if FETCH_SUBS:
        print("➡️ Fetching subscribed subreddits")
        all_subs = list(reddit.user.subreddits(limit=None))
        random.shuffle(all_subs)
        chosen = all_subs[:MAX_RANDOM_SUBS]

        for sub_obj in chosen:
            print(f"[SUB] {sub_obj.display_name}")
            for s in sub_obj.new(limit=POST_LIMIT_PER_SUB):
                submissions.append((s, "sub"))

    print(f"📊 Collected {len(submissions)} candidate posts")

    total_downloaded = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(process_post, s, source): (s, source)
            for s, source in submissions
        }

        for f in tqdm(as_completed(futures), total=len(futures)):
            try:
                total_downloaded += f.result()
            except Exception as e:
                errors += 1
                sub_obj, source = futures[f]
                sub_id = getattr(sub_obj, "id", "unknown")
                print(f"\n[FATAL] Post {sub_id} from {source}: {e}")
                traceback.print_exc()

    print(f"\n🔥 FINISHED")
    print(f"📥 Files downloaded today: {total_downloaded}")
    if errors:
        print(f"⚠️ Errors encountered: {errors}")

    print("\n📊 Today Summary:")
    with db_lock:
        cur = _db_conn.cursor()
        cur.execute("""
            SELECT source, COUNT(*), SUM(files_downloaded)
            FROM daily_stats
            WHERE date=?
            GROUP BY source
        """, (TODAY,))
        rows = cur.fetchall()

    for r in rows:
        print(f"Source: {r[0]} | Posts: {r[1]} | Files: {r[2]}")

    _db_conn.close()

if __name__ == "__main__":
    main()
