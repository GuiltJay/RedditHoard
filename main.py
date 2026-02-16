import os
import random
import time
import sqlite3
import threading
import requests
from datetime import datetime, timezone
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import praw
from dotenv import load_dotenv
from tqdm import tqdm

# ================= CONFIG =================

FETCH_HOME = True
FETCH_SAVED = True
FETCH_SUBS = False 

POST_LIMIT_HOME = 300
POST_LIMIT_SAVED = 100
POST_LIMIT_PER_SUB = 35
MAX_RANDOM_SUBS = 50
MAX_WORKERS = 12

DOWNLOAD_DIR = "downloads"
DB_FILE = "reddit_stats.db"

CUTOFF = time.time() - 24 * 3600
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ================= SETUP =================

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
existing_files = set(os.listdir(DOWNLOAD_DIR))

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

conn = sqlite3.connect(DB_FILE, check_same_thread=False)

with conn:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        post_id TEXT PRIMARY KEY,
        subreddit TEXT,
        created_utc INTEGER,
        fetched_date TEXT,
        downloaded_count INTEGER
    )
    """)

    conn.execute("""
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
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM posts WHERE post_id=?", (post_id,))
        return cur.fetchone() is not None


def save_post(post_id, subreddit, created_utc, downloaded):
    with db_lock:
        conn.execute("""
            INSERT OR IGNORE INTO posts 
            (post_id, subreddit, created_utc, fetched_date, downloaded_count)
            VALUES (?, ?, ?, ?, ?)
        """, (post_id, subreddit, created_utc, TODAY, downloaded))


def update_daily_stat(source, subreddit, files_downloaded):
    with db_lock:
        conn.execute("""
            INSERT INTO daily_stats 
            (date, source, subreddit, posts_fetched, files_downloaded)
            VALUES (?, ?, ?, ?, ?)
        """, (TODAY, source, subreddit, 1, files_downloaded))


# ================= DOWNLOAD =================

def download_file(url, filename):
    if not url:
        return 0
    if filename in existing_files:
        return 0

    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            path = os.path.join(DOWNLOAD_DIR, filename)
            with open(path, "wb") as f:
                f.write(r.content)
            existing_files.add(filename)
            return 1
    except:
        pass
    return 0


# ================= PROCESS =================

def process_post(submission, source):

    if submission.created_utc < CUTOFF:
        return 0

    if post_exists(submission.id):
        return 0

    sub = submission.subreddit.display_name
    created = datetime.fromtimestamp(
        submission.created_utc, tz=timezone.utc
    ).strftime("%Y%m%d_%H%M%S")

    url = submission.url
    dom = urlparse(url).netloc.lower()

    downloaded = 0

    try:
        if hasattr(submission, "gallery_data") and submission.gallery_data:
            for item in submission.gallery_data["items"]:
                media_id = item["media_id"]
                meta = submission.media_metadata.get(media_id) if hasattr(submission, "media_metadata") else None
                if meta and meta.get("status") == "valid":
                    img_url = meta["s"]["u"].replace("&amp;", "&")
                    fname = f"{sub}-{submission.id}-{media_id}.jpg"
                    downloaded += download_file(img_url, fname)

        elif hasattr(submission, "media") and submission.media:
            video_data = submission.media.get("reddit_video")
            if video_data:
                video_url = video_data.get("fallback_url")
                fname = f"{sub}-{submission.id}-{created}.mp4"
                downloaded += download_file(video_url, fname)

        elif "i.redd.it" in dom or "preview.redd.it" in dom:
            ext = os.path.splitext(url)[1] or ".jpg"
            fname = f"{sub}-{submission.id}-{created}{ext}"
            downloaded += download_file(url, fname)

    except Exception as e:
        print(f"[ERROR] {submission.id}: {e}")

    save_post(submission.id, sub, submission.created_utc, downloaded)
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

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [
            pool.submit(process_post, s, source)
            for s, source in submissions
        ]

        for f in tqdm(as_completed(futures), total=len(futures)):
            total_downloaded += f.result()

    conn.commit()

    print("\n🔥 FINISHED")
    print(f"📥 Files downloaded today: {total_downloaded}")

    print("\n📊 Today Summary:")
    with db_lock:
        cur = conn.cursor()
        cur.execute("""
            SELECT source, COUNT(*), SUM(files_downloaded)
            FROM daily_stats
            WHERE date=?
            GROUP BY source
        """, (TODAY,))
        rows = cur.fetchall()

    for r in rows:
        print(f"Source: {r[0]} | Posts: {r[1]} | Files: {r[2]}")

    conn.close()


if __name__ == "__main__":
    main()
