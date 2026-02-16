import os
import json
import asyncio
import aiohttp
import time
from datetime import datetime
from urllib.parse import urlparse

import praw
from dotenv import load_dotenv
from tqdm import tqdm

# =========================
# LOAD ENV
# =========================
load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USER_AGENT = os.getenv("USER_AGENT")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

if not all([CLIENT_ID, CLIENT_SECRET, USER_AGENT, USERNAME, PASSWORD]):
    print("Missing environment variables.")
    exit(1)

# =========================
# CONFIG
# =========================
POST_LIMIT = 200
FETCH_MULTIPLIER = 3
MAX_CONCURRENT_DOWNLOADS = 100
DOWNLOAD_TIMEOUT = 60
RETRIES = 3
DOWNLOAD_DIR = "downloads"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
existing_files = set(os.listdir(DOWNLOAD_DIR))

# =========================
# REDDIT AUTH
# =========================
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT,
    username=USERNAME,
    password=PASSWORD
)

try:
    reddit.user.me()
    print("✅ Reddit Authenticated")
except Exception as e:
    print("❌ Auth failed:", e)
    exit(1)

# =========================
# HELPERS
# =========================

def is_media_post(submission):
    if submission.is_self:
        return False

    url = submission.url.lower()
    domain = urlparse(url).netloc

    if any(d in domain for d in ["i.redd.it", "v.redd.it", "redgifs.com"]):
        return True

    if url.endswith((".jpg", ".jpeg", ".png", ".gif", ".mp4", ".webm")):
        return True

    if submission.media and "reddit_video" in submission.media:
        return True

    return False


async def fetch_with_retry(session, url):
    for attempt in range(RETRIES):
        try:
            async with session.get(url, timeout=DOWNLOAD_TIMEOUT) as resp:
                if resp.status == 200:
                    return await resp.read()
        except:
            await asyncio.sleep(2 ** attempt)
    return None


async def download_file(session, semaphore, url, filename):
    if filename in existing_files:
        return 0

    async with semaphore:
        data = await fetch_with_retry(session, url)
        if not data:
            return 0

        filepath = os.path.join(DOWNLOAD_DIR, filename)
        try:
            with open(filepath, "wb") as f:
                f.write(data)
            existing_files.add(filename)
            return 1
        except:
            return 0


async def get_redgifs_video(session, gif_id):
    try:
        api_url = f"https://api.redgifs.com/v2/gifs/{gif_id}"
        async with session.get(api_url) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data["gif"]["urls"]["hd"]
    except:
        return None


async def process_post(session, semaphore, submission):
    downloaded = 0
    url = submission.url
    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    sub = submission.subreddit.display_name
    created = datetime.fromtimestamp(submission.created_utc).strftime("%Y%m%d_%H%M%S")

    try:
        # RedGifs
        if "redgifs.com" in domain:
            gif_id = parsed.path.split("/")[-1].split(".")[0]
            video_url = await get_redgifs_video(session, gif_id)
            if video_url:
                filename = f"{sub}-{submission.id}-{created}-redgifs.mp4"
                downloaded += await download_file(session, semaphore, video_url, filename)

        # Reddit Image
        elif "i.redd.it" in domain:
            ext = os.path.splitext(url)[1] or ".jpg"
            filename = f"{sub}-{submission.id}-{created}{ext}"
            downloaded += await download_file(session, semaphore, url, filename)

        # Reddit Video
        elif "v.redd.it" in domain:
            if submission.media and "reddit_video" in submission.media:
                video_url = submission.media["reddit_video"].get("fallback_url")
                if video_url:
                    filename = f"{sub}-{submission.id}-{created}.mp4"
                    downloaded += await download_file(session, semaphore, video_url, filename)

        # Direct Media
        elif url.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".mp4", ".webm")):
            ext = os.path.splitext(url)[1]
            filename = f"{sub}-{submission.id}-{created}{ext}"
            downloaded += await download_file(session, semaphore, url, filename)

    except:
        pass

    return downloaded


# =========================
# MAIN ASYNC
# =========================

async def main():

    print("\n⚡ ULTRA TURBO Async Reddit Downloader")
    print(f"Target Posts: {POST_LIMIT}")
    print(f"Max Concurrent: {MAX_CONCURRENT_DOWNLOADS}\n")

    media_posts = []
    fetch_limit = POST_LIMIT * FETCH_MULTIPLIER

    print("🔎 Fetching posts...")

    for submission in reddit.front.best(limit=fetch_limit):
        if len(media_posts) >= POST_LIMIT:
            break
        if is_media_post(submission):
            media_posts.append(submission)

    print(f"✅ Found {len(media_posts)} media posts\n")

    if not media_posts:
        return

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_DOWNLOADS)
    timeout = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    start_time = time.time()
    total_downloaded = 0

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            process_post(session, semaphore, post)
            for post in media_posts
        ]

        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            total_downloaded += await future

    end_time = time.time()

    stats = {
        "session_date": datetime.now().isoformat(),
        "posts_processed": len(media_posts),
        "files_downloaded": total_downloaded,
        "time_taken_seconds": round(end_time - start_time, 2)
    }

    with open(os.path.join(DOWNLOAD_DIR, "session_stats.json"), "w") as f:
        json.dump(stats, f, indent=2)

    print("\n🔥 DONE")
    print(f"📥 Downloaded: {total_downloaded}")
    print(f"⏱ Time: {round(end_time - start_time, 2)} seconds")


# =========================
# RUN
# =========================

if __name__ == "__main__":
    asyncio.run(main())
