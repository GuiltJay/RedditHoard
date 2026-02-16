import os
import json
import asyncio
import aiohttp
import time
import subprocess
from datetime import datetime
from urllib.parse import urlparse

import asyncpraw
from dotenv import load_dotenv
from tqdm import tqdm

# ================= CONFIG =================

SUBREDDITS = [
    "pics",
    "videos",
    "memes",
    "aww"
]  # Add as many as you want

POST_LIMIT_PER_SUB = 50
MAX_CONCURRENT = 100
DOWNLOAD_DIR = "downloads"
RETRIES = 3
TIMEOUT = 60

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
existing_files = set(os.listdir(DOWNLOAD_DIR))

# ================= LOAD ENV =================

load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USER_AGENT = os.getenv("USER_AGENT")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
IMGUR_CLIENT_ID = os.getenv("IMGUR_CLIENT_ID")

# ================= GENERIC HELPERS =================

async def fetch(session, url, headers=None):
    for attempt in range(RETRIES):
        try:
            async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    if "application/json" in resp.headers.get("Content-Type", ""):
                        return await resp.json()
                    return await resp.read()
        except:
            await asyncio.sleep(2 ** attempt)
    return None


async def download_file(session, sem, url, filename):
    if filename in existing_files:
        return 0

    async with sem:
        data = await fetch(session, url)
        if not data:
            return 0

        path = os.path.join(DOWNLOAD_DIR, filename)
        with open(path, "wb") as f:
            f.write(data)

        existing_files.add(filename)
        return 1


# ================= REDDIT VIDEO (DASH MERGE) =================

async def merge_dash(video_path, audio_path, output_path):
    cmd = ["ffmpeg", "-y", "-i", video_path, "-i", audio_path, "-c", "copy", output_path]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.remove(video_path)
    os.remove(audio_path)


async def handle_reddit_video(session, sem, submission, sub, created):
    video_data = submission.media["reddit_video"]
    video_url = video_data.get("fallback_url")
    dash_url = video_data.get("dash_url")

    if dash_url and "DASH" in dash_url:
        base = dash_url.rsplit("/", 1)[0]
        audio_url = base + "/DASH_audio.mp4"

        video_tmp = f"{submission.id}_video.mp4"
        audio_tmp = f"{submission.id}_audio.mp4"
        final_name = f"{sub}-{submission.id}-{created}.mp4"

        await download_file(session, sem, video_url, video_tmp)
        await download_file(session, sem, audio_url, audio_tmp)

        await merge_dash(
            os.path.join(DOWNLOAD_DIR, video_tmp),
            os.path.join(DOWNLOAD_DIR, audio_tmp),
            os.path.join(DOWNLOAD_DIR, final_name)
        )

        existing_files.add(final_name)
        return 1

    elif video_url:
        filename = f"{sub}-{submission.id}-{created}.mp4"
        return await download_file(session, sem, video_url, filename)

    return 0


# ================= GALLERY =================

async def handle_gallery(session, sem, submission, sub):
    downloaded = 0
    for item in submission.gallery_data["items"]:
        media_id = item["media_id"]
        meta = submission.media_metadata.get(media_id)
        if meta and meta["status"] == "valid":
            url = meta["s"]["u"].replace("&amp;", "&")
            filename = f"{sub}-{submission.id}-{media_id}.jpg"
            downloaded += await download_file(session, sem, url, filename)
    return downloaded


# ================= IMGUR =================

async def handle_imgur(session, sem, url, sub, submission_id, created):
    parsed = urlparse(url)
    path = parsed.path.strip("/")

    # Album
    if "/a/" in url or "/gallery/" in url:
        album_id = path.split("/")[-1]

        if IMGUR_CLIENT_ID:
            headers = {"Authorization": f"Client-ID {IMGUR_CLIENT_ID}"}
            api = f"https://api.imgur.com/3/album/{album_id}/images"
            data = await fetch(session, api, headers=headers)
            if data and data.get("data"):
                total = 0
                for img in data["data"]:
                    img_url = img["link"]
                    filename = f"{sub}-{submission_id}-{img['id']}.jpg"
                    total += await download_file(session, sem, img_url, filename)
                return total

    # Single image fallback
    direct = f"https://i.imgur.com/{path}.jpg"
    filename = f"{sub}-{submission_id}-{created}.jpg"
    return await download_file(session, sem, direct, filename)


# ================= GFYCAT =================

async def handle_gfycat(session, sem, url, sub, submission_id, created):
    slug = urlparse(url).path.strip("/")

    # Try RedGifs API (most migrated)
    api = f"https://api.redgifs.com/v2/gifs/{slug}"
    data = await fetch(session, api)

    if data and data.get("gif"):
        video_url = data["gif"]["urls"]["hd"]
        filename = f"{sub}-{submission_id}-{created}.mp4"
        return await download_file(session, sem, video_url, filename)

    # Fallback direct webm
    webm = f"https://giant.gfycat.com/{slug}.webm"
    filename = f"{sub}-{submission_id}-{created}.webm"
    return await download_file(session, sem, webm, filename)


# ================= POST PROCESSOR =================

async def process_post(session, sem, submission):
    downloaded = 0

    # Crosspost resolve
    if submission.crosspost_parent_list:
        submission = submission.crosspost_parent_list[0]

    url = submission.url
    domain = urlparse(url).netloc.lower()
    sub = submission.subreddit.display_name
    created = datetime.fromtimestamp(submission.created_utc).strftime("%Y%m%d_%H%M%S")

    try:
        if submission.gallery_data:
            downloaded += await handle_gallery(session, sem, submission, sub)

        elif submission.media and "reddit_video" in submission.media:
            downloaded += await handle_reddit_video(session, sem, submission, sub, created)

        elif "imgur.com" in domain:
            downloaded += await handle_imgur(session, sem, url, sub, submission.id, created)

        elif "gfycat.com" in domain:
            downloaded += await handle_gfycat(session, sem, url, sub, submission.id, created)

        elif "i.redd.it" in domain or "preview.redd.it" in domain:
            ext = os.path.splitext(url)[1] or ".jpg"
            filename = f"{sub}-{submission.id}-{created}{ext}"
            downloaded += await download_file(session, sem, url, filename)

    except:
        pass

    return downloaded


# ================= MAIN =================

async def main():
    print("\n🔥 Reddit Hoarder Engine v2")
    print("Subreddits:", ", ".join(SUBREDDITS))
    print("Concurrency:", MAX_CONCURRENT, "\n")

    reddit = asyncpraw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT,
        username=USERNAME,
        password=PASSWORD
    )

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    total_downloaded = 0
    start = time.time()

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []

        for sub_name in SUBREDDITS:
            subreddit = await reddit.subreddit(sub_name)
            async for submission in subreddit.hot(limit=POST_LIMIT_PER_SUB):
                tasks.append(process_post(session, sem, submission))

        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            total_downloaded += await f

    end = time.time()

    print("\n🔥 DONE")
    print("Downloaded:", total_downloaded)
    print("Time:", round(end - start, 2), "seconds")

    await reddit.close()


if __name__ == "__main__":
    asyncio.run(main())
