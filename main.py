import os
import random
import time
import requests
import subprocess
from datetime import datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import praw
from dotenv import load_dotenv
from tqdm import tqdm

# ================= CONFIG =================

POST_LIMIT_HOME = 300
POST_LIMIT_SAVED = 300
POST_LIMIT_PER_SUB = 35
MAX_RANDOM_SUBS = 50
MAX_WORKERS = 12
DOWNLOAD_DIR = "downloads"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
existing_files = set(os.listdir(DOWNLOAD_DIR))

# 24-hour cutoff (UTC)
CUTOFF = time.time() - 24 * 3600

# ================= LOAD ENV =================

load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    user_agent=os.getenv("USER_AGENT"),
    username=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
)

IMGUR_CLIENT_ID = os.getenv("IMGUR_CLIENT_ID")

# ================= HELPERS =================

def download_file(url, filename):
    if not url or filename in existing_files:
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


def merge_dash(video_path, audio_path, output_path):
    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-i", audio_path,
        "-c", "copy",
        output_path
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for p in (video_path, audio_path):
        if os.path.exists(p):
            os.remove(p)

# ================= MEDIA HANDLERS =================

def handle_reddit_video(submission, sub, created):
    media = submission.media
    if not media or "reddit_video" not in media:
        return 0

    video_data = media["reddit_video"]
    video_url = video_data.get("fallback_url")
    dash_url = video_data.get("dash_url")

    if dash_url and "DASH" in dash_url:
        base = dash_url.rsplit("/", 1)[0]
        audio_url = base + "/DASH_audio.mp4"
        tmp_v = f"{submission.id}_video.mp4"
        tmp_a = f"{submission.id}_audio.mp4"
        final_name = f"{sub}-{submission.id}-{created}.mp4"

        download_file(video_url, tmp_v)
        download_file(audio_url, tmp_a)

        merge_dash(os.path.join(DOWNLOAD_DIR, tmp_v),
                   os.path.join(DOWNLOAD_DIR, tmp_a),
                   os.path.join(DOWNLOAD_DIR, final_name))

        return 1
    elif video_url:
        filename = f"{sub}-{submission.id}-{created}.mp4"
        return download_file(video_url, filename)
    return 0


def handle_gallery(submission, sub):
    if not submission.gallery_data:
        return 0
    total = 0
    for item in submission.gallery_data["items"]:
        media_id = item["media_id"]
        meta = submission.media_metadata.get(media_id)
        if meta and meta.get("status") == "valid":
            url = meta["s"]["u"].replace("&amp;", "&")
            filename = f"{sub}-{submission.id}-{media_id}.jpg"
            total += download_file(url, filename)
    return total


def handle_redgifs(url, sub, sid, created):
    parts = urlparse(url).path.strip("/").split("/")
    if not parts:
        return 0
    gif_id = parts[-1]
    if gif_id.lower() == "watch" and len(parts) > 1:
        gif_id = parts[-2]
    try:
        r = requests.get(f"https://api.redgifs.com/v2/gifs/{gif_id}", timeout=12)
        if r.status_code == 200:
            data = r.json()
            v = data.get("gif", {}).get("urls", {}).get("hd")
            if v:
                filename = f"{sub}-{sid}-{created}.mp4"
                return download_file(v, filename)
    except:
        pass
    return 0


def handle_imgur(url, sub, sid, created):
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if "/a/" in url or "/gallery/" in url:
        album_id = path.split("/")[-1]
        headers = {"Authorization": f"Client-ID {IMGUR_CLIENT_ID}"}
        try:
            r = requests.get(f"https://api.imgur.com/3/album/{album_id}/images",
                             headers=headers, timeout=12)
            if r.status_code == 200:
                total = 0
                for img in r.json().get("data", []):
                    iurl = img.get("link")
                    fname = f"{sub}-{sid}-{img['id']}.jpg"
                    total += download_file(iurl, fname)
                return total
        except:
            pass
    direct = f"https://i.imgur.com/{path}.jpg"
    fname = f"{sub}-{sid}-{created}.jpg"
    return download_file(direct, fname)

# ================= PROCESS =================

def process_post(submission):
    # Skip if older than 24h
    if submission.created_utc < CUTOFF:
        return 0

    d = 0
    try:
        if submission.crosspost_parent_list:
            submission = submission.crosspost_parent_list[0]
    except:
        pass

    url = submission.url
    sub = submission.subreddit.display_name
    created = datetime.utcfromtimestamp(submission.created_utc).strftime("%Y%m%d_%H%M%S")
    dom = urlparse(url).netloc.lower()

    try:
        if submission.gallery_data:
            d += handle_gallery(submission, sub)
        elif submission.media:
            d += handle_reddit_video(submission, sub, created)
        elif "redgifs.com" in dom:
            d += handle_redgifs(url, sub, submission.id, created)
        elif "imgur.com" in dom:
            d += handle_imgur(url, sub, submission.id, created)
        elif "i.redd.it" in dom or "preview.redd.it" in dom:
            ext = os.path.splitext(url)[1] or ".jpg"
            fname = f"{sub}-{submission.id}-{created}{ext}"
            d += download_file(url, fname)
    except:
        pass

    return d

# ================= MAIN =================

def main():
    print("🔥 Reddit Hybrid Hoarder Engine (Last 24h Only)")

    submissions = {}

    # 1️⃣ Home feed (hot/new)
    print("Fetching home feed…")
    for s in reddit.front.new(limit=POST_LIMIT_HOME):
        if s.created_utc >= CUTOFF:
            submissions[s.id] = s

    # 2️⃣ Saved posts
    print("Fetching your saved posts…")
    user = reddit.user.me()
    for s in user.saved(limit=POST_LIMIT_SAVED):
        if getattr(s, "created_utc", 0) >= CUTOFF:
            submissions[s.id] = s

    # 3️⃣ Random subscribed subs
    print("Fetching subscribed subreddits…")
    all_subs = list(reddit.user.subreddits(limit=None))
    random.shuffle(all_subs)
    selected = all_subs[:MAX_RANDOM_SUBS]

    for sub_obj in selected:
        print("✔", sub_obj.display_name)
        for s in sub_obj.new(limit=POST_LIMIT_PER_SUB):
            if s.created_utc >= CUTOFF:
                submissions[s.id] = s

    print(f"\nTotal unique posts in last 24h: {len(submissions)}")

    total = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(process_post, s) for s in submissions.values()]
        for f in tqdm(as_completed(futures), total=len(futures)):
            total += f.result()

    print("\n🔥 DONE")
    print("Downloaded:", total)


if __name__ == "__main__":
    main()
