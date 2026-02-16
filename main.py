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

POST_LIMIT_HOME = 200
POST_LIMIT_SAVED = 50
POST_LIMIT_PER_SUB = 25
MAX_RANDOM_SUBS = 50
MAX_WORKERS = 10
DOWNLOAD_DIR = "downloads"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
existing_files = set(os.listdir(DOWNLOAD_DIR))

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

    if os.path.exists(video_path):
        os.remove(video_path)
    if os.path.exists(audio_path):
        os.remove(audio_path)

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

        video_tmp = f"{submission.id}_video.mp4"
        audio_tmp = f"{submission.id}_audio.mp4"
        final_name = f"{sub}-{submission.id}-{created}.mp4"

        download_file(video_url, video_tmp)
        download_file(audio_url, audio_tmp)

        merge_dash(
            os.path.join(DOWNLOAD_DIR, video_tmp),
            os.path.join(DOWNLOAD_DIR, audio_tmp),
            os.path.join(DOWNLOAD_DIR, final_name)
        )

        existing_files.add(final_name)
        return 1

    elif video_url:
        filename = f"{sub}-{submission.id}-{created}.mp4"
        return download_file(video_url, filename)

    return 0


def handle_gallery(submission, sub):
    if not submission.gallery_data:
        return 0

    downloaded = 0
    for item in submission.gallery_data["items"]:
        media_id = item["media_id"]
        meta = submission.media_metadata.get(media_id)

        if meta and meta.get("status") == "valid":
            url = meta["s"]["u"].replace("&amp;", "&")
            filename = f"{sub}-{submission.id}-{media_id}.jpg"
            downloaded += download_file(url, filename)

    return downloaded


def handle_redgifs(url, sub, submission_id, created):
    parts = urlparse(url).path.strip("/").split("/")
    if not parts:
        return 0

    gif_id = parts[-1]
    if gif_id.lower() == "watch" and len(parts) > 1:
        gif_id = parts[-2]

    try:
        r = requests.get(f"https://api.redgifs.com/v2/gifs/{gif_id}", timeout=15)
        if r.status_code == 200:
            data = r.json()
            video_url = data.get("gif", {}).get("urls", {}).get("hd")
            if video_url:
                filename = f"{sub}-{submission_id}-{created}.mp4"
                return download_file(video_url, filename)
    except:
        pass

    return 0


def handle_imgur(url, sub, submission_id, created):
    parsed = urlparse(url)
    path = parsed.path.strip("/")

    if "/a/" in url or "/gallery/" in url:
        album_id = path.split("/")[-1]
        headers = {"Authorization": f"Client-ID {IMGUR_CLIENT_ID}"}

        try:
            r = requests.get(
                f"https://api.imgur.com/3/album/{album_id}/images",
                headers=headers,
                timeout=15
            )
            if r.status_code == 200:
                data = r.json()
                total = 0
                for img in data["data"]:
                    img_url = img["link"]
                    filename = f"{sub}-{submission_id}-{img['id']}.jpg"
                    total += download_file(img_url, filename)
                return total
        except:
            pass

    direct = f"https://i.imgur.com/{path}.jpg"
    filename = f"{sub}-{submission_id}-{created}.jpg"
    return download_file(direct, filename)

# ================= PROCESS =================

def process_post(submission):
    downloaded = 0

    try:
        if submission.crosspost_parent_list:
            submission = submission.crosspost_parent_list[0]
    except:
        pass

    url = submission.url
    sub = submission.subreddit.display_name
    created = datetime.fromtimestamp(submission.created_utc).strftime("%Y%m%d_%H%M%S")
    domain = urlparse(url).netloc.lower()

    try:
        if submission.gallery_data:
            downloaded += handle_gallery(submission, sub)

        elif submission.media:
            downloaded += handle_reddit_video(submission, sub, created)

        elif "redgifs.com" in domain:
            downloaded += handle_redgifs(url, sub, submission.id, created)

        elif "imgur.com" in domain:
            downloaded += handle_imgur(url, sub, submission.id, created)

        elif "i.redd.it" in domain or "preview.redd.it" in domain:
            ext = os.path.splitext(url)[1] or ".jpg"
            filename = f"{sub}-{submission.id}-{created}{ext}"
            downloaded += download_file(url, filename)

    except:
        pass

    return downloaded

# ================= MAIN =================

def main():
    print("🔥 Hybrid Reddit Hoarder Engine")

    submissions = {}
    user = reddit.user.me()

    # 1️⃣ Home feed
    print("Fetching home feed...")
    for submission in reddit.front.hot(limit=POST_LIMIT_HOME):
        submissions[submission.id] = submission

    # 2️⃣ Saved posts
    print("Fetching saved posts...")
    for submission in user.saved(limit=POST_LIMIT_SAVED):
        submissions[submission.id] = submission

    # 3️⃣ Random subscribed subreddits
    print("Fetching random subscribed subreddits...")
    subs = list(user.subreddits(limit=None))
    random.shuffle(subs)
    selected_subs = subs[:MAX_RANDOM_SUBS]

    for subreddit in selected_subs:
        print("✔", subreddit.display_name)
        for submission in subreddit.hot(limit=POST_LIMIT_PER_SUB):
            submissions[submission.id] = submission

    print(f"\nTotal unique posts collected: {len(submissions)}")

    total_downloaded = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_post, s) for s in submissions.values()]
        for f in tqdm(as_completed(futures), total=len(futures)):
            total_downloaded += f.result()

    print("\n🔥 DONE")
    print("Downloaded:", total_downloaded)


if __name__ == "__main__":
    main()
