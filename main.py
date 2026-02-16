import os
import random
import time
import requests
import subprocess
from datetime import datetime, timezone
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Only posts newer than 24h
CUTOFF = time.time() - 24 * 3600

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

IMGUR_CLIENT_ID = os.getenv("IMGUR_CLIENT_ID")


# ================= HELPERS =================

def download_file(url, filename):
    if not url:
        print(f"[WARN] No URL for {filename}, skipping")
        return 0

    if filename in existing_files:
        print(f"[SKIP] Already downloaded: {filename}")
        return 0

    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            path = os.path.join(DOWNLOAD_DIR, filename)
            with open(path, "wb") as f:
                f.write(r.content)
            existing_files.add(filename)
            print(f"[DOWNLOADED] {filename}")
            return 1
        else:
            print(f"[FAIL] {url} returned {r.status_code}")
    except Exception as e:
        print(f"[ERROR] Download error for {url}: {e}")

    return 0


def merge_dash(video_path, audio_path, output_path):
    print(f"[MERGE] Combining video + audio → {output_path}")
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
    except Exception as e:
        print(f"[ERROR] RedGifs parsing failed for {url}: {e}")

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
        except Exception as e:
            print(f"[ERROR] Imgur album failed for {url}: {e}")

    direct = f"https://i.imgur.com/{path}.jpg"
    fname = f"{sub}-{sid}-{created}.jpg"
    return download_file(direct, fname)


# ================= PROCESS =================

def process_post(submission):
    if submission.created_utc < CUTOFF:
        print(f"[SKIP] Older than 24h: {submission.id}")
        return 0

    try:
        cp = submission.crosspost_parent_list
        if cp and isinstance(cp, list):
            parent_id = cp[0].get("id")
            if parent_id:
                print(f"[CROSSPOST] Resolving parent {parent_id}")
                submission = reddit.submission(id=parent_id)
    except:
        pass

    d = 0

    url = submission.url
    sub = submission.subreddit.display_name

    created = datetime.fromtimestamp(
        submission.created_utc, tz=timezone.utc
    ).strftime("%Y%m%d_%H%M%S")

    dom = urlparse(url).netloc.lower()

    try:
        if submission.gallery_data:
            print(f"[GALLERY] {submission.id}")
            d += handle_gallery(submission, sub)
        elif submission.media:
            print(f"[REDDIT VIDEO] {submission.id}")
            d += handle_reddit_video(submission, sub, created)
        elif "redgifs.com" in dom:
            print(f"[REDGIFS] {url}")
            d += handle_redgifs(url, sub, submission.id, created)
        elif "imgur.com" in dom:
            print(f"[IMGUR] {url}")
            d += handle_imgur(url, sub, submission.id, created)
        elif "i.redd.it" in dom or "preview.redd.it" in dom:
            print(f"[IMAGE] {submission.id}")
            ext = os.path.splitext(url)[1] or ".jpg"
            fname = f"{sub}-{submission.id}-{created}{ext}"
            d += download_file(url, fname)
        else:
            print(f"[NO MEDIA] {submission.id} ({url})")
    except Exception as e:
        print(f"[ERROR] Media handler failed for {submission.id}: {e}")

    return d


# ================= MAIN =================

def main():
    print("🔥 Reddit Hybrid Hoarder Engine (24h Only)")
    submissions = {}
    count_before = len(submissions)

    # 🏠 Home feed
    if FETCH_HOME:
        print("➡️ Fetching home feed…")
        home_added = 0
        for s in reddit.front.new(limit=POST_LIMIT_HOME):
            if s.created_utc >= CUTOFF:
                submissions[s.id] = s
                home_added += 1
        print(f"[DONE] Home feed added: {home_added}")

    # 🗂 Saved posts
    if FETCH_SAVED:
        print("➡️ Fetching saved posts…")
        saved_added = 0
        user = reddit.user.me()
        for s in user.saved(limit=POST_LIMIT_SAVED):
            if getattr(s, "created_utc", 0) >= CUTOFF:
                submissions[s.id] = s
                saved_added += 1
        print(f"[DONE] Saved posts added: {saved_added}")

    # 🌐 Subscribed random subs
    if FETCH_SUBS:
        print("➡️ Fetching subscribed subreddits…")
        try:
            all_subs = list(reddit.user.subreddits(limit=None))
            random.shuffle(all_subs)
            chosen = all_subs[:MAX_RANDOM_SUBS]

            sub_added = 0
            for sub_obj in chosen:
                print(f"[SUB] {sub_obj.display_name}")
                for s in sub_obj.new(limit=POST_LIMIT_PER_SUB):
                    if s.created_utc >= CUTOFF:
                        submissions[s.id] = s
                        sub_added += 1
            print(f"[DONE] Subscribed subs added: {sub_added}")
        except Exception as e:
            print(f"[WARN] Failed to fetch subscribed subs: {e}")

    print(f"\n📊 Total unique posts collected: {len(submissions)}")
    total_downloaded = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(process_post, s) for s in submissions.values()]
        for f in tqdm(as_completed(futures), total=len(futures)):
            total_downloaded += f.result()

    print("\n🔥 FINISHED")
    print(f"📥 Total files downloaded: {total_downloaded}")


if __name__ == "__main__":
    main()
