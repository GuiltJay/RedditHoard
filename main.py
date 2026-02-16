import os
from dotenv import load_dotenv
import praw
import requests
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from datetime import datetime
import json
from tqdm import tqdm
import time

# Load environment variables from .env file
load_dotenv()

# IMPORTANT: Create a .env file in the same directory with the following format:
# CLIENT_ID=your_client_id
# CLIENT_SECRET=your_client_secret
# USER_AGENT=MediaDownloader/1.0 (by u/YOUR_USERNAME)
# USERNAME=your_username
# PASSWORD=your_password
#
# Set up a Reddit app at https://www.reddit.com/prefs/apps/
# For security, never commit the .env file to version control.

# Retrieve credentials from environment variables
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
USER_AGENT = os.getenv('USER_AGENT')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

# Validate that all required env vars are set
missing_vars = [var for var, value in [
    ('CLIENT_ID', CLIENT_ID),
    ('CLIENT_SECRET', CLIENT_SECRET),
    ('USER_AGENT', USER_AGENT),
    ('USERNAME', USERNAME),
    ('PASSWORD', PASSWORD)
] if not value]

if missing_vars:
    print(f"Missing environment variables: {', '.join(missing_vars)}")
    print("Please check your .env file.")
    exit(1)

# Configuration
POST_LIMIT = 100
MAX_WORKERS = 5
REQUEST_DELAY = 0.3  # Delay between requests to avoid rate limiting

# Initialize Reddit instance
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT,
    username=USERNAME,
    password=PASSWORD
)

# Verify authentication
try:
    reddit.user.me()
    print("Authenticated successfully.")
except Exception as e:
    print(f"Authentication failed: {e}")
    print("Please check your credentials and Reddit app setup.")
    exit(1)

# Create downloads directory
downloads_dir = "downloads"
os.makedirs(downloads_dir, exist_ok=True)

def is_media_post(submission):
    """
    Check if a submission has downloadable media.
    """
    if submission.is_self or not hasattr(submission, 'url'):
        return False
    
    url = submission.url
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    
    if any(domain_check in domain for domain_check in ['i.redd.it', 'v.redd.it', 'redgifs.com', 'gfycat.com']):
        return True
    
    if url.lower().endswith(('.gif', '.png', '.jpg', '.jpeg', '.mp4', '.webm')):
        return True
    
    if hasattr(submission, 'media') and submission.media is not None:
        if 'reddit_gallery' in submission.media:
            return True
        if 'reddit_video' in submission.media:
            return True
    
    return False

def download_file(url, filepath, headers=None):
    """
    Download a file from URL to filepath with error resilience.
    """
    try:
        # Skip if file already exists
        if os.path.exists(filepath):
            return True
            
        response = requests.get(url, stream=True, headers=headers, timeout=30)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def get_redgifs_video_url_batch(gif_ids):
    """
    Batch process RedGifs URLs for multiple IDs at once.
    Returns dict with {gif_id: video_url}
    """
    results = {}
    
    def fetch_single_gif(gif_id):
        """Fetch single RedGifs URL"""
        try:
            time.sleep(REQUEST_DELAY)  # Rate limiting
            page_url = f"https://redgifs.com/watch/{gif_id}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            }
            
            response = requests.get(page_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # Method 1: Look for video source in HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            video_tag = soup.find('video')
            if video_tag:
                source = video_tag.find('source')
                if source and source.get('src'):
                    return gif_id, source.get('src')
            
            # Method 2: Look for meta tags
            meta_tags = soup.find_all('meta')
            for meta in meta_tags:
                if meta.get('property') in ['og:video', 'og:video:url']:
                    video_url = meta.get('content')
                    if video_url and 'redgifs.com' in video_url:
                        return gif_id, video_url
            
            # Method 3: Regex search
            video_patterns = [
                r'"contentUrl":"(https://[^"]*\.redgifs\.com/[^"]*)"',
                r'"video":{"url":"(https://[^"]*)"',
                r'<source src="(https://[^"]*)" type="video/mp4"',
            ]
            
            for pattern in video_patterns:
                matches = re.findall(pattern, response.text)
                if matches:
                    url = matches[0].replace('\\/', '/')
                    return gif_id, url
                    
            return gif_id, None
            
        except Exception as e:
            print(f"Error parsing RedGifs page for {gif_id}: {e}")
            return gif_id, None
    
    # Process in batch with progress bar
    print(f"Fetching {len(gif_ids)} RedGifs URLs...")
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, 5)) as executor:
        future_to_gif = {executor.submit(fetch_single_gif, gif_id): gif_id for gif_id in gif_ids}
        
        for future in tqdm(as_completed(future_to_gif), total=len(gif_ids), desc="RedGifs"):
            gif_id, video_url = future.result()
            if video_url:
                results[gif_id] = video_url
    
    return results

def handle_reddit_gallery(submission, media_dir):
    """
    Download all images from a Reddit gallery.
    """
    try:
        sub = submission.subreddit.display_name
        created = datetime.fromtimestamp(submission.created_utc).strftime("%Y%m%d_%H%M%S")
        gallery_data = submission.media.get('reddit_gallery', {})
        items = gallery_data.get('items', [])
        downloaded_count = 0
        
        for i, item in enumerate(items):
            # Try to get the source URL (full resolution)
            source = item.get('media', {}).get('s', {})
            img_url = source.get('u', '').replace('&amp;', '&')
            if not img_url:
                # Fallback to oembed URL if available
                oembed = item.get('media', {}).get('oembed', {})
                img_url = oembed.get('url', '')
            
            if img_url:
                # Replace mobile format if present
                img_url = img_url.replace('amp;', '')
                if img_url.endswith('?format=jpg&amp;auto=webp&amp;s=...'):
                    img_url = img_url.split('?')[0] + '.jpg'
                
                ext = '.jpg'  # Default for Reddit images
                if img_url.lower().endswith(('.png', '.gif')):
                    ext = os.path.splitext(img_url)[1]
                
                filename = f"{sub}-{submission.id}-{created}_gallery_{i}{ext}"
                filepath = os.path.join(media_dir, filename)
                if download_file(img_url, filepath):
                    downloaded_count += 1
        return downloaded_count
    except Exception as e:
        print(f"Error handling gallery for post {submission.id}: {e}")
        return 0

def process_posts_batch(posts):
    """
    Process posts in optimized batches by type for better performance.
    """
    # Categorize posts by type for batch processing
    redgifs_posts = []
    other_posts = []
    
    for post in posts:
        url = post.url
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        if 'redgifs.com' in domain:
            redgifs_posts.append(post)
        else:
            other_posts.append(post)
    
    # Pre-fetch all RedGifs URLs in batch
    redgifs_urls = {}
    if redgifs_posts:
        gif_ids = []
        post_id_to_gif_id = {}
        
        for post in redgifs_posts:
            parsed = urlparse(post.url)
            gif_id = parsed.path.split('/')[-1].split('.')[0]
            gif_ids.append(gif_id)
            post_id_to_gif_id[post.id] = gif_id
        
        redgifs_urls = get_redgifs_video_url_batch(gif_ids)
    
    # Process all posts
    total_downloaded = 0
    all_posts = redgifs_posts + other_posts
    
    print(f"Downloading media from {len(all_posts)} posts...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_post = {}
        
        for post in all_posts:
            future = executor.submit(process_single_post, post, redgifs_urls, post_id_to_gif_id)
            future_to_post[future] = post.id
        
        for future in tqdm(as_completed(future_to_post), total=len(all_posts), desc="Downloading"):
            try:
                downloaded = future.result()
                total_downloaded += downloaded
            except Exception as e:
                print(f"Error processing post: {e}")
    
    return total_downloaded

def process_single_post(submission, redgifs_urls=None, post_id_to_gif_id=None):
    """
    Process a single post for media download.
    """
    if redgifs_urls is None:
        redgifs_urls = {}
    if post_id_to_gif_id is None:
        post_id_to_gif_id = {}
    
    downloaded_count = 0
    url = submission.url
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    
    sub = submission.subreddit.display_name
    created = datetime.fromtimestamp(submission.created_utc).strftime("%Y%m%d_%H%M%S")
    
    try:
        if 'redgifs.com' in domain:
            # Use pre-fetched RedGifs URL
            gif_id = post_id_to_gif_id.get(submission.id)
            if gif_id and gif_id in redgifs_urls:
                video_url = redgifs_urls[gif_id]
                if video_url:
                    filename = f"{sub}-{submission.id}-{created}_redgifs.mp4"
                    filepath = os.path.join(downloads_dir, filename)
                    if download_file(video_url, filepath):
                        downloaded_count += 1
        
        elif 'i.redd.it' in domain:
            # Handle Reddit images/GIFs
            ext = os.path.splitext(url)[1] or '.jpg'
            filename = f"{sub}-{submission.id}-{created}{ext}"
            filepath = os.path.join(downloads_dir, filename)
            if download_file(url, filepath):
                downloaded_count += 1
        
        elif 'v.redd.it' in domain:
            # Handle Reddit videos
            if hasattr(submission, 'media') and submission.media is not None and 'reddit_video' in submission.media:
                video_data = submission.media['reddit_video']
                video_url = video_data.get('fallback_url')
                if video_url:
                    filename = f"{sub}-{submission.id}-{created}.mp4"
                    filepath = os.path.join(downloads_dir, filename)
                    if download_file(video_url, filepath):
                        downloaded_count += 1
        
        elif hasattr(submission, 'media') and submission.media is not None and 'reddit_gallery' in submission.media:
            # Handle Reddit galleries
            downloaded_count += handle_reddit_gallery(submission, downloads_dir)
        
        elif url.lower().endswith(('.gif', '.png', '.jpg', '.jpeg', '.mp4', '.webm')):
            # Direct media links
            ext = os.path.splitext(url)[1]
            filename = f"{sub}-{submission.id}-{created}{ext}"
            filepath = os.path.join(downloads_dir, filename)
            if download_file(url, filepath):
                downloaded_count += 1
    
    except Exception as e:
        print(f"Unexpected error handling post {submission.id}: {e}")
    
    return downloaded_count

def save_session_stats(posts_processed, files_downloaded):
    """Save session statistics to a JSON file."""
    stats = {
        'session_date': datetime.now().isoformat(),
        'posts_processed': posts_processed,
        'files_downloaded': files_downloaded,
        'post_limit': POST_LIMIT
    }
    
    stats_file = os.path.join(downloads_dir, 'session_stats.json')
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)

def load_previous_stats():
    """Load previous session statistics."""
    stats_file = os.path.join(downloads_dir, 'session_stats.json')
    if os.path.exists(stats_file):
        try:
            with open(stats_file, 'r') as f:
                return json.load(f)
        except:
            return None
    return None

# Main execution
if __name__ == "__main__":
    print(f"🚀 Reddit Media Downloader")
    print(f"📝 Configuration: {POST_LIMIT} posts, {MAX_WORKERS} workers")
    
    # Show previous stats
    previous_stats = load_previous_stats()
    if previous_stats:
        print(f"📊 Previous session: {previous_stats['files_downloaded']} files from {previous_stats['posts_processed']} posts")
    
    print(f"\nFetching up to {POST_LIMIT} media posts from frontpage...")
    
    # Fetch media posts only
    media_posts = []
    max_fetch = 1000  # Safety limit
    fetch_count = 0
    
    for submission in reddit.front.best(limit=max_fetch):
        if len(media_posts) >= POST_LIMIT:
            break
        if is_media_post(submission):
            media_posts.append(submission)
        fetch_count += 1
        if fetch_count >= max_fetch:
            break
    
    print(f"✅ Found {len(media_posts)} media posts (scanned {fetch_count} total posts)")
    
    if len(media_posts) == 0:
        print("❌ No media posts found.")
        exit(0)
    
    # Process posts in optimized batches
    start_time = time.time()
    total_downloaded = process_posts_batch(media_posts)
    end_time = time.time()
    
    # Save session statistics
    save_session_stats(len(media_posts), total_downloaded)
    
    # Print summary
    print(f"\n🎉 Run complete!")
    print(f"📥 Total files downloaded: {total_downloaded}")
    print(f"⏱️  Time taken: {end_time - start_time:.2f} seconds")
    print(f"📁 Files saved in '{downloads_dir}' folder")
    
    if total_downloaded == 0:
        print("💡 Tip: Check if files already exist in downloads folder (skipping duplicates)")
