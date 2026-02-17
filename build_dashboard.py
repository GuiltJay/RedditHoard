#!/usr/bin/env python3
"""export_dashboard_data.py - Export SQLite data to JSON for the GitHub Pages dashboard."""

import sqlite3
import json
import os
from collections import defaultdict
from datetime import datetime, timezone

DB_FILE = "reddit_stats.db"
OUTPUT_FILE = "dashboard_data.json"

def export():
    if not os.path.exists(DB_FILE):
        print(f"[ERROR] {DB_FILE} not found.")
        return

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row

    data = {}

    # ── 1. Overview / KPIs ──
    cur = conn.execute("SELECT COUNT(*) as total_posts FROM posts")
    data["total_posts"] = cur.fetchone()["total_posts"]

    cur = conn.execute("SELECT COALESCE(SUM(downloaded_count), 0) as total_files FROM posts")
    data["total_files_downloaded"] = cur.fetchone()["total_files"]

    cur = conn.execute("SELECT COUNT(DISTINCT subreddit) as total_subs FROM posts")
    data["total_subreddits"] = cur.fetchone()["total_subs"]

    cur = conn.execute("SELECT COUNT(DISTINCT fetched_date) as total_days FROM posts")
    data["total_active_days"] = cur.fetchone()["total_days"]

    cur = conn.execute("SELECT MIN(fetched_date) as first, MAX(fetched_date) as last FROM posts")
    row = cur.fetchone()
    data["first_fetch_date"] = row["first"]
    data["last_fetch_date"] = row["last"]

    cur = conn.execute("SELECT MIN(created_utc) as oldest, MAX(created_utc) as newest FROM posts")
    row = cur.fetchone()
    data["oldest_post_utc"] = row["oldest"]
    data["newest_post_utc"] = row["newest"]

    # ── 2. Posts per fetched_date ──
    cur = conn.execute("""
        SELECT fetched_date, COUNT(*) as cnt, COALESCE(SUM(downloaded_count), 0) as dl
        FROM posts GROUP BY fetched_date ORDER BY fetched_date
    """)
    data["posts_by_date"] = [dict(r) for r in cur.fetchall()]

    # ── 3. Posts per subreddit (top 50) ──
    cur = conn.execute("""
        SELECT subreddit, COUNT(*) as cnt, COALESCE(SUM(downloaded_count), 0) as dl
        FROM posts GROUP BY subreddit ORDER BY cnt DESC LIMIT 50
    """)
    data["top_subreddits"] = [dict(r) for r in cur.fetchall()]

    # ── 4. All subreddits full list ──
    cur = conn.execute("""
        SELECT subreddit, COUNT(*) as cnt, COALESCE(SUM(downloaded_count), 0) as dl,
               MIN(fetched_date) as first_seen, MAX(fetched_date) as last_seen
        FROM posts GROUP BY subreddit ORDER BY cnt DESC
    """)
    data["all_subreddits"] = [dict(r) for r in cur.fetchall()]

    # ── 5. Hourly distribution (hour of created_utc) ──
    cur = conn.execute("""
        SELECT CAST(strftime('%H', created_utc, 'unixepoch') AS INTEGER) as hour,
               COUNT(*) as cnt
        FROM posts GROUP BY hour ORDER BY hour
    """)
    data["posts_by_hour"] = [dict(r) for r in cur.fetchall()]

    # ── 6. Day-of-week distribution ──
    cur = conn.execute("""
        SELECT CAST(strftime('%w', created_utc, 'unixepoch') AS INTEGER) as dow,
               COUNT(*) as cnt
        FROM posts GROUP BY dow ORDER BY dow
    """)
    data["posts_by_dow"] = [dict(r) for r in cur.fetchall()]

    # ── 7. Download rate by date ──
    cur = conn.execute("""
        SELECT fetched_date,
               COUNT(*) as total,
               SUM(CASE WHEN downloaded_count > 0 THEN 1 ELSE 0 END) as with_dl,
               COALESCE(SUM(downloaded_count), 0) as files
        FROM posts GROUP BY fetched_date ORDER BY fetched_date
    """)
    data["download_rate_by_date"] = [dict(r) for r in cur.fetchall()]

    # ── 8. daily_stats: by date + source ──
    cur = conn.execute("""
        SELECT date, source,
               SUM(posts_fetched) as posts, SUM(files_downloaded) as files
        FROM daily_stats GROUP BY date, source ORDER BY date, source
    """)
    data["daily_stats_by_source"] = [dict(r) for r in cur.fetchall()]

    # ── 9. daily_stats: source totals ──
    cur = conn.execute("""
        SELECT source,
               SUM(posts_fetched) as posts, SUM(files_downloaded) as files
        FROM daily_stats GROUP BY source ORDER BY posts DESC
    """)
    data["source_totals"] = [dict(r) for r in cur.fetchall()]

    # ── 10. daily_stats: top subreddits by files ──
    cur = conn.execute("""
        SELECT subreddit,
               SUM(posts_fetched) as posts, SUM(files_downloaded) as files
        FROM daily_stats GROUP BY subreddit ORDER BY files DESC LIMIT 30
    """)
    data["daily_stats_top_subs"] = [dict(r) for r in cur.fetchall()]

    # ── 11. Cumulative posts over time ──
    cur = conn.execute("""
        SELECT fetched_date, COUNT(*) as cnt
        FROM posts GROUP BY fetched_date ORDER BY fetched_date
    """)
    rows = cur.fetchall()
    cumulative = []
    running = 0
    for r in rows:
        running += r["cnt"]
        cumulative.append({"fetched_date": r["fetched_date"], "cumulative": running})
    data["cumulative_posts"] = cumulative

    # ── 12. Monthly aggregation ──
    cur = conn.execute("""
        SELECT strftime('%Y-%m', fetched_date) as month,
               COUNT(*) as cnt, COALESCE(SUM(downloaded_count), 0) as dl
        FROM posts GROUP BY month ORDER BY month
    """)
    data["posts_by_month"] = [dict(r) for r in cur.fetchall()]

    # ── 13. Subreddit activity heatmap data (sub x date) top 20 subs ──
    cur = conn.execute("""
        SELECT subreddit FROM posts
        GROUP BY subreddit ORDER BY COUNT(*) DESC LIMIT 20
    """)
    top20 = [r["subreddit"] for r in cur.fetchall()]
    heatmap = []
    for sub in top20:
        cur = conn.execute("""
            SELECT fetched_date, COUNT(*) as cnt
            FROM posts WHERE subreddit=? GROUP BY fetched_date ORDER BY fetched_date
        """, (sub,))
        for r in cur.fetchall():
            heatmap.append({"subreddit": sub, "date": r["fetched_date"], "count": r["cnt"]})
    data["subreddit_heatmap"] = heatmap

    # ── 14. Posts with vs without downloads ──
    cur = conn.execute("""
        SELECT
            SUM(CASE WHEN downloaded_count > 0 THEN 1 ELSE 0 END) as with_media,
            SUM(CASE WHEN downloaded_count = 0 THEN 1 ELSE 0 END) as without_media
        FROM posts
    """)
    row = cur.fetchone()
    data["media_split"] = {"with_media": row["with_media"] or 0, "without_media": row["without_media"] or 0}

    # ── 15. Average downloads per post by subreddit (top 30, min 5 posts) ──
    cur = conn.execute("""
        SELECT subreddit, COUNT(*) as cnt,
               ROUND(AVG(downloaded_count), 2) as avg_dl
        FROM posts GROUP BY subreddit HAVING cnt >= 5
        ORDER BY avg_dl DESC LIMIT 30
    """)
    data["avg_downloads_per_sub"] = [dict(r) for r in cur.fetchall()]

    conn.close()

    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"✅ Exported dashboard data to {OUTPUT_FILE}")
    print(f"   Posts: {data['total_posts']} | Files: {data['total_files_downloaded']} | Subs: {data['total_subreddits']}")

if __name__ == "__main__":
    export() 
