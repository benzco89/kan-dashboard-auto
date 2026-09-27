# -*- coding: utf-8 -*-
"""
Probe: how much content did the page publish, by type — 2025 vs 2026.

Read-only. Paginates the FB page's published_posts and the IG account's media
list from 2025-01-01 to today, and prints counts per type per period. No
insights calls, no Sheets access, nothing written anywhere except a CSV of
(platform, date, type) rows for the artifact.

Stories are NOT here on purpose: the Graph API only exposes stories while they
are live (24h), so there is no historical stories list to count. Our own
capture ("סטוריז אינסטגרם" sheet) only starts 2026-07-05.
"""
import csv
import os
import sys
import time
from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

TOKEN = os.environ["FACEBOOK_TOKEN"]
API = "v25.0"
PAGE_ID = os.environ.get("FACEBOOK_PAGE_ID") or "220634478361516"
IL = ZoneInfo("Asia/Jerusalem")
START = datetime(2025, 1, 1, tzinfo=IL)


def get_json(url, params=None, retries=4):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            data = r.json()
            if "error" in data:
                code = data["error"].get("code")
                if code in (4, 17, 32, 613):  # rate limits
                    print(f"  rate limited (code {code}), sleeping 60s...", flush=True)
                    time.sleep(60)
                    continue
                raise RuntimeError(data["error"].get("message"))
            return data
        except (requests.RequestException, ValueError):
            if i == retries - 1:
                raise
            time.sleep(5)
    raise RuntimeError("retries exhausted")


def detect_fb_type(post):
    # Same logic as facebook_collector.detect_media_type
    permalink = post.get("permalink_url", "")
    if "/reel/" in permalink:
        return "Reel"
    if "/videos/" in permalink:
        return "Video"
    if "attachments" in post and "data" in post["attachments"]:
        att = post["attachments"]["data"][0]
        att_type = att.get("type", "")
        url = att.get("url", "")
        if "reel" in url or "reel" in att.get("target", {}).get("url", ""):
            return "Reel"
        if att_type in ["video_inline", "video_direct", "video_autoplay", "video"]:
            return "Video"
        if att_type in ["photo", "cover_photo", "album"]:
            return "Photo"
        if att_type in ["share", "link"]:
            return "Link"
    return "Status"


def fetch_facebook():
    print("== Facebook ==", flush=True)
    rows = []
    url = f"https://graph.facebook.com/{API}/{PAGE_ID}/published_posts"
    params = {
        "access_token": TOKEN,
        "limit": 100,
        "fields": "id,created_time,permalink_url,attachments{type,url,target}",
        "since": int(START.timestamp()),
    }
    page = 0
    while url:
        data = get_json(url, params)
        params = None  # the paging.next url carries everything
        batch = data.get("data", [])
        if not batch:
            break
        for post in batch:
            ct = datetime.strptime(post["created_time"], "%Y-%m-%dT%H:%M:%S%z").astimezone(IL)
            if ct < START:
                continue
            rows.append(("facebook", ct.strftime("%Y-%m-%d"), detect_fb_type(post)))
        page += 1
        print(f"  page {page}: total {len(rows)} (oldest {batch[-1].get('created_time', '?')})", flush=True)
        url = data.get("paging", {}).get("next")
    return rows


def fetch_instagram():
    print("== Instagram ==", flush=True)
    me = get_json(f"https://graph.facebook.com/{API}/me",
                  {"access_token": TOKEN, "fields": "id,name,instagram_business_account"})
    ig = me.get("instagram_business_account")
    if not ig:
        pg = get_json(f"https://graph.facebook.com/{API}/{PAGE_ID}",
                      {"access_token": TOKEN, "fields": "instagram_business_account"})
        ig = pg.get("instagram_business_account")
    ig_id = ig["id"]

    rows = []
    url = f"https://graph.facebook.com/{API}/{ig_id}/media"
    params = {
        "access_token": TOKEN,
        "limit": 100,
        "fields": "id,timestamp,media_type,media_product_type",
    }
    page = 0
    done = False
    while url and not done:
        data = get_json(url, params)
        params = None
        batch = data.get("data", [])
        if not batch:
            break
        for m in batch:
            ts = datetime.strptime(m["timestamp"], "%Y-%m-%dT%H:%M:%S%z").astimezone(IL)
            if ts < START:
                done = True
                break
            if m.get("media_product_type") == "REELS":
                mtype = "Reel"
            elif m.get("media_type") == "CAROUSEL_ALBUM":
                mtype = "Carousel"
            elif m.get("media_type") == "IMAGE":
                mtype = "Photo"
            elif m.get("media_type") == "VIDEO":
                mtype = "Video"
            else:
                mtype = m.get("media_type", "?")
            rows.append(("instagram", ts.strftime("%Y-%m-%d"), mtype))
        page += 1
        print(f"  page {page}: total {len(rows)} (oldest {batch[-1].get('timestamp', '?')})", flush=True)
        url = data.get("paging", {}).get("next")
    return rows


def summarize(rows, label, today):
    print(f"\n=== {label} ===", flush=True)
    periods = [
        ("2025 full-year", "2025-01-01", "2025-12-31"),
        ("2025 same-period (to 10/08)", "2025-01-01", f"2025-{today[5:]}"),
        (f"2026 to date (to {today[8:10]}/{today[5:7]})", "2026-01-01", today),
    ]
    for pname, lo, hi in periods:
        c = Counter(r[2] for r in rows if lo <= r[1] <= hi)
        total = sum(c.values())
        parts = ", ".join(f"{k}={v}" for k, v in c.most_common())
        print(f"{pname}: total={total} | {parts}", flush=True)


def main():
    today = datetime.now(IL).strftime("%Y-%m-%d")
    fb = fetch_facebook()
    ig = fetch_instagram()
    summarize(fb, "FACEBOOK", today)
    summarize(ig, "INSTAGRAM", today)

    out = sys.argv[sys.argv.index("--csv") + 1] if "--csv" in sys.argv else None
    if out:
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["platform", "date", "type"])
            w.writerows(fb + ig)
        print(f"\nwrote {len(fb) + len(ig)} rows -> {out}", flush=True)


if __name__ == "__main__":
    main()
