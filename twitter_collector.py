"""
Twitter/X Collector - איסוף ציוצים ומדדי מעורבות מטוויטר
מקור: GetXAPI (https://api.getxapi.com). שומר לגוגל שיטס בלבד, בלי שליחה לטלגרם.
מבנה תואם לשאר הקולקטורים: מיזוג חכם לפי tweet_id + חישוב views_delta.
"""

import os
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time
import json
import re
import pytz

from utils import http_get_json, backfill_zero_metrics

# Load .env file if exists (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Config ---
API_BASE = "https://api.getxapi.com"
USERNAME = os.environ.get("TWITTER_USERNAME", "kann_news")
DAYS_BACK = 7
# תקרת ביטחון בלבד - בפועל עוצרים כשחוצים את חלון DAYS_BACK.
# @kann_news מצייץ ~40 ציוצים/יום (~18 לעמוד), כך ש-7 ימים ≈ 16 עמודים; 30 נותן מרווח לימים כבדים.
MAX_PAGES = 30

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET_NAME = "נתוני טוויטר"

IL_TZ = pytz.timezone("Asia/Jerusalem")

# --- GetXAPI Functions ---

def get_api_key():
    return os.environ.get("GETXAPI_KEY", "")


def _headers():
    return {"Authorization": f"Bearer {get_api_key()}"}


def get_tweets(username, cutoff, max_pages=MAX_PAGES):
    """
    משיכת ציוצים אחרונים עם pagination חכמה לפי תאריך.
    הציוצים מגיעים מהחדש לישן; עוצרים ברגע שעמוד כולל ציוץ ישן מ-cutoff
    (כלומר כיסינו את כל החלון), או כשנגמרו העמודים / נחצתה תקרת הביטחון.
    """
    all_tweets = []
    cursor = None
    stop_reason = "max_pages"  # ברירת מחדל: עצרנו כי נגמרה התקרה (כיסוי חלקי!)
    pages_used = 0

    for page in range(max_pages):
        pages_used = page + 1
        params = {"userName": username}
        if cursor:
            params["cursor"] = cursor

        try:
            data = http_get_json(f"{API_BASE}/twitter/user/tweets", headers=_headers(), params=params)
        except Exception as e:
            print(f"❌ GetXAPI error on page {pages_used}: {e}")
            stop_reason = "error"
            break

        if "tweets" not in data:
            print(f"❌ GetXAPI unexpected response on page {pages_used}: {str(data)[:200]}")
            stop_reason = "error"
            break

        page_tweets = data.get("tweets", [])
        all_tweets.extend(page_tweets)

        # האם הגענו אל מעבר לחלון? (קיים בעמוד ציוץ מוקדם מה-cutoff)
        reached_cutoff = any(
            (ct := parse_tweet_time(t)) and ct.astimezone(IL_TZ) < cutoff
            for t in page_tweets
        )
        if reached_cutoff:
            stop_reason = "cutoff"
            break

        if not data.get("has_more") or not data.get("next_cursor"):
            stop_reason = "end_of_feed"
            break
        cursor = data["next_cursor"]

    print(f"   ({pages_used} pages fetched, stop={stop_reason})")
    if stop_reason == "max_pages":
        print(f"⚠️ Hit MAX_PAGES={max_pages} before reaching the {DAYS_BACK}-day window — "
              f"coverage is PARTIAL. Raise MAX_PAGES for full coverage.")
    return all_tweets


def parse_tweet_time(tweet):
    """המרת זמן יצירת הציוץ ל-datetime מודע-אזור"""
    try:
        return datetime.strptime(tweet["createdAt"], "%a %b %d %H:%M:%S %z %Y")
    except (ValueError, KeyError):
        return None


def classify_media(tweet):
    """זיהוי סוג מדיה של הציוץ"""
    media = tweet.get("media") or []
    types = {m.get("type") for m in media}
    if "video" in types:
        return "Video"
    if "photo" in types or "animated_gif" in types:
        return "Photo"
    return "Text"


def tweet_id_of(tweet):
    """חילוץ מזהה הציוץ (שדה ישיר או מתוך ה-URL)"""
    for key in ("id", "tweetId", "id_str", "rest_id"):
        val = tweet.get(key)
        if val:
            return str(val)
    url = tweet.get("url", "")
    m = re.search(r"/status/(\d+)", url)
    return m.group(1) if m else ""


# --- Build DataFrame ---

def fetch_twitter_data():
    print(f"🚀 Twitter Collector - @{USERNAME} - {datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M')}")

    if not get_api_key():
        print("❌ Missing GETXAPI_KEY environment variable")
        return pd.DataFrame()

    cutoff = datetime.now(IL_TZ) - timedelta(days=DAYS_BACK)
    raw_tweets = get_tweets(USERNAME, cutoff)
    print(f"📥 Fetched {len(raw_tweets)} raw tweets")

    rows = []
    for t in raw_tweets:
        created = parse_tweet_time(t)
        if not created:
            continue
        created_il = created.astimezone(IL_TZ)
        if created_il < cutoff:
            continue

        views = int(t.get("viewCount", 0) or 0)
        likes = int(t.get("likeCount", 0) or 0)
        retweets = int(t.get("retweetCount", 0) or 0)
        replies = int(t.get("replyCount", 0) or 0)
        quotes = int(t.get("quoteCount", 0) or 0)
        bookmarks = int(t.get("bookmarkCount", 0) or 0)

        total_eng = likes + retweets + replies + quotes
        engagement_rate = round((total_eng / views) * 100, 2) if views > 0 else 0

        tid = tweet_id_of(t)
        rows.append({
            "tweet_id": tid,
            "date": created_il.strftime("%Y-%m-%d"),
            "time": created_il.strftime("%H:%M"),
            "type": classify_media(t),
            "text": (t.get("text", "") or "").replace("\n", " ")[:500],
            "views": views,
            "likes": likes,
            "retweets": retweets,
            "replies": replies,
            "quotes": quotes,
            "bookmarks": bookmarks,
            "total_engagement": total_eng,
            "engagement_rate": engagement_rate,
            "permalink": t.get("url", "") or (f"https://x.com/{USERNAME}/status/{tid}" if tid else ""),
            "pulled_at": datetime.now(IL_TZ).strftime("%Y-%m-%d %H:%M"),
        })

    print(f"📊 {len(rows)} tweets in the last {DAYS_BACK} days")
    return pd.DataFrame(rows)


def save_to_sheets(new_df):
    """שמירה חכמה לגוגל שיטס עם מיזוג נתונים (זהה בדפוס לשאר הקולקטורים)"""
    if new_df.empty:
        print("⚠️ No data to save")
        return

    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT") or os.environ.get("GOOGLE_CREDENTIALS")
    creds_dict = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = sh.worksheet(SHEET_NAME)
    except Exception:
        worksheet = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=20)
        print(f"✅ Created new sheet: {SHEET_NAME}")

    # קריאת היסטוריה
    try:
        existing_df = pd.DataFrame(worksheet.get_all_records())
    except Exception as e:
        print(f"⚠️ Warning reading existing data: {e}")
        existing_df = pd.DataFrame()

    # מיזוג
    if not existing_df.empty and "tweet_id" in existing_df.columns:
        new_df["tweet_id"] = new_df["tweet_id"].astype(str)
        existing_df["tweet_id"] = existing_df["tweet_id"].astype(str)

        # הגנה מפני כשלי API רגעיים: 0 חדש לא דורס ערך חיובי קיים
        new_df = backfill_zero_metrics(
            new_df, existing_df, key="tweet_id",
            cols=["views", "likes", "retweets", "replies", "quotes",
                  "bookmarks", "total_engagement", "engagement_rate"]
        )

        # חישוב דלתא לצפיות
        if "views" in existing_df.columns:
            existing_df["views"] = pd.to_numeric(existing_df["views"], errors="coerce").fillna(0)
            views_map = existing_df.set_index("tweet_id")["views"].to_dict()
            new_df["views_delta"] = new_df.apply(
                lambda x: x["views"] - views_map.get(x["tweet_id"], x["views"]), axis=1
            )
        else:
            new_df["views_delta"] = 0

        # וידוא עמודות
        for col in new_df.columns:
            if col not in existing_df.columns:
                existing_df[col] = ""

        combined = pd.concat([new_df, existing_df])
        final_df = combined.drop_duplicates(subset=["tweet_id"], keep="first")
        print(f"🔄 Merged: {len(new_df)} new/updated + {len(existing_df)} existing -> {len(final_df)} total")
    else:
        new_df["views_delta"] = 0
        final_df = new_df

    # ניקוי ומיון
    final_df = final_df.sort_values(by="date", ascending=False)
    final_df = final_df.fillna(0).replace([float("inf"), float("-inf")], 0)

    # שמירה
    worksheet.clear()
    worksheet.update([final_df.columns.tolist()] + final_df.values.tolist())
    print(f"✅ Saved {len(final_df)} rows to {SHEET_NAME}")


def main():
    print(f"\n{'='*50}")
    print(f"🐦 Twitter Collector - {datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}\n")

    df = fetch_twitter_data()
    if not df.empty:
        save_to_sheets(df)
        print(f"\n✅ Done! {len(df)} tweets processed.")
    else:
        print("❌ No data collected.")


if __name__ == "__main__":
    main()
