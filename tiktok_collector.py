"""
TikTok Collector - איסוף סרטונים ומדדי מעורבות מטיקטוק
מקור: TikHub (https://api.tikhub.io) - API לא-רשמי, pay-per-call (כמו GetXAPI לטוויטר).
שומר לגוגל שיטס בלבד, בלי שליחה לטלגרם.
מבנה תואם לשאר הקולקטורים: מיזוג חכם לפי video_id + חישוב views_delta.

הרצה רגילה: חלון 7 ימים. משיכה היסטורית: TIKTOK_DAYS_BACK=30 python tiktok_collector.py
"""

import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import json
import pytz

from utils import http_get_json, backfill_zero_metrics

# Load .env file if exists (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Config ---
API_BASE = "https://api.tikhub.io"
USERNAME = os.environ.get("TIKTOK_USERNAME", "kan_news")
DAYS_BACK = int(os.environ.get("TIKTOK_DAYS_BACK", "7"))
# תקרת ביטחון בלבד - בפועל עוצרים כשחוצים את חלון DAYS_BACK.
# @kan_news מעלה ~10-20 סרטונים/יום (20 לעמוד), כך ש-7 ימים ≈ 7 עמודים; החישוב
# מותאם לחלון בפועל כדי שגם TIKTOK_DAYS_BACK=30 יעבור בשלמות.
MAX_PAGES = max(15, DAYS_BACK * 2)

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET_NAME = "נתוני טיקטוק"

IL_TZ = pytz.timezone("Asia/Jerusalem")

# --- TikHub Functions ---

def get_api_key():
    return os.environ.get("TIKHUB_TOKEN", "")


def _headers():
    return {"Authorization": f"Bearer {get_api_key()}"}


def get_sec_uid(username):
    """המרת שם משתמש ל-secUid (ה-endpoint של הפרופיל רץ על ה-Web API)"""
    data = http_get_json(
        f"{API_BASE}/api/v1/tiktok/web/fetch_user_profile",
        headers=_headers(), params={"uniqueId": username},
    )
    return data["data"]["userInfo"]["user"]["secUid"]


def get_videos(sec_uid, cutoff, max_pages=MAX_PAGES):
    """
    משיכת סרטונים אחרונים עם pagination חכמה לפי תאריך (App V3 - היציב;
    ה-Web API המקביל נוטה ליפול בצד של TikHub).
    הסרטונים מגיעים מהחדש לישן, אבל סרטונים נעוצים (is_top=1) חוזרים ראשונים
    בלי קשר לגילם - לכן בדיקת ה-cutoff מתעלמת מנעוצים, אחרת סרטון נעוץ ישן
    היה עוצר את המשיכה בעמוד הראשון.
    """
    all_videos = []
    cursor = 0
    stop_reason = "max_pages"  # ברירת מחדל: עצרנו כי נגמרה התקרה (כיסוי חלקי!)
    pages_used = 0

    for page in range(max_pages):
        pages_used = page + 1
        params = {
            "sec_user_id": sec_uid,
            "max_cursor": cursor,
            "count": 20,
            "sort_type": 0,
        }

        try:
            data = http_get_json(
                f"{API_BASE}/api/v1/tiktok/app/v3/fetch_user_post_videos",
                headers=_headers(), params=params,
            )
        except Exception as e:
            print(f"❌ TikHub error on page {pages_used}: {e}")
            stop_reason = "error"
            break

        payload = data.get("data") or {}
        page_videos = payload.get("aweme_list")
        if not isinstance(page_videos, list) or not page_videos:
            detail = data.get("detail") or {}
            if detail:
                print(f"❌ TikHub unexpected response on page {pages_used}: {str(detail)[:200]}")
                stop_reason = "error"
            else:
                stop_reason = "end_of_feed"
            break
        all_videos.extend(page_videos)

        # האם הגענו אל מעבר לחלון? (סרטון לא-נעוץ מוקדם מה-cutoff)
        reached_cutoff = any(
            not v.get("is_top")
            and (ct := parse_video_time(v)) and ct < cutoff
            for v in page_videos
        )
        if reached_cutoff:
            stop_reason = "cutoff"
            break

        if not payload.get("has_more"):
            stop_reason = "end_of_feed"
            break
        cursor = payload.get("max_cursor", 0)

    print(f"   ({pages_used} pages fetched, stop={stop_reason})")
    if stop_reason == "max_pages":
        print(f"⚠️ Hit MAX_PAGES={max_pages} before reaching the {DAYS_BACK}-day window — "
              f"coverage is PARTIAL. Raise MAX_PAGES for full coverage.")
    return all_videos


def parse_video_time(video):
    """המרת זמן יצירת הסרטון ל-datetime בשעון ישראל"""
    ts = video.get("create_time")
    if not ts:
        return None
    return datetime.fromtimestamp(int(ts), tz=pytz.utc).astimezone(IL_TZ)


def classify_type(video):
    """זיהוי סוג הפריט: תמונות (קרוסלה) או וידאו רגיל"""
    if video.get("image_infos") or video.get("aweme_type") == 150:
        return "Photo"
    return "Video"


# --- Build DataFrame ---

def fetch_tiktok_data():
    print(f"🚀 TikTok Collector - @{USERNAME} - {datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M')}")

    if not get_api_key():
        print("❌ Missing TIKHUB_TOKEN environment variable")
        return pd.DataFrame()

    try:
        sec_uid = get_sec_uid(USERNAME)
    except Exception as e:
        print(f"❌ Failed to resolve secUid for @{USERNAME}: {e}")
        return pd.DataFrame()

    cutoff = datetime.now(IL_TZ) - timedelta(days=DAYS_BACK)
    raw_videos = get_videos(sec_uid, cutoff)
    print(f"📥 Fetched {len(raw_videos)} raw videos")

    rows = []
    seen_ids = set()
    for v in raw_videos:
        created = parse_video_time(v)
        if not created or created < cutoff:
            continue

        vid = str(v.get("aweme_id", ""))
        if not vid or vid in seen_ids:  # נעוצים חוזרים גם במיקומם הכרונולוגי
            continue
        seen_ids.add(vid)

        s = v.get("statistics") or {}
        views = int(s.get("play_count", 0) or 0)
        likes = int(s.get("digg_count", 0) or 0)
        comments = int(s.get("comment_count", 0) or 0)
        shares = int(s.get("share_count", 0) or 0)
        whatsapp_shares = int(s.get("whatsapp_share_count", 0) or 0)
        saves = int(s.get("collect_count", 0) or 0)
        downloads = int(s.get("download_count", 0) or 0)

        total_eng = likes + comments + shares + saves
        engagement_rate = round((total_eng / views) * 100, 2) if views > 0 else 0

        duration_sec = round((v.get("video") or {}).get("duration", 0) / 1000)

        rows.append({
            "video_id": vid,
            "date": created.strftime("%Y-%m-%d"),
            "time": created.strftime("%H:%M"),
            "type": classify_type(v),
            "title": (v.get("desc", "") or "").replace("\n", " ")[:500],
            "duration_sec": duration_sec,
            "views": views,
            "likes": likes,
            "comments": comments,
            "shares": shares,
            "whatsapp_shares": whatsapp_shares,
            "saves": saves,
            "downloads": downloads,
            "total_engagement": total_eng,
            "engagement_rate": engagement_rate,
            "is_pinned": 1 if v.get("is_top") else 0,
            "permalink": f"https://www.tiktok.com/@{USERNAME}/video/{vid}",
            "pulled_at": datetime.now(IL_TZ).strftime("%Y-%m-%d %H:%M"),
        })

    print(f"📊 {len(rows)} videos in the last {DAYS_BACK} days")
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
    suspicious_cols = []
    if not existing_df.empty and "video_id" in existing_df.columns:
        new_df["video_id"] = new_df["video_id"].astype(str)
        existing_df["video_id"] = existing_df["video_id"].astype(str)

        # הגנה מפני כשלי API רגעיים: 0 חדש לא דורס ערך חיובי קיים
        new_df, suspicious_cols = backfill_zero_metrics(
            new_df, existing_df, key="video_id",
            cols=["views", "likes", "comments", "shares", "whatsapp_shares",
                  "saves", "downloads", "total_engagement", "engagement_rate"]
        )

        # חישוב דלתא לצפיות
        if "views" in existing_df.columns:
            existing_df["views"] = pd.to_numeric(existing_df["views"], errors="coerce").fillna(0)
            views_map = existing_df.set_index("video_id")["views"].to_dict()
            new_df["views_delta"] = new_df.apply(
                lambda x: x["views"] - views_map.get(x["video_id"], x["views"]), axis=1
            )
        else:
            new_df["views_delta"] = 0

        # וידוא עמודות
        for col in new_df.columns:
            if col not in existing_df.columns:
                existing_df[col] = ""

        combined = pd.concat([new_df, existing_df])
        final_df = combined.drop_duplicates(subset=["video_id"], keep="first")
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
    return suspicious_cols


def main():
    print(f"\n{'='*50}")
    print(f"🎵 TikTok Collector - {datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}\n")

    df = fetch_tiktok_data()
    if not df.empty:
        save_to_sheets(df)
        print(f"\n✅ Done! {len(df)} videos processed.")
    else:
        print("❌ No data collected.")


if __name__ == "__main__":
    main()
