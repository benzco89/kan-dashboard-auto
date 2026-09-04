# -*- coding: utf-8 -*-
"""ארכיון וידאו - כל סרטון שכאן חדשות מפרסמת לאינסטגרם ולטיקטוק, לדרייב.

זהו אחיו של hot_sniffer.py ולא של הפייפליין היומי: ריצה תוך-יומית, גילוי ישירות
מה-API של הפלטפורמות, כתיבה **רק** לגיליון משלו. הגיליונות היומיים אינם יכולים
לשמש מקור גילוי - הקולקטורים כותבים פעם ביום ב-08:30, אז ארכיון שרץ כל שעתיים
מולם לא היה רואה דבר בין ריצה לריצה.

הסינון הוא מה שהופך את הקצב לחינמי: פריט שכבר באינדקס לא יורד שוב, אז ריצה
שלא מצאה חדש עושה קריאת API אחת לפלטפורמה ויוצאת. העלות מתקנת לפי פריטים
שפורסמו (~11 ביום), לא לפי תדירות הריצה.

**כלום לא מדודפל בזמן לכידה.** פריט יכול להגיע לטיקטוק ב-14:00 ולאינסטגרם
ב-16:00, ואי אפשר לדעת בהגעת הראשון איזה עותק "טוב יותר". כל עותק נשמר; מעבר
לילי (--reconcile) מקשר ביניהם ואינו מוחק לעולם.

Env: FACEBOOK_TOKEN, TIKHUB_TOKEN, GCP_SERVICE_ACCOUNT, GEMINI_API_KEY,
     GDRIVE_CLIENT_ID, GDRIVE_CLIENT_SECRET, GDRIVE_REFRESH_TOKEN,
     GDRIVE_ROOT_FOLDER_ID (אופציונלי).
"""

import os
import re
import sys
import json
from datetime import datetime, timedelta

import gspread
import pytz
from google.oauth2.service_account import Credentials

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "social_dashboard"))
from content_tags import tag_item, strip_bidi  # noqa: E402
from utils import http_get_json  # noqa: E402

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ACCESS_TOKEN = os.environ.get("FACEBOOK_TOKEN")
API_VERSION = "v25.0"
BASE = f"https://graph.facebook.com/{API_VERSION}"

TIKHUB_TOKEN = os.environ.get("TIKHUB_TOKEN")
TIKTOK_USERNAME = os.environ.get("TIKTOK_USERNAME", "kan_news")
TIKTOK_SEC_UID = os.environ.get(
    "TIKTOK_SEC_UID",
    "MS4wLjABAAAA3p5tyX2Z3cacCWU34-nHbK-dpVBO5Y6IGvTj9xufL60rC6ItchtdzkEe-0frXJZX")

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
INDEX_SHEET = "ארכיון וידאו"
ARCHIVER_VERSION = "1.0"

# רחב בכוונה מ-YOUNG_HOURS=24 של הרחרחן: הרחרחן שואל "האם זה מתפוצץ עכשיו",
# שאלה עם חיי מדף קצרים; הארכיון רק צריך שריצה שהוחמצה תתאושש בבאה אחריה.
ARCHIVE_LOOKBACK_HOURS = 48

IL_TZ = pytz.timezone("Asia/Jerusalem")

INDEX_HEADER = [
    "post_id", "platform", "posted_at", "permalink", "caption",
    "drive_file_id", "drive_path", "bytes", "duration_sec",
    "person", "program", "program_source",
    "category", "tags", "summary", "credit_flag",
    "same_as", "archived_at", "archiver_version",
]


def open_spreadsheet():
    creds_json = (os.environ.get("GCP_SERVICE_ACCOUNT")
                  or os.environ.get("GOOGLE_CREDENTIALS"))
    creds_dict = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)


def get_index(sh):
    """גיליון האינדקס + סט המפתחות שכבר בו. הסט הוא כל הזיכרון של המערכת."""
    try:
        ws = sh.worksheet(INDEX_SHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=INDEX_SHEET, rows=2000,
                              cols=len(INDEX_HEADER))
        ws.append_row(INDEX_HEADER, value_input_option="RAW")
        print(f"✅ נוצר גיליון {INDEX_SHEET}")
        return ws, set()
    rows = ws.get_all_values()
    known = {(str(r[1]).strip(), str(r[0]).strip())
             for r in rows[1:] if len(r) > 1 and str(r[0]).strip()}
    return ws, known


def filter_new(items, known):
    """מה שעוד לא בארכיון. זו התכונה שכל הקצב נשען עליה."""
    return [i for i in items if (i["platform"], str(i["id"])) not in known]


def _parse_ts(ts):
    ts = re.sub(r"\+0000$", "+00:00", str(ts).replace("Z", "+00:00"))
    return datetime.fromisoformat(ts).astimezone(IL_TZ)


def discover_instagram(hours=ARCHIVE_LOOKBACK_HOURS):
    """רילסים מהחלון. media_url **לא** נשמר על הפריט - הוא נפתר בזמן ההורדה."""
    res = http_get_json(f"{BASE}/me", params={
        "access_token": ACCESS_TOKEN, "fields": "instagram_business_account"})
    ig_id = (res.get("instagram_business_account") or {}).get("id")
    if not ig_id:
        print("⚠️ לא הצלחתי לזהות את חשבון האינסטגרם")
        return []
    res = http_get_json(f"{BASE}/{ig_id}/media", params={
        "access_token": ACCESS_TOKEN,
        "fields": "id,caption,timestamp,permalink,media_type,media_product_type",
        "limit": 50,
    })
    cutoff = datetime.now(IL_TZ) - timedelta(hours=hours)
    out = []
    for m in res.get("data", []):
        if m.get("media_type") != "VIDEO":
            continue
        try:
            posted = _parse_ts(m.get("timestamp"))
        except (ValueError, TypeError):
            continue
        if posted < cutoff:
            continue
        out.append({
            "id": str(m["id"]), "platform": "instagram",
            "posted": posted, "permalink": m.get("permalink", ""),
            "caption": m.get("caption") or "", "duration_sec": "",
        })
    return out


def discover_tiktok(hours=ARCHIVE_LOOKBACK_HOURS):
    """סרטונים מהחלון. play_addr הוא העותק **בלי** הסימן - download_addr עם."""
    if not TIKHUB_TOKEN:
        print("⚠️ אין TIKHUB_TOKEN - מדלג על טיקטוק")
        return []
    res = http_get_json(
        "https://api.tikhub.io/api/v1/tiktok/app/v3/fetch_user_post_videos",
        headers={"Authorization": f"Bearer {TIKHUB_TOKEN}"},
        params={"sec_user_id": TIKTOK_SEC_UID, "max_cursor": 0,
                "count": 30, "sort_type": 0},
    )
    cutoff = datetime.now(IL_TZ) - timedelta(hours=hours)
    out = []
    for v in ((res.get("data") or {}).get("aweme_list") or []):
        ts = v.get("create_time")
        if not ts:
            continue
        posted = datetime.fromtimestamp(int(ts), tz=pytz.utc).astimezone(IL_TZ)
        if posted < cutoff:
            continue
        video = v.get("video") or {}
        urls = list(((video.get("play_addr") or {}).get("url_list")) or [])
        vid = str(v.get("aweme_id", ""))
        out.append({
            "id": vid, "platform": "tiktok", "posted": posted,
            "permalink": f"https://www.tiktok.com/@{TIKTOK_USERNAME}/video/{vid}",
            "caption": v.get("desc") or "",
            "duration_sec": round((video.get("duration") or 0) / 1000) or "",
            "_tiktok_urls": urls,
        })
    return out
