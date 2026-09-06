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
     GDRIVE_ROOT_FOLDER_ID (אופציונלי, ולא מומלץ - ראו drive_store).
"""

import os
import re
import sys
import json
import shutil
import subprocess
import tempfile
import argparse
from datetime import datetime, timedelta

import gspread
import pytz
from google import genai
from google.genai import types
from google.oauth2.service_account import Credentials

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "social_dashboard"))
from content_tags import tag_item, strip_bidi  # noqa: E402
from utils import http_get_json  # noqa: E402
import drive_store  # noqa: E402

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

# חוברת נפרדת משל הארכיון, ולא חוברת האנליטיקס של הדשבורד: הארכיון לא קורא
# מאף גיליון קולקטור (המצב היחיד שלו הוא INDEX_SHEET), אז אין צימוד נתונים
# לשמור עליו - ומי שיצטרך לקרוא את האינדקס מבחוץ לא אמור לקבל איתו את כל
# נתוני הרשתות. הקובץ יושב בתיקיית הארכיון בדרייב ומשותף לחשבון השירות.
SPREADSHEET_ID = os.environ.get(
    "ARCHIVE_SPREADSHEET_ID",
    "1mktwIgMj8HOh6n066o4rc1Cat8cxVea0DHFpfVuKTaI")
INDEX_SHEET = "ארכיון וידאו"
ARCHIVER_VERSION = "1.0"

# 24 שעות, מול 48 עד 2026-09-06. החלון אינו עולה דבר - הקריאה זהה (30
# פריטים בטיקטוק, 50 בגראף) והוא רק מסנן את מה שכבר חזר - אז כל תפקידו הוא
# מרווח הביטחון: כמה ריצות רשאיות להיכשל ברצף בלי שפריט יאבד. משהתחילו
# הריצות כל שעתיים **סביב השעון** (kan-media-archiver.timer), 24 שעות הן
# 12 ניסיונות לכל פריט, וזה המרווח שנבחר. אין להעמיק את החלון מעבר ל-4 ימים
# בערך בלי pagination: הדף אינו מדפדף, ו-discover_* יזעיקו על כיסוי חלקי.
ARCHIVE_LOOKBACK_HOURS = int(os.environ.get("ARCHIVE_LOOKBACK_HOURS", "24"))

IL_TZ = pytz.timezone("Asia/Jerusalem")

INDEX_HEADER = [
    "post_id", "platform", "posted_at", "permalink", "caption",
    "drive_file_id", "drive_path", "bytes", "duration_sec",
    "person", "program", "program_source",
    "category", "tags", "summary", "credit_flag",
    "same_as", "archived_at", "archiver_version",
    # תמיד אחרון. השורות נכתבות לפי מיקום, אז עמודה שנוספת באמצע משנה את
    # המשמעות של כל ערך אחריה בכל 18 השורות שכבר בגיליון - זה בדיוק התקלה
    # ש-verify_collector.py קיים בשבילה.
    "deleted_at",
    # נמדד ב-ffprobe אחרי ההורדה, לפני ההעלאה. נמדד 2026-09-06: אינסטגרם
    # מגישה 716x1266 ואין לה רנדישן אחר (ig_quality_probe.py), טיקטוק מגישה
    # 1080x1910 - לרוב, לא תמיד. לכן "מי העותק המועדף" נקבע מהמספרים האלה
    # ולא מהפלטפורמה. preferred נכתב רק ע"י run_reconcile.
    "width", "height", "kbps", "preferred",
]

# הקובץ נגרע מהדרייב אחרי שבוע; **השורה נשארת**. הארכיון הזה הוא באפר ולא
# ארכיון, וזו החלטה מודעת (2026-09-06): הדרייב פרטי. המחיר, כדי שלא יופתעו
# ממנו - הגילוי אינו מדפדף (50 בגראף, 30 ב-TikHub, כלומר 4-6 ימים), וה-URL
# של המדיה חתום ופג, אז קובץ שנגרע **אינו ניתן להשגה חוזרת**. מי שצורך את
# הארכיון חייב למשוך בתוך החלון.
RETENTION_DAYS = int(os.environ.get("ARCHIVE_RETENTION_DAYS", "7"))


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


def peek_index(sh):
    """כמו get_index, אבל לעולם לא יוצר גיליון - למצב יבש שמבטיח לא לגעת בכלום.

    get_index רגיל היה נכון גם כאן, אבל --dry-run מתועד כ"בלי הורדה, העלאה
    או כתיבה", וזה מפר את ההבטחה: הריצה הראשונה בחיי הגיליון הייתה כותבת
    שורת כותרת אמיתית דרך add_worksheet + append_row.
    """
    try:
        ws = sh.worksheet(INDEX_SHEET)
    except gspread.WorksheetNotFound:
        print(f"ℹ️ גיליון {INDEX_SHEET} עוד לא קיים - במצב יבש לא נוצר.")
        return set()
    rows = ws.get_all_values()
    return {(str(r[1]).strip(), str(r[0]).strip())
            for r in rows[1:] if len(r) > 1 and str(r[0]).strip()}


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
    data = res.get("data", [])
    cutoff = datetime.now(IL_TZ) - timedelta(hours=hours)
    out = []
    for m in data:
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
    # אין pagination כאן (limit=50 בלבד). אם הדף התמלא וגם הפריט האחרון בו
    # (הישן ביותר שהוחזר) עדיין חדש יותר מה-cutoff, החלון המבוקש לא כוסה
    # במלואו - יש עוד היסטוריה מעבר לדף הזה שלא נסרקה.
    if len(data) >= 50 and data:
        try:
            oldest_in_page = _parse_ts(data[-1].get("timestamp"))
        except (ValueError, TypeError):
            oldest_in_page = None
        if oldest_in_page is not None and oldest_in_page > cutoff:
            print(f"⚠️ אינסטגרם: הדף הכיל {len(data)} פריטים וכולם חדשים "
                  f"מהחיתוך - החלון של {hours} שעות לא כוסה במלואו (תוצאה חלקית)")
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
    aweme_list = (res.get("data") or {}).get("aweme_list") or []
    cutoff = datetime.now(IL_TZ) - timedelta(hours=hours)
    out = []
    for v in aweme_list:
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
    # כמו באינסטגרם: אין pagination (count=30 בלבד). דף מלא שכולו עדיין
    # חדש מה-cutoff אומר שיש עוד היסטוריה מעבר אליו שלא נסרקה.
    if len(aweme_list) >= 30 and aweme_list:
        last_ts = aweme_list[-1].get("create_time")
        oldest_in_page = (datetime.fromtimestamp(int(last_ts), tz=pytz.utc)
                          .astimezone(IL_TZ)) if last_ts else None
        if oldest_in_page is not None and oldest_in_page > cutoff:
            print(f"⚠️ טיקטוק: הדף הכיל {len(aweme_list)} פריטים וכולם חדשים "
                  f"מהחיתוך - החלון של {hours} שעות לא כוסה במלואו (תוצאה חלקית)")
    return out


# דפדפן-ish; ה-CDN של טיקטוק מחזיר 403 ל-User-Agent של requests
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

CREDIT_RE = re.compile(r"📸|סעיף 27א|צילום:|קרדיט|Reuters|AP |AFP|Getty")


def resolve_media_url(item):
    """URL טרי, ברגע ההורדה. **לעולם לא נשמר** - הוא חתום לזמן קצר ומת אחריו.

    זו הסיבה שגישת "להוסיף עמודת media_url לקולקטורים" נדחתה: היא הייתה
    מוסיפה סיכון סכמה בייצור (verify_collector.py קיים כי עמודה שנדחפת באמצע
    מזיזה כל ערך אחריה) כדי לשמור ערך שפג.

    **הסתייגות לגבי טיקטוק:** "ברגע ההורדה" נכון לאינסטגרם בלבד. אצל טיקטוק
    ה-URL כבר נלכד ב-discover_tiktok (`_tiktok_urls`) ומוחזר כאן מהזיכרון -
    הוא עבר דרך הסינון, המיון וההעלאה לפני שהגיע לפה. האילוץ הגלובלי
    ("media URL לעולם לא נשמר") עדיין מתקיים כי שום דבר לא כותב אותו לדיסק
    או לגיליון, אבל זה לא אותו "טרי ברגע ההורדה" שמתואר למעלה.
    """
    if item["platform"] == "tiktok":
        urls = item.get("_tiktok_urls") or []
        if not urls:
            raise RuntimeError("אין play_addr באובייקט ה-aweme")
        return urls[0]
    res = http_get_json(f"{BASE}/{item['id']}", params={
        "access_token": ACCESS_TOKEN, "fields": "media_url"})
    url = res.get("media_url")
    if not url:
        raise RuntimeError(f"Graph לא החזיר media_url ל-{item['id']}")
    return url


_URL_RE = re.compile(r"https?://\S+")


def _safe_exc_str(e):
    """מחרוזת שגיאה בלי URL בתוכה, מוכנה להדפסה ליומן.

    requests.exceptions.HTTPError (ש-raise_for_status מעלה) שם בהודעת השגיאה
    את ה-URL המלא שביקשנו - וה-URL הזה הוא בדיוק ה-media_url/play_addr
    שנפתר רגע קודם ושאסור שייחשף: הוא חתום לחלון זמן קצר, וכל שאר העיצוב פה
    (media_url שלא נשמר בשום עמודה) נועד להגן עליו. היומן של הריפו הזה
    **ציבורי** (GitHub Actions logs), אז זו לא רק תוספת סגנון - בלי הניקוי
    כאן, כל 403 מדליף כתובת חתומה החוצה.
    """
    return _URL_RE.sub("<url>", str(e))


def http_download(url, dest, headers=None, timeout=120):
    """זרימה לקובץ. מוחזר מספר הבייטים שנכתבו."""
    import requests
    with requests.get(url, headers=headers, timeout=timeout, stream=True) as r:
        r.raise_for_status()
        n = 0
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)
                    n += len(chunk)
    return n


def download_media(item, dest):
    """הנתיב היחיד שמותר לו לפתור URL. 403 מנוסה שוב עם כותרות דפדפן."""
    url = resolve_media_url(item)
    try:
        return http_download(url, dest)
    except Exception as first:
        print(f"   ↻ הורדה ראשונה נכשלה ({_safe_exc_str(first)[:80]}) - מנסה עם UA דפדפן")
        return http_download(url, dest, headers={
            "User-Agent": BROWSER_UA, "Referer": "https://www.tiktok.com/"})


def drive_filename(item):
    """תאריך, שעה, פלטפורמה ומזהה - כדי שקובץ יהיה מזוהה גם מחוץ לתיקייה שלו."""
    return (f"{item['posted'].strftime('%Y-%m-%d_%H%M')}_"
            f"{item['platform']}_{item['id']}.mp4")


def probe_media(path):
    """רוחב, גובה, ביטרייט ומשך מהקובץ עצמו. None אם אין ffprobe או שהוא נכשל.

    מדידה ולא ניחוש: שמות השדות של מטא (`format` עם 1084x1916) תיארו את
    ההעלאה, לא את הקובץ שהגיע - רק הפיקסלים סגרו את זה. כשל כאן הוא פריט
    בלי מדידה, לא פריט שלא נשמר.
    """
    if not shutil.which("ffprobe"):
        return None
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height",
             "-show_entries", "format=duration,bit_rate",
             "-of", "json", path],
            capture_output=True, text=True, timeout=60)
        d = json.loads(out.stdout or "{}")
        st = (d.get("streams") or [{}])[0]
        fm = d.get("format") or {}
        w, h = st.get("width"), st.get("height")
        if not w or not h:
            return None
        return {"width": int(w), "height": int(h),
                "kbps": int(fm.get("bit_rate") or 0) // 1000,
                "duration_sec": round(float(fm.get("duration") or 0))}
    except Exception:
        return None


def _pixels(row):
    try:
        return int(row.get("width") or 0) * int(row.get("height") or 0)
    except (TypeError, ValueError):
        return 0


def pick_preferred(a, b):
    """מי משני העותקים עדיף - לפי פיקסלים, ואז ביטרייט. None כשאין מדידה.

    הכלל הישן אמר "אינסטגרם, כי טיקטוק מקודדת מחדש". המדידה (2026-09-06)
    אמרה הפוך בשניים משלושה זוגות, ולכן אין כאן שום פלטפורמה - רק מספרים.
    שניים לא מדודים = אין העדפה; ניחוש היה מסמן לצרכן את הקובץ הגרוע.
    """
    pa, pb = _pixels(a), _pixels(b)
    if pa == pb == 0:
        return None
    if pa != pb:
        return a if pa > pb else b
    try:
        ka, kb = int(a.get("kbps") or 0), int(b.get("kbps") or 0)
    except (TypeError, ValueError):
        return None
    return a if ka >= kb else b


def build_row(item, upload, drive_path, topic, probe=None):
    """שורת אינדקס אחת. הסיווג הדטרמיניסטי ומה ש-Gemini החזיר, בשורה אחת."""
    tags = tag_item(item.get("caption"), item["platform"])
    topic = topic or {}
    probe = probe or {}
    duration = item.get("duration_sec", "") or probe.get("duration_sec", "")
    return [
        str(item["id"]), item["platform"],
        item["posted"].strftime("%Y-%m-%d %H:%M"),
        item.get("permalink", ""),
        # באורך מלא - הקולקטורים קוטעים ב-500 ואיתם נעלמים קרדיטי סוף-כיתוב.
        # זהו strip_bidi(caption) - הכיתוב הגולמי לא נשמר בשום מקום אחר.
        # person/program כבר חושבו למעלה משורה זו: אסור להזין את הכיתוב
        # השמור כאן בחזרה ל-extract_handles כדי "לתייג מחדש" - ניקוי ה-bidi
        # שכבר בוצע פה בדיוק ישחזר את השחתת הידיות ש-A1 קיים כדי למנוע.
        strip_bidi(item.get("caption") or ""),
        upload["id"], drive_path, upload["bytes"], duration,
        tags["person"], tags["program"], tags["program_source"],
        topic.get("category", ""), ", ".join(topic.get("tags") or []),
        topic.get("summary", ""),
        "כן" if CREDIT_RE.search(str(item.get("caption") or "")) else "",
        "", datetime.now(IL_TZ).strftime("%Y-%m-%d %H:%M"), ARCHIVER_VERSION,
        "",   # deleted_at - נכתב רק ע"י prune_old
        probe.get("width", ""), probe.get("height", ""), probe.get("kbps", ""),
        "",   # preferred - נכתב רק ע"י run_reconcile
    ]


# רשימת פתיחה, לכוונון אחרי שיהיה פלט אמיתי.
TOPIC_CATEGORIES = [
    "חדשות שולחן", "חוץ", "צבא וביטחון", "משפט ופלילים", "כלכלה",
    "טכנולוגיה", "בריאות", "אוכל וצרכנות", "תרבות ובידור", "מגזין אנושי",
    "סאטירה",
]

TOPIC_SCHEMA = {
    "type": "object",
    "properties": {
        "category": {"type": "string", "enum": TOPIC_CATEGORIES},
        "tags": {"type": "array", "items": {"type": "string"},
                 "description": "אירוע או סיפור ספציפי, למשל \"בחירות 2026\""},
        "summary": {"type": "string", "description": "שורה אחת לאינדקס"},
    },
    "required": ["category", "tags", "summary"],
}

# נמדד 2026-09-06 (gemini_model_probe.py, ריצה על הפרומפט הזה עצמו): טוקני
# חשיבה מחויבים כפלט, ו-gemini-3.5-flash חשב 595 טוקנים כדי להחזיר 73 -
# החשיבה, לא התשובה, היא רוב החשבון. שני אלה נבחרו כי הם היחידים שגם מוגשים
# למפתח שלנו וגם מקבלים thinking_budget=0: 3.6-flash דוחה אותו ב-400, וכך גם
# gemini-2.5-pro שהיה כאן כנפילה אחורה - שחשב 855 טוקנים לאותה שאלה. נפילה
# שקטה למודל יקר פי ארבעה שאי אפשר להשתיק אינה נפילה בטוחה, ולכן היא הוסרה:
# הכשל הנכון כאן הוא פריט בלי סיווג, לא חשבון מפתיע.
GEMINI_MODELS = ["gemini-3.8-flash", "gemini-3.7-flash"]


def classify_topic(client, item, program):
    """קטגוריה אחת + תגיות חופשיות + סיכום. None בכשל - פריט אחד, לא הריצה.

    התגיות החופשיות הן מה שהופך את "כל מה שעלה היום על הבחירות" לשאלה שאפשר
    לענות עליה בלי שאיש חזה את הנושא מראש. הן נשמרות כפי שנכתבו - נרמול שלהן
    לאוצר מילים מבוקר הוא בכוונה מחוץ לתחום: זה נראה מסודר ומשמיד בשקט את
    הסיגנל שבגללו הן שוות משהו.
    """
    caption = strip_bidi(item.get("caption") or "")[:1500]
    program_line = ("התוכנית שזוהתה: " + program) if program else "לא זוהתה תוכנית."
    prompt = f"""אתה עורך ארכיון של חדר חדשות (כאן חדשות). לפניך כיתוב של סרטון
שפורסם ב{item['platform']}.

{program_line}

הכיתוב:
{caption}

החזר קטגוריה אחת מתוך: {" · ".join(TOPIC_CATEGORIES)}
ותגיות חופשיות שמזהות את **האירוע או הסיפור הספציפי** (למשל "בחירות 2026",
"חטיפת יהלי") - לא מילות מפתח כלליות. אם הכיתוב לא מספיק כדי לזהות סיפור,
החזר תגיות ריקות ואל תמציא.
summary: שורה אחת בעברית שמתארת מה רואים בסרטון."""

    for model_name in GEMINI_MODELS:
        try:
            res = client.models.generate_content(
                model=model_name, contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=TOPIC_SCHEMA,
                    # ראו GEMINI_MODELS: בלי זה רוב העלות היא חשיבה שאיש
                    # לא קורא. הסיווג הזה הוא בחירה מרשימה סגורה על כיתוב
                    # של 150 תווים - אין שם מה להסיק.
                    thinking_config=types.ThinkingConfig(thinking_budget=0),
                ),
            )
            return json.loads(res.text)
        except Exception as e:
            print(f"   ⚠️ {model_name} נכשל: {str(e)[:120]}")
    return None


def archive_item(item, drive, ws, client):
    """פריט אחד, מקצה לקצה. מחזיר את השורה שנכתבה, או None אם דולג.

    **שורת האינדקס נכתבת אחרונה**, אחרי שהקובץ בדרייב. קריסה בין השתיים עולה
    בקובץ כפול בריצה הבאה, וזה מתאושש. ההפך - אינדקס שרשום וקובץ שאינו - הופך
    את הארכיון לשקרן, ואת זה אי אפשר לתקן בלי ביקורת ידנית.
    """
    tmpdir = tempfile.mkdtemp(prefix="kanarch_")
    try:
        name = drive_filename(item)
        local = os.path.join(tmpdir, name)
        download_media(item, local)
        probe = probe_media(local)

        date_path = item["posted"].strftime("%Y/%m/%d")
        upload = drive.upload(local, name, drive.ensure_folder(date_path))

        tags = tag_item(item.get("caption"), item["platform"])
        topic = classify_topic(client, item, tags["program"])

        folders = []
        if tags["program"]:
            folders.append(f"לפי תוכנית/{tags['program']}")
        if topic and topic.get("category"):
            folders.append(f"לפי קטגוריה/{topic['category']}")
        for folder in folders:
            try:
                drive.shortcut(upload["id"], name, drive.ensure_folder(folder))
            except Exception as e:   # קיצור שנכשל לא שווה איבוד הפריט
                print(f"   ⚠️ קיצור ל-{folder} נכשל: {str(e)[:100]}")

        row = build_row(item, upload, date_path, topic, probe)
        ws.append_row(row, value_input_option="RAW")
        return row
    except Exception as e:
        print(f"   ❌ {item['platform']}/{item['id']} דולג: {_safe_exc_str(e)[:160]}")
        return None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def run_archive(sh, drive, client, hours=ARCHIVE_LOOKBACK_HOURS):
    ws, known = get_index(sh)
    print(f"📋 {len(known)} פריטים כבר בארכיון")

    found = discover_instagram(hours)
    try:
        found += discover_tiktok(hours)
    except Exception as e:   # ספק לא-רשמי, best-effort - לא מפיל את אינסטגרם
        print(f"⚠️ משיכת טיקטוק נכשלה (מדלג): {_safe_exc_str(e)[:120]}")
    print(f"🔎 {len(found)} סרטונים בחלון של {hours} שעות")

    fresh = filter_new(found, known)
    if not fresh:
        print("ℹ️ אין חדש - הכל כבר בארכיון.")
        return 0, 0

    archived = failed = 0
    total_bytes = 0
    for item in sorted(fresh, key=lambda i: i["posted"]):
        print(f"\n--- {item['platform']} · {item['id']} · "
              f"{item['posted'].strftime('%d/%m %H:%M')} ---")
        row = archive_item(item, drive, ws, client)
        if row:
            archived += 1
            total_bytes += int(row[INDEX_HEADER.index("bytes")] or 0)
        else:
            failed += 1
    print(f"\n✅ {archived} נשמרו ({total_bytes / 1e6:,.0f}MB), {failed} דולגו")
    return archived, failed


# מילות עצירה - זהות לאלה של עמוד הוויראליות (aggregate.py:1001)
_STOP = set("""של את על עם לא זה זו זאת הוא היא הם הן אני אתם אנחנו יש אין
גם רק כל כי מה מי איך למה בין אחרי לפני נגד מול אבל או עוד כבר היום אמש מחר
כאן חדשות בעקבות במהלך בזמן כדי לפי אצל בגלל האם כמה שני שתי כמו יותר פחות
אשר כאשר היה היו תהיה הזה הזאת האלה עצמו שלו שלה שלהם ידי לאחר עקב""".split())

RECONCILE_CONTAINMENT = 0.5   # ראו הערת find_pairs
RECONCILE_WINDOW_DAYS = 2
RECONCILE_MIN_TOKENS = 4


def caption_tokens(text, limit=40):
    text = re.sub(r"[^0-9א-תa-zA-Z\s]", " ", strip_bidi(text))
    out = []
    for w in text.split():
        if len(w) >= 3 and w not in _STOP and not w.isdigit():
            out.append(w)
            if len(out) >= limit:
                break
    return frozenset(out)


def containment(a, b):
    """חפיפה ביחס לקצר מבין השניים."""
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


def find_pairs(rows):
    """זוגות אינסטגרם-טיקטוק שהם אותו פריט. אף פעם לא מוחק, רק מקשר.

    הסף כאן 0.5 ולא 0.6 של עמוד הוויראליות, ומותר לו: שם רץ union-find על כל
    הפוסטים בשבוע, וסף נמוך יצר אשכול-ענק של 54 פוסטים בשרשור טרנזיטיבי; כאן
    זה זיווג 1:1 בין שתי פלטפורמות בלבד, בלי מעבר בין זוגות, אז אין מה לשרשר.

    **כשל ידוע, וזה הכיוון הבטוח:** כיתובי טיקטוק הם טיזרים וכיתובי אינסטגרם
    הם ידיעה מלאה, אז פריטים זהים באמת אינם מזווגים - "תיעוד קשה מהשומרון"
    עם זנב הקריאה-לפעולה שלו קיבל 0.33 מול פוסט האינסטגרם של עצמו (נמדד).
    שימו לב שהמנגנון תלוי באורך הטיזר ולא ברור מאליו: טיזר של שלוש מילים
    בלבד מגיע דווקא ל-1.00, כי כל הטוקנים שלו מוכלים בידיעה המלאה, ומה שמונע
    ממנו לזווג הוא RECONCILE_MIN_TOKENS. שני המסלולים נבדקים בנפרד.

    הכיוון הזה של הטעות הוא הבטוח: זוג שלא זווג משאיר שני קבצים בארכיון,
    בעוד זיווג שגוי היה מסתיר תוכן אמיתי מאחורי סימון כפילות.
    """
    prepped = []
    for i, r in enumerate(rows):
        toks = caption_tokens(r.get("caption", ""))
        if len(toks) < RECONCILE_MIN_TOKENS:
            continue
        try:
            d = datetime.strptime(str(r.get("posted_at", ""))[:10], "%Y-%m-%d")
        except ValueError:
            continue
        prepped.append((i, r.get("platform", ""), d, toks))

    # כל המועמדים תחילה, ואז שיבוץ מהחזק לחלש. הגרסה הקודמת שיבצה לפי סדר
    # הסריקה, כך שפריט שנסרק מוקדם יכול היה לחטוף בן זוג בציון בינוני
    # ולהשאיר התאמה חזקה יותר יתומה. על הנתונים של 2026-09-06 שתי השיטות
    # נתנו 40 זיווגים זהים - זה תיקון של תלות בסדר, לא של פער נצפה.
    cands = []
    for a in range(len(prepped)):
        ia, pa, da, ta = prepped[a]
        for b in range(a + 1, len(prepped)):
            ib, pb, db, tb = prepped[b]
            if pb == pa or abs((da - db).days) > RECONCILE_WINDOW_DAYS:
                continue
            s = containment(ta, tb)
            if s >= RECONCILE_CONTAINMENT:
                cands.append((s, ia, ib))
    # שובר שוויון יציב במזהי השורות, כדי ששני ציונים זהים לא יוכרעו בסדר
    cands.sort(key=lambda c: (-c[0], c[1], c[2]))

    used, pairs = set(), []
    for _, ia, ib in cands:
        if ia in used or ib in used:
            continue
        used.update({ia, ib})
        pairs.append((ia, ib))
    return pairs


def write_cells(ws, cells):
    """כל התאים בקריאה אחת. cells: [(שורה, עמודה, ערך)].

    היה כאן update_cell לכל תא - עד ~76 קריאות מול מכסה של 60 לדקה, בלי טיפול
    בשגיאות, וזה הפך את ההצלבה למסלול היחיד שיכול להפיל **ריצה** ולא פריט.
    קריאה אחת גם הופכת את הכתיבה לאטומית מבחינתנו: או שכל הסימונים נכתבו או
    שאף אחד, במקום מצב ביניים שבו חצי מהזוגות מקושרים.
    """
    if not cells:
        return 0
    ws.batch_update(
        [{"range": gspread.utils.rowcol_to_a1(r, c), "values": [[v]]}
         for r, c, v in cells],
        value_input_option="RAW")
    return len(cells)


def audit_archive(drive, ws):
    """מצליב את האינדקס מול הדרייב. קורא בלבד - לא מוחק ולא מתקן.

    שני ממצאים, ושניהם נובעים מכלל הסדר: השורה נכתבת אחרי הקובץ.
      * **חסר** - שורה חיה שהקובץ שלה איננו. הארכיון משקר על עצמו, וזה
        המצב היחיד כאן שאי אפשר לתקן בלי לרדת לפרטים.
      * **יתום** - קובץ בלי שורה. ריצה שמתה בין ההעלאה לכתיבת השורה, בדיוק
        המחיר שכלל הסדר בוחר לשלם. הפריט יורד שוב בריצה הבאה, והעותק הישן
        נשאר תופס מקום.
    שורה עם deleted_at אינה ממצא: היעדר הקובץ שלה הוא מה שאמור לקרות.
    """
    values = ws.get_all_values()
    header = values[0] if values else list(INDEX_HEADER)
    i_file = header.index("drive_file_id")
    i_del = header.index("deleted_at")
    live = set()
    for raw in values[1:]:
        r = list(raw) + [""] * (len(header) - len(raw))
        if r[i_file].strip() and not r[i_del].strip():
            live.add(r[i_file].strip())

    on_drive = {f["id"] for f in drive.list_files(mime="video/mp4")}
    missing = sorted(live - on_drive)
    orphans = sorted(on_drive - live)
    print(f"🔎 שלמות: {len(live)} שורות חיות, {len(on_drive)} קבצים בדרייב")
    if missing:
        print(f"   ❌ {len(missing)} שורות שהקובץ שלהן נעלם: "
              f"{', '.join(missing[:10])}")
    if orphans:
        print(f"   ⚠️ {len(orphans)} קבצים בלי שורה: "
              f"{', '.join(orphans[:10])}")
    if not missing and not orphans:
        print("   ✅ האינדקס והדרייב מסכימים")
    return {"missing": missing, "orphans": orphans}


def check_drive(drive):
    """קריאת דרייב אמיתית אחת: פותרת את תיקיית השורש. None אם האישורים מתים.

    קיים כי החלפת סוד אינה הוכחה שהוא עובד, וריצה רגילה נוגעת בדרייב רק כשיש
    פריט חדש - כלומר יכולה לעבור בירוק בלי לאמת את הטוקן כלל. הכשל שזה נועד
    לתפוס הוא שקט לפי טבעו: refresh token שפג מפיל את הריצה הראשונה שבה
    במקרה יש מה לשמור, ולא לפני.
    """
    try:
        root = drive._root()
        print(f"✅ הדרייב נגיש. תיקיית השורש: {root}")
        return root
    except Exception as e:
        print(f"❌ הדרייב לא נגיש: {_safe_exc_str(e)[:160]}")
        return None


def prune_old(drive, ws, days=RETENTION_DAYS, dry_run=False):
    """מוחק את הקבצים שעברו את חלון השמירה ומסמן את שורותיהם. מחזיר בייטים.

    **סדר הפעולות הפוך לזה של הארכוב, ומאותו נימוק.** בארכוב שורת האינדקס
    נכתבת אחרונה, כי אינדקס שמצביע על קובץ שאינו קיים משקר על עצמו. כאן
    המחיקה קודמת לסימון: אילו הסימון היה ראשון וההסרה נכשלת, השורה כבר
    טוענת "נמחק" ואף ריצה לא תחזור אליה - הקובץ היה דולף לתמיד. בסדר הזה
    כשל משאיר שורה לא מסומנת, שהריצה הבאה מנסה שוב, ומחיקה חוזרת של קובץ
    שכבר איננו מוחזרת כ-404 ונחשבת הצלחה.
    """
    values = ws.get_all_values()
    if len(values) < 2:
        return 0
    header, body = values[0], values[1:]
    i_posted = header.index("posted_at")
    i_file = header.index("drive_file_id")
    i_bytes = header.index("bytes")
    i_del = header.index("deleted_at")
    cutoff = (datetime.now(IL_TZ) - timedelta(days=days)).strftime("%Y-%m-%d")
    now = datetime.now(IL_TZ).strftime("%Y-%m-%d %H:%M")

    freed, removed, failed, cells = 0, 0, 0, []
    for n, raw in enumerate(body, start=2):
        row = list(raw) + [""] * (len(header) - len(raw))
        if row[i_del].strip() or not row[i_file].strip():
            continue
        if str(row[i_posted])[:10] >= cutoff:
            continue
        if dry_run:
            freed += int(row[i_bytes] or 0)
            removed += 1
            continue
        try:
            drive.delete(row[i_file])
            drive.delete_shortcuts(row[i_file])
        except Exception as e:
            print(f"   ⚠️ {row[i_file]}: {_safe_exc_str(e)[:80]}")
            failed += 1
            continue
        cells.append((n, i_del + 1, now))
        freed += int(row[i_bytes] or 0)
        removed += 1

    # הסימונים נאספים ונכתבים בבת אחת, וזה בטוח כאן בזכות האידמפוטנטיות:
    # אם הכתיבה נופלת אחרי שהקבצים כבר נמחקו, השורות נשארות לא מסומנות,
    # הריצה הבאה מנסה למחוק שוב, מקבלת 404 - שנחשב הצלחה - ומסמנת.
    write_cells(ws, cells)

    label = "יימחקו" if dry_run else "נמחקו"
    print(f"🧹 {removed} {label} (מעל {days} ימים), "
          f"{freed / 1e6:.0f}MB, {failed} נכשלו")
    return freed


def run_reconcile(sh, days=7):
    """מקשר עותקים ומדפיס את דוח הפערים - מה קיים בפלטפורמה אחת ולא בשנייה."""
    ws, _ = get_index(sh)
    values = ws.get_all_values()
    if len(values) < 2:
        print("ℹ️ האינדקס ריק.")
        return 0
    header, body = values[0], values[1:]
    cutoff = (datetime.now(IL_TZ) - timedelta(days=days)).strftime("%Y-%m-%d")
    rows, row_numbers = [], []
    for n, raw in enumerate(body, start=2):
        r = dict(zip(header, list(raw) + [""] * (len(header) - len(raw))))
        if str(r.get("posted_at", ""))[:10] >= cutoff:
            rows.append(r)
            row_numbers.append(n)

    same_col = header.index("same_as") + 1
    pref_col = header.index("preferred") + 1
    pairs = find_pairs(rows)
    cells, updates, preferred = [], 0, 0
    for i, j in pairs:
        for src, dst in ((i, j), (j, i)):
            if not rows[src].get("same_as"):
                cells.append((row_numbers[src], same_col,
                              rows[dst].get("drive_file_id", "")))
                updates += 1
        win = pick_preferred(rows[i], rows[j])
        if win is not None and not win.get("preferred"):
            cells.append((row_numbers[i if win is rows[i] else j],
                          pref_col, "1"))
            preferred += 1
    write_cells(ws, cells)

    linked = {i for p in pairs for i in p}
    only = {"instagram": [], "tiktok": []}
    for idx, r in enumerate(rows):
        if idx not in linked and r.get("platform") in only:
            only[r["platform"]].append(r)
    print(f"🔗 {len(pairs)} זוגות קושרו ({updates} תאים עודכנו, "
          f"{preferred} עותקים מועדפים סומנו לפי מדידה)")
    print(f"📊 דוח פערים ל-{days} הימים האחרונים: "
          f"{len(only['tiktok'])} רק בטיקטוק, "
          f"{len(only['instagram'])} רק באינסטגרם")
    for plat, gap_items in only.items():
        for r in gap_items[:15]:
            print(f"   [{plat}] {r.get('posted_at', '')} "
                  f"{r.get('caption', '')[:70]}")
    rep = storage_report(rows)
    print(f"💾 {rep['total_bytes'] / 1e9:.2f}GB על פני {rep['days']} ימים = "
          f"{rep['per_day_mb']:,.0f}MB ליום → "
          f"{rep['projected_gb_month']:.1f}GB לחודש")
    return len(pairs)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="ארכיון וידאו - אינסטגרם וטיקטוק")
    p.add_argument("--since-days", type=int, default=None,
                   help=f"לחזור כמה ימים אחורה במקום {ARCHIVE_LOOKBACK_HOURS} שעות")
    p.add_argument("--reconcile", action="store_true",
                   help="מעבר ההצלבה הלילי במקום ארכוב")
    p.add_argument("--dry-run", action="store_true",
                   help="לגלות ולסנן בלבד - בלי הורדה, העלאה או כתיבה")
    p.add_argument("--audit", action="store_true",
                   help="הצלבת האינדקס מול הדרייב, קריאה בלבד")
    p.add_argument("--check", action="store_true",
                   help="קריאת דרייב אחת לאימות האישורים, ויציאה")
    p.add_argument("--prune", action="store_true",
                   help=f"למחוק קבצים בני יותר מ-{RETENTION_DAYS} ימים")
    p.add_argument("--retention-days", type=int, default=RETENTION_DAYS,
                   help="חלון השמירה בימים")
    args = p.parse_args(argv)
    args.hours = (args.since_days * 24 if args.since_days
                  else ARCHIVE_LOOKBACK_HOURS)
    return args


def storage_report(rows):
    """כמה הארכיון באמת שוקל. ההערכה במפרט הייתה 1GB לחודש והמדידה מהכיתובים
    שעל הדיסק אמרה ~12 - אז הריצה מודדת במקום להעריך, ואחרי שבועיים יש עובדה
    בלוג במקום שתי הערכות."""
    total = 0
    days = set()
    for r in rows:
        try:
            total += int(str(r.get("bytes") or 0).strip() or 0)
        except ValueError:
            pass
        d = str(r.get("posted_at", ""))[:10]
        if d:
            days.add(d)
    n = len(days)
    per_day = (total / n / 1e6) if n else 0
    return {"total_bytes": total, "days": n, "per_day_mb": per_day,
            "projected_gb_month": per_day * 30 / 1000}


def main():
    # שומר יחיד: גם דליפת ה-URL החתום דרך traceback גולמי וגם ריצה מקומית
    # שקורסת בלי exit code הם אותה בעיה - discover_instagram (בניגוד ל-
    # discover_tiktok) לא עטופה ב-run_archive, אז כשל שלה מגיע לכאן חשוף.
    try:
        args = parse_args()
        now = datetime.now(IL_TZ)
        print(f"\n🎬 ארכיון וידאו - {now.strftime('%Y-%m-%d %H:%M')}\n")

        if args.check:
            drive = drive_store.DriveStore.from_env()
            sys.exit(0 if check_drive(drive) else 1)

        sh = open_spreadsheet()

        if args.reconcile:
            run_reconcile(sh)
            return

        if args.audit or args.prune:
            ws, _ = get_index(sh)
            drive = drive_store.DriveStore.from_env()
            if args.audit:
                audit_archive(drive, ws)
            if args.prune:
                prune_old(drive, ws, args.retention_days, args.dry_run)
            return

        if not ACCESS_TOKEN:
            print("❌ חסר FACEBOOK_TOKEN")
            sys.exit(1)

        if args.dry_run:
            known = peek_index(sh)
            found = discover_instagram(args.hours)
            try:
                found += discover_tiktok(args.hours)
            except Exception as e:
                print(f"⚠️ משיכת טיקטוק נכשלה: {_safe_exc_str(e)[:120]}")
            fresh = filter_new(found, known)
            print(f"\n🧪 מצב יבש: {len(found)} בחלון, {len(fresh)} חדשים")
            for i in fresh:
                print(f"   {i['platform']:10s} {i['id']:20s} "
                      f"{i['posted'].strftime('%d/%m %H:%M')}  "
                      f"{strip_bidi(i.get('caption', ''))[:60]}")
            return

        # דרייב שלא נגיש מפיל את הריצה בכוונה - הוורקפלואו צריך להאדים
        drive = drive_store.DriveStore.from_env()
        client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
        archived, failed = run_archive(sh, drive, client, args.hours)
        if archived == 0 and failed > 0:
            sys.exit(1)
    except Exception as e:
        print(f"❌ {_safe_exc_str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
