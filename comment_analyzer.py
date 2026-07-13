"""
Comment Analyzer - ניתוח Gemini של תגובות לפוסטים "מדוברים" באינסטגרם ובפייסבוק.

המספרים אומרים *כמה* פוסט עבד; התגובות מגלות *למה*. הקולקטור בוחר בכל ריצה
את הפוסטים שעוררו הכי הרבה שיחה בכל פלטפורמה, מושך את שרשור התגובות המלא
ומחלץ עם Gemini סנטימנט, נושאים דומיננטיים והשערת "למה זה עבד". כל פוסט
מנותח פעם אחת בלבד והתוצאה נשמרת לגיליון "ניתוח תגובות" (append-only).

כלל הבחירה, לכל פלטפורמה בנפרד (כויל מול נתוני אמת 2026-07-13):
  אינסטגרם: רצפה 300 תגובות (~p80; חציון 95, p90 500) -> ~2 פוסטים ביום.
  פייסבוק:  רצפה 500 תגובות (~p76; חציון 151, p90 1087) -> ~4 פוסטים ביום.
  גיל מינימלי 24 שעות - שרשור מתייצב אחרי יום-יומיים; ניתוח מוקדם תופס
  חצי שיחה. חריג "חם עכשיו": פוסט צעיר מ-24ש נכנס אם עבר פי-2 מהרצפה -
  שיחה בסדר גודל כזה תוך שעות שווה מבט מיידי.
  עד 5 ניתוחים לריצה לפלטפורמה - רזרבה להשלמות אחרי ריצה שנכשלה.

Env: FACEBOOK_TOKEN, GCP_SERVICE_ACCOUNT, GEMINI_API_KEY.
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta

import gspread
import pytz
from google.oauth2.service_account import Credentials
from google import genai
from google.genai import types

from utils import http_get_json

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# --- Config ---
ACCESS_TOKEN = os.environ.get('FACEBOOK_TOKEN')
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
API_VERSION = "v25.0"
BASE = f"https://graph.facebook.com/{API_VERSION}"

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
TARGET_SHEET = "ניתוח תגובות"

IL_TZ = pytz.timezone('Asia/Jerusalem')

PLATFORMS = [
    {
        'key': 'instagram',
        'label': 'אינסטגרם',
        'sheet': 'נתוני אינסטגרם',
        'id_col': 'media_id',
        'caption_col': 'caption',
        'date_col': 'date', 'time_col': 'time',
        'type_col': 'type', 'url_col': 'permalink',
        'floor': 300,
        'text_field': 'text',      # שם שדה הטקסט ב-Graph API (IG: text, FB: message)
    },
    {
        'key': 'facebook',
        'label': 'פייסבוק',
        'sheet': 'נתוני פייסבוק',
        'id_col': 'post_id',
        'caption_col': 'title',
        'date_col': 'date', 'time_col': 'time',
        'type_col': 'type', 'url_col': 'permalink',
        'floor': 500,
        'text_field': 'message',
    },
    {
        # תגובות יוטיוב הן ציבוריות - API key מספיק, בלי OAuth.
        # ערוץ שקט יחסית: p50=37, p90≈190 (כויל 2026-07-13) -> רצפה 200 ≈ סרטון ביום.
        'key': 'youtube',
        'label': 'יוטיוב',
        'sheet': 'נתוני יוטיוב',
        'id_col': 'video_id',
        'caption_col': 'title',
        'date_col': 'published_at', 'time_col': 'published_time',
        'type_col': 'video_type', 'url_col': 'video_url',
        'floor': 200,
        'text_field': None,        # לא Graph API - יש פוצ'ר ייעודי
    },
]

HOT_MULTIPLIER = 2       # פוסט צעיר מ-24ש נכנס אם עבר פי-2 מהרצפה ("חם עכשיו")
# תקרת ניתוחים לריצה לפלטפורמה; ניתן לדריסה ב-env לצורך backfill חד-פעמי
MAX_PER_RUN = int(os.environ.get('MAX_PER_RUN') or 5)
MIN_AGE_HOURS = 24       # שרשור צעיר מזה עוד לא התייצב (אלא אם חם)
WINDOW_DAYS = 7          # לא חוזרים אחורה מעבר לחלון האיסוף של הפוסטים
MAX_COMMENTS_PULLED = 600     # תקרת עמודי Graph API לפוסט
MAX_COMMENTS_TO_GEMINI = 300  # הכי מלויקקות; מעבר לזה רק מנפח את הפרומפט

# flash הוא GA (בניגוד ל-preview שנכבים בלי אזהרה - 3-pro-preview מת במרץ)
# והמשימה לא דורשת הסקה כבדה; 2.5-pro נשאר כפולבק יציב.
GEMINI_MODELS = ["gemini-3.5-flash", "gemini-2.5-pro"]

TARGET_HEADER = ['media_id', 'platform', 'post_date', 'analyzed_at', 'type', 'caption',
                 'comments_in_sheet', 'comments_pulled',
                 'sentiment_positive', 'sentiment_negative', 'sentiment_neutral',
                 'coverage_criticism',
                 'themes', 'top_comments', 'why_it_worked', 'controversy',
                 'summary', 'permalink']


def _to_int(v):
    try:
        return int(float(str(v).replace(',', '').strip() or 0))
    except (ValueError, TypeError):
        return 0


def open_spreadsheet():
    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT') or os.environ.get('GOOGLE_CREDENTIALS')
    creds_dict = json.loads(creds_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)


def get_target_sheet(sh):
    """הגיליון + media_id-ים שכבר נותחו. מיגרציה חד-פעמית: שורות מהגרסה
    הראשונה (לפני עמודת platform) מקבלות platform=instagram."""
    try:
        ws = sh.worksheet(TARGET_SHEET)
    except gspread.WorksheetNotFound:
        return None, set()

    values = ws.get_all_values()
    if values and 'platform' not in values[0]:
        migrated = [TARGET_HEADER]
        for row in values[1:]:
            row = row + [''] * (len(TARGET_HEADER) - 1 - len(row))
            migrated.append([row[0], 'instagram'] + row[1:len(TARGET_HEADER) - 1])
        ws.clear()
        ws.update(migrated)
        print(f"🔄 Migrated {len(migrated) - 1} legacy rows to platform-aware schema")
        values = migrated

    analyzed = {str(r[0]).strip() for r in values[1:] if r and str(r[0]).strip()}
    return ws, analyzed


def pick_candidates(rows, plat, analyzed_ids):
    """המדוברים של השבוע שטרם נותחו: רצפה + גיל 24ש (או "חם" בפי-2) + top-N."""
    now = datetime.now(IL_TZ)
    cutoff_old = now - timedelta(days=WINDOW_DAYS)
    cutoff_young = now - timedelta(hours=MIN_AGE_HOURS)

    candidates = []
    for r in rows:
        item_id = str(r.get(plat['id_col'], '')).strip()
        comments = _to_int(r.get('comments'))
        if not item_id or item_id in analyzed_ids or comments < plat['floor']:
            continue
        try:
            posted = IL_TZ.localize(datetime.strptime(
                f"{str(r.get(plat['date_col'], '')).strip()} "
                f"{str(r.get(plat['time_col'], '')).strip()[:5] or '00:00'}",
                '%Y-%m-%d %H:%M'))
        except ValueError:
            continue
        if posted < cutoff_old:
            continue
        is_young = posted > cutoff_young
        if is_young and comments < plat['floor'] * HOT_MULTIPLIER:
            continue  # צעיר ולא מספיק חם
        candidates.append((comments, item_id, r, is_young))

    candidates.sort(key=lambda c: c[0], reverse=True)
    return candidates[:MAX_PER_RUN]


def fetch_comments(item_id, text_field):
    """שרשור התגובות המלא כולל רמה אחת של replies (מקוצב ב-MAX_COMMENTS_PULLED)."""
    reply_edge = 'replies' if text_field == 'text' else 'comments'
    comments = []
    url = f"{BASE}/{item_id}/comments"
    params = {
        'access_token': ACCESS_TOKEN,
        'fields': f"{text_field},like_count,{reply_edge}{{{text_field},like_count}}",
        'limit': 50,
    }
    while url and len(comments) < MAX_COMMENTS_PULLED:
        res = http_get_json(url, params=params, timeout=20, max_retries=2)
        if 'error' in res:
            print(f"⚠️ Comments error for {item_id}: {res['error'].get('message', '')[:120]}")
            break
        for c in res.get('data', []):
            comments.append({
                'text': c.get(text_field, '') or '',
                'like_count': c.get('like_count', 0) or 0,
                'replies': [{'text': r.get(text_field, '') or '',
                             'like_count': r.get('like_count', 0) or 0}
                            for r in c.get(reply_edge, {}).get('data', [])],
            })
        url = res.get('paging', {}).get('next')
        params = None  # ה-URL הבא כבר נושא את הפרמטרים
    return comments


def fetch_youtube_comments(video_id):
    """שרשור תגובות יוטיוב (Data API v3, ציבורי - API key בלבד, בלי OAuth)."""
    comments = []
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        'key': YOUTUBE_API_KEY, 'part': 'snippet,replies', 'videoId': video_id,
        'maxResults': 100, 'order': 'relevance', 'textFormat': 'plainText',
    }
    while len(comments) < MAX_COMMENTS_PULLED:
        res = http_get_json(url, params=params, timeout=20, max_retries=2)
        if 'error' in res:
            # למשל commentsDisabled - לא כשל שלנו
            print(f"⚠️ YT comments error for {video_id}: {str(res['error'])[:120]}")
            break
        for t in res.get('items', []):
            top = t.get('snippet', {}).get('topLevelComment', {}).get('snippet', {})
            comments.append({
                'text': top.get('textOriginal', '') or '',
                'like_count': top.get('likeCount', 0) or 0,
                'replies': [{'text': r.get('snippet', {}).get('textOriginal', '') or '',
                             'like_count': r.get('snippet', {}).get('likeCount', 0) or 0}
                            for r in t.get('replies', {}).get('comments', [])],
            })
        token = res.get('nextPageToken')
        if not token:
            break
        params['pageToken'] = token
    return comments


# שני מדדים נפרדים, כל אחד עם ציר מוגדר (גלגול שלישי של ההגדרה - ההיסטוריה
# חשובה): ציר לא מוגדר נתן למודל לבחור לבד (כתבת "המעיין" יצאה 80% חיובי
# כשהקהל זעם על הסיקור); ציר "יחס לסיקור" היה מוגדר אבל דל - רוב המגיבים
# בכלל לא מתייחסים לסיקור, אז הכול קרס ל"ניטרלי" (קסטרו: 85%). לכן:
#   sentiment_*        = הטמפרטורה הרגשית של השיחה, לא משנה כלפי מי
#   coverage_criticism = הסיגנל הייחודי לכאן, כמספר נפרד (יכול לחפוף לשלילי)
ANALYSIS_SCHEMA = {
    "type": "object",
    "properties": {
        "sentiment_positive": {"type": "integer",
                               "description": "אחוז התגובות שמביעות רגש חיובי: תמיכה, הזדהות, התרגשות, שמחה, גאווה - כלפי כל גורם (0-100)"},
        "sentiment_negative": {"type": "integer",
                               "description": "אחוז התגובות שמביעות רגש שלילי: זעם, לעג, תסכול, עצב, עוינות - כלפי כל גורם (0-100)"},
        "sentiment_neutral": {"type": "integer",
                              "description": "אחוז התגובות הענייניות בלבד, בלי מטען רגשי (0-100)"},
        "coverage_criticism": {"type": "integer",
                               "description": "בנפרד מהסנטימנט: אחוז התגובות שמבקרות את הסיקור, הכתבה או הערוץ עצמם (0-100; יכול לחפוף לשלילי)"},
        "themes": {"type": "array", "items": {"type": "string"},
                   "description": "2-4 נושאים דומיננטיים בשיחה"},
        "top_comments": {"type": "array", "items": {"type": "string"},
                         "description": "3 התגובות המייצגות/בולטות (ציטוט מקוצר)"},
        "why_it_worked": {"type": "string",
                          "description": "השערה: מה באמת הניע את השיחה (2-3 משפטים)"},
        "controversy": {"type": "boolean", "description": "האם השיחה טעונה/ויכוח"},
        "summary": {"type": "string", "description": "שורה אחת לדשבורד"},
    },
    "required": ["sentiment_positive", "sentiment_negative", "sentiment_neutral",
                 "coverage_criticism", "themes", "top_comments", "why_it_worked",
                 "controversy", "summary"],
}


def analyze_with_gemini(client, plat, post, comments):
    """מחלץ מהשרשור סנטימנט, נושאים והשערת 'למה זה עבד' - JSON מובנה."""
    ranked = sorted(comments, key=lambda c: c['like_count'], reverse=True)
    lines = []
    for c in ranked[:MAX_COMMENTS_TO_GEMINI]:
        lines.append(f"[{c['like_count']}] {c['text'][:200]}")
        for r in c['replies'][:3]:
            lines.append(f"    ↳ [{r['like_count']}] {r['text'][:150]}")

    prompt = f"""אתה אנליסט סושיאל של חדר חדשות (כאן חדשות). לפניך פוסט {plat['label']} והתגובות אליו.
המספר בסוגריים בתחילת כל תגובה הוא כמות הלייקים שלה - תגובה מלויקקת משקפת סנטימנט רחב.

הפוסט ({post.get('type', '')}):
{str(post.get(plat['caption_col'], ''))[:500]}

מדדים: {_to_int(post.get('views'))} צפיות, {_to_int(post.get('comments'))} תגובות, {_to_int(post.get('shares'))} שיתופים.

התגובות ({len(comments)} נמשכו, מוצגות לפי לייקים):
{chr(10).join(lines)}

נתח בעברית. שני מדדים נפרדים, אל תערבב ביניהם:
1. סנטימנט = הטמפרטורה הרגשית של התגובות, לא משנה כלפי מי: חיובי (תמיכה,
   הזדהות, התרגשות, שמחה), שלילי (זעם, לעג, תסכול, עצב), ניטרלי (ענייני
   בלבד - אמור להיות מיעוט קטן ברוב השיחות).
2. coverage_criticism = כמה מהתגובות תוקפות את הסיקור, הכתבה או הערוץ עצמם
   (יכול לחפוף לשלילי; 0 אם השיחה בכלל לא עוסקת בסיקור).
ב-why_it_worked אל תסתפק במה שהפוסט אומר - הסבר מה בתגובות מגלה מדוע
הוא עורר שיחה (עצבים חשופים, ויכוח, הזדהות, סרקזם, טרנד). היה ספציפי וציטוטי."""

    last_err = None
    for model_name in GEMINI_MODELS:
        try:
            res = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=ANALYSIS_SCHEMA,
                ),
            )
            return json.loads(res.text)
        except Exception as e:
            last_err = e
            print(f"⚠️ Model {model_name} failed: {str(e)[:150]}")
    raise RuntimeError(f"all Gemini models failed: {last_err}")


def run_platform(sh, client, plat, analyzed_ids):
    """מריץ פלטפורמה אחת; מחזיר (שורות חדשות, מספר כשלונות)."""
    if plat['key'] == 'youtube' and not YOUTUBE_API_KEY:
        print("⚠️ youtube: YOUTUBE_API_KEY missing - skipping platform")
        return [], 0
    rows = sh.worksheet(plat['sheet']).get_all_records()
    candidates = pick_candidates(rows, plat, analyzed_ids)
    if not candidates:
        print(f"ℹ️ {plat['key']}: no posts above {plat['floor']} comments awaiting analysis.")
        return [], 0
    print(f"🎯 {plat['key']}: {len(candidates)} candidates (floor={plat['floor']}):")
    for comments, item_id, _, is_young in candidates:
        print(f"   {item_id}  ({comments} comments{' · 🔥 hot <24h' if is_young else ''})")

    new_rows, failures = [], 0
    for sheet_comments, item_id, post, _ in candidates:
        print(f"\n--- {plat['key']} · {item_id} ---")
        try:
            if plat['key'] == 'youtube':
                comments = fetch_youtube_comments(item_id)
            else:
                comments = fetch_comments(item_id, plat['text_field'])
            if len(comments) < 20:
                # מאות תגובות בשיטס אבל שרשור כמעט ריק = בעיית API, לא תוכן
                print(f"⚠️ Only {len(comments)} comments pulled (sheet says {sheet_comments}) - skipping")
                failures += 1
                continue
            print(f"📥 {len(comments)} comments pulled")

            analysis = analyze_with_gemini(client, plat, post, comments)
            print(f"🧠 {analysis.get('summary', '')[:100]}")

            new_rows.append([
                item_id,
                plat['key'],
                str(post.get(plat['date_col'], '')),
                datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M'),
                str(post.get(plat['type_col'], '')),
                str(post.get(plat['caption_col'], ''))[:200],
                sheet_comments,
                len(comments),
                analysis['sentiment_positive'],
                analysis['sentiment_negative'],
                analysis['sentiment_neutral'],
                analysis['coverage_criticism'],
                '; '.join(analysis['themes'][:4]),
                ' | '.join(t[:150] for t in analysis['top_comments'][:3]),
                analysis['why_it_worked'],
                'כן' if analysis['controversy'] else 'לא',
                analysis['summary'],
                str(post.get(plat['url_col'], '')),
            ])
        except Exception as e:
            print(f"❌ Failed to analyze {item_id}: {str(e)[:200]}")
            failures += 1
        time.sleep(0.5)
    return new_rows, failures


def main():
    print(f"\n{'='*50}")
    print(f"💬 Comment Analyzer - {datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}\n")

    missing = [n for n, v in [('FACEBOOK_TOKEN', ACCESS_TOKEN),
                              ('GEMINI_API_KEY', GEMINI_API_KEY)] if not v]
    if missing:
        print(f"❌ Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    sh = open_spreadsheet()
    target_ws, analyzed_ids = get_target_sheet(sh)
    print(f"📋 {len(analyzed_ids)} posts already analyzed")

    client = genai.Client(api_key=GEMINI_API_KEY)
    all_rows, total_failures = [], 0
    for plat in PLATFORMS:
        new_rows, failures = run_platform(sh, client, plat, analyzed_ids)
        all_rows.extend(new_rows)
        total_failures += failures

    if all_rows:
        if target_ws is None:
            target_ws = sh.add_worksheet(title=TARGET_SHEET, rows=1000,
                                         cols=len(TARGET_HEADER))
            target_ws.append_row(TARGET_HEADER)
            print(f"✅ Created new sheet: {TARGET_SHEET}")
        target_ws.append_rows(all_rows, value_input_option='RAW')
        print(f"\n✅ Saved {len(all_rows)} analyses to {TARGET_SHEET}")

    if total_failures and not all_rows:
        print(f"❌ All {total_failures} candidate analyses failed")
        sys.exit(1)
    if total_failures:
        print(f"⚠️ {total_failures} candidates failed - will retry in the next run")


if __name__ == "__main__":
    main()
