"""
Comment Analyzer - ניתוח Gemini של תגובות לפוסטים "מדוברים" באינסטגרם.

המספרים אומרים *כמה* פוסט עבד; התגובות מגלות *למה*. הקולקטור בוחר בכל ריצה
את הפוסטים שעוררו הכי הרבה שיחה, מושך את שרשור התגובות המלא ומחלץ עם Gemini
סנטימנט, נושאים דומיננטיים והשערת "למה זה עבד". כל פוסט מנותח פעם אחת בלבד
והתוצאה נשמרת לגיליון "ניתוח תגובות" (append-only, שורה לכל media_id).

כלל הבחירה (כויל מול נתוני אמת, 2026-07-13, 942 פוסטים / 90 יום):
  חציון תגובות ≈ 95, p90 ≈ 500. רצפה של 300 תגובות (~p80) נותנת ~2 פוסטים
  ביום. עד 5 ניתוחים לריצה - רזרבה להשלמות אחרי ריצה שנכשלה.
  גיל מינימלי 24 שעות - שרשור תגובות מתייצב אחרי יום-יומיים; ניתוח מוקדם
  מדי תופס חצי שיחה.

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
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
API_VERSION = "v25.0"
BASE = f"https://graph.facebook.com/{API_VERSION}"

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SOURCE_SHEET = "נתוני אינסטגרם"
TARGET_SHEET = "ניתוח תגובות"

IL_TZ = pytz.timezone('Asia/Jerusalem')

COMMENT_FLOOR = 300      # רצפת תגובות (~p80 של החשבון) - מתחתיה אין מה לנתח
MAX_PER_RUN = 5          # תקרת ניתוחים לריצה (עלות Gemini + Graph API)
MIN_AGE_HOURS = 24       # שרשור צעיר מזה עוד לא התייצב
WINDOW_DAYS = 7          # לא חוזרים אחורה מעבר לחלון האיסוף של הפוסטים
MAX_COMMENTS_PULLED = 600    # תקרת עמודי Graph API לפוסט
MAX_COMMENTS_TO_GEMINI = 300  # הכי מלויקקות; מעבר לזה רק מנפח את הפרומפט

GEMINI_MODELS = ["gemini-3.1-pro-preview", "gemini-2.5-pro"]

TARGET_HEADER = ['media_id', 'post_date', 'analyzed_at', 'type', 'caption',
                 'comments_in_sheet', 'comments_pulled',
                 'sentiment_positive', 'sentiment_negative', 'sentiment_neutral',
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


def get_analyzed_ids(sh):
    """media_id-ים שכבר נותחו - פוסט מנותח פעם אחת בלבד."""
    try:
        ws = sh.worksheet(TARGET_SHEET)
    except gspread.WorksheetNotFound:
        return set(), None
    ids = {str(v).strip() for v in ws.col_values(1)[1:] if str(v).strip()}
    return ids, ws


def pick_candidates(sh, analyzed_ids):
    """הפוסטים המדוברים של השבוע שטרם נותחו: רצפה + top-N + גיל מינימלי."""
    rows = sh.worksheet(SOURCE_SHEET).get_all_records()
    now = datetime.now(IL_TZ)
    cutoff_old = now - timedelta(days=WINDOW_DAYS)
    cutoff_young = now - timedelta(hours=MIN_AGE_HOURS)

    candidates = []
    for r in rows:
        media_id = str(r.get('media_id', '')).strip()
        comments = _to_int(r.get('comments'))
        if not media_id or media_id in analyzed_ids or comments < COMMENT_FLOOR:
            continue
        try:
            posted = IL_TZ.localize(datetime.strptime(
                f"{str(r.get('date', '')).strip()} {str(r.get('time', '')).strip() or '00:00'}",
                '%Y-%m-%d %H:%M'))
        except ValueError:
            continue
        if posted < cutoff_old or posted > cutoff_young:
            continue
        candidates.append((comments, media_id, r))

    candidates.sort(key=lambda c: c[0], reverse=True)
    return candidates[:MAX_PER_RUN]


def fetch_comments(media_id):
    """שרשור התגובות המלא כולל רמה אחת של replies (מקוצב ב-MAX_COMMENTS_PULLED)."""
    comments = []
    url = f"{BASE}/{media_id}/comments"
    params = {
        'access_token': ACCESS_TOKEN,
        'fields': 'text,like_count,timestamp,replies{text,like_count}',
        'limit': 50,
    }
    while url and len(comments) < MAX_COMMENTS_PULLED:
        res = http_get_json(url, params=params, timeout=20, max_retries=2)
        if 'error' in res:
            print(f"⚠️ Comments error for {media_id}: {res['error'].get('message', '')[:120]}")
            break
        for c in res.get('data', []):
            comments.append({
                'text': c.get('text', ''),
                'like_count': c.get('like_count', 0) or 0,
                'replies': [{'text': r.get('text', ''), 'like_count': r.get('like_count', 0) or 0}
                            for r in c.get('replies', {}).get('data', [])],
            })
        url = res.get('paging', {}).get('next')
        params = None  # ה-URL הבא כבר נושא את הפרמטרים
    return comments


ANALYSIS_SCHEMA = {
    "type": "object",
    "properties": {
        "sentiment_positive": {"type": "integer", "description": "אחוז 0-100"},
        "sentiment_negative": {"type": "integer", "description": "אחוז 0-100"},
        "sentiment_neutral": {"type": "integer", "description": "אחוז 0-100"},
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
                 "themes", "top_comments", "why_it_worked", "controversy", "summary"],
}


def analyze_with_gemini(client, post, comments):
    """מחלץ מהשרשור סנטימנט, נושאים והשערת 'למה זה עבד' - JSON מובנה."""
    ranked = sorted(comments, key=lambda c: c['like_count'], reverse=True)
    lines = []
    for c in ranked[:MAX_COMMENTS_TO_GEMINI]:
        lines.append(f"[{c['like_count']}] {c['text'][:200]}")
        for r in c['replies'][:3]:
            lines.append(f"    ↳ [{r['like_count']}] {r['text'][:150]}")

    prompt = f"""אתה אנליסט סושיאל של חדר חדשות (כאן חדשות). לפניך פוסט אינסטגרם והתגובות אליו.
המספר בסוגריים בתחילת כל תגובה הוא כמות הלייקים שלה - תגובה מלויקקת משקפת סנטימנט רחב.

הפוסט ({post.get('type', '')}):
{str(post.get('caption', ''))[:500]}

מדדים: {_to_int(post.get('views'))} צפיות, {_to_int(post.get('comments'))} תגובות, {_to_int(post.get('shares'))} שיתופים, {_to_int(post.get('saved'))} שמירות.

התגובות ({len(comments)} נמשכו, מוצגות לפי לייקים):
{chr(10).join(lines)}

נתח בעברית. ב-why_it_worked אל תסתפק במה שהפוסט אומר - הסבר מה בתגובות מגלה מדוע
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
    analyzed_ids, target_ws = get_analyzed_ids(sh)
    print(f"📋 {len(analyzed_ids)} posts already analyzed")

    candidates = pick_candidates(sh, analyzed_ids)
    if not candidates:
        print(f"ℹ️ No posts above {COMMENT_FLOOR} comments awaiting analysis - nothing to do.")
        return
    print(f"🎯 {len(candidates)} candidates (floor={COMMENT_FLOOR}, max={MAX_PER_RUN}):")
    for comments, media_id, _ in candidates:
        print(f"   {media_id}  ({comments} comments)")

    client = genai.Client(api_key=GEMINI_API_KEY)
    new_rows, failures = [], 0

    for sheet_comments, media_id, post in candidates:
        print(f"\n--- {media_id} ---")
        try:
            comments = fetch_comments(media_id)
            if len(comments) < 20:
                # פוסט עם מאות תגובות בשיטס אבל שרשור כמעט ריק = בעיית API, לא תוכן
                print(f"⚠️ Only {len(comments)} comments pulled (sheet says {sheet_comments}) - skipping")
                failures += 1
                continue
            print(f"📥 {len(comments)} comments pulled")

            analysis = analyze_with_gemini(client, post, comments)
            print(f"🧠 {analysis.get('summary', '')[:100]}")

            new_rows.append([
                media_id,
                str(post.get('date', '')),
                datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M'),
                str(post.get('type', '')),
                str(post.get('caption', ''))[:200],
                sheet_comments,
                len(comments),
                analysis['sentiment_positive'],
                analysis['sentiment_negative'],
                analysis['sentiment_neutral'],
                '; '.join(analysis['themes'][:4]),
                ' | '.join(t[:150] for t in analysis['top_comments'][:3]),
                analysis['why_it_worked'],
                'כן' if analysis['controversy'] else 'לא',
                analysis['summary'],
                str(post.get('permalink', '')),
            ])
        except Exception as e:
            print(f"❌ Failed to analyze {media_id}: {str(e)[:200]}")
            failures += 1
        time.sleep(0.5)

    if new_rows:
        if target_ws is None:
            target_ws = sh.add_worksheet(title=TARGET_SHEET, rows=1000,
                                         cols=len(TARGET_HEADER))
            target_ws.append_row(TARGET_HEADER)
            print(f"✅ Created new sheet: {TARGET_SHEET}")
        target_ws.append_rows(new_rows, value_input_option='RAW')
        print(f"\n✅ Saved {len(new_rows)} analyses to {TARGET_SHEET}")

    if failures and not new_rows:
        print(f"❌ All {failures} candidate analyses failed")
        sys.exit(1)
    if failures:
        print(f"⚠️ {failures} candidates failed - will retry in the next run")


if __name__ == "__main__":
    main()
