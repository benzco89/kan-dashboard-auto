"""
Hot Sniffer - זיהוי תוך-יומי של פוסט שמתפוצץ עכשיו -> התראת טלגרם.

הפייפליין המלא רץ פעם ביום ב-08:30, אז פוסט שמתלקח באמצע היום מתגלה רק
למחרת. הרחרחן רץ כל כמה שעות (טיימר על ה-VPS -> workflow_dispatch, אותו
דפוס כמו הריצה היומית), שולף רק ספירות חיות של הפוסטים מ-24 השעות האחרונות,
ומתריע כשמשהו חוצה סף. חשוב: לא נוגע בגיליונות הקולקטורים (שם מחושבות
דלתות יומיות) - כותב רק לטאב ייעודי hot_alerts לצורך דה-דופ.

ספים (פוסט צעיר מ-24ש שחוצה אחד מהם = חם):
  תגובות: רצפת ה"חם" של comment_analyzer (אינסטגרם 600, פייסבוק 1000)
  צפיות / לייקים / שיתופים: פי-1.5 מ-p90 של 7 הימים האחרונים (מחושב בזמן
  ריצה מהגיליון). הריצה הראשונה הראתה ש-p90 לבדו רועש - פוסט בן 20+ שעות
  נושק ל-p90 באופן טבעי; רק חצייה ברורה של מה שפוסט *בשל* משיג = מתפוצץ.

Env: FACEBOOK_TOKEN, GCP_SERVICE_ACCOUNT, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID.
"""

import os
import re
import sys
import json
from datetime import datetime, timedelta

import gspread
import pytz
from google.oauth2.service_account import Credentials

from utils import http_get_json, send_telegram_alert

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ACCESS_TOKEN = os.environ.get('FACEBOOK_TOKEN')
PAGE_ID = os.environ.get('FACEBOOK_PAGE_ID') or "220634478361516"
API_VERSION = "v25.0"
BASE = f"https://graph.facebook.com/{API_VERSION}"

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
STATE_SHEET = "hot_alerts"
STATE_HEADER = ['post_id', 'platform', 'alerted_at', 'triggers', 'permalink']

IL_TZ = pytz.timezone('Asia/Jerusalem')

HOT_COMMENTS = {'instagram': 600, 'facebook': 1000}  # = 2x רצפת comment_analyzer
BASELINE_MULT = 1.5   # "חם" = פי-1.5 מ-p90, לא סתם לגעת בו
BASELINE_DAYS = 7
YOUNG_HOURS = 24


def _num(v):
    try:
        return float(str(v).replace(',', '').strip() or 0)
    except (ValueError, TypeError):
        return 0.0


def _p90(xs):
    xs = sorted(x for x in xs if x > 0)
    if not xs:
        return 0.0
    k = (len(xs) - 1) * 0.9
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] * (1 - (k - lo)) + xs[hi] * (k - lo)


def _parse_ts(ts):
    ts = re.sub(r'\+0000$', '+00:00', str(ts).replace('Z', '+00:00'))
    return datetime.fromisoformat(ts).astimezone(IL_TZ)


def open_spreadsheet():
    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT') or os.environ.get('GOOGLE_CREDENTIALS')
    creds_dict = json.loads(creds_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)


def get_baselines(sh):
    """p90 של 7 ימים לכל פלטפורמה, מהגיליונות שהפייפליין כבר מתחזק (קריאה בלבד)."""
    cutoff = (datetime.now(IL_TZ) - timedelta(days=BASELINE_DAYS)).strftime('%Y-%m-%d')
    out = {}
    for plat, sheet in (('instagram', 'נתוני אינסטגרם'), ('facebook', 'נתוני פייסבוק')):
        rows = [r for r in sh.worksheet(sheet).get_all_records()
                if str(r.get('date', ''))[:10] >= cutoff]
        out[plat] = {
            'views': _p90([_num(r.get('views')) for r in rows]),
            'likes': _p90([_num(r.get('likes')) for r in rows]),
            'shares': _p90([_num(r.get('shares')) for r in rows]),
        }
        print(f"📐 {plat} p90 (7d, n={len(rows)}): " +
              ", ".join(f"{k}={v:,.0f}" for k, v in out[plat].items()))
    return out


def get_state(sh):
    """פוסטים שכבר הותרעו - פוסט מתריע פעם אחת בלבד."""
    try:
        ws = sh.worksheet(STATE_SHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=STATE_SHEET, rows=500, cols=len(STATE_HEADER))
        ws.append_row(STATE_HEADER)
        print(f"✅ Created state sheet: {STATE_SHEET}")
        return ws, set()
    ids = {str(v).strip() for v in ws.col_values(1)[1:] if str(v).strip()}
    return ws, ids


def fetch_young_instagram():
    """פוסטים אחרונים + ספירות חיות. צפיות/שיתופים דורשים קריאת insights לפוסט."""
    res = http_get_json(f"{BASE}/me", params={
        'access_token': ACCESS_TOKEN, 'fields': 'instagram_business_account'})
    ig_id = (res.get('instagram_business_account') or {}).get('id')
    if not ig_id:
        print("⚠️ Could not resolve IG account id")
        return []

    res = http_get_json(f"{BASE}/{ig_id}/media", params={
        'access_token': ACCESS_TOKEN,
        'fields': 'id,caption,timestamp,permalink,like_count,comments_count',
        'limit': 25,
    })
    cutoff = datetime.now(IL_TZ) - timedelta(hours=YOUNG_HOURS)
    posts = []
    for m in res.get('data', []):
        try:
            posted = _parse_ts(m.get('timestamp'))
        except (ValueError, TypeError):
            continue
        if posted < cutoff:
            continue
        views = shares = 0
        ins = http_get_json(f"{BASE}/{m['id']}/insights", params={
            'access_token': ACCESS_TOKEN, 'metric': 'views,shares'},
            timeout=15, max_retries=2)
        for item in ins.get('data', []):
            vals = item.get('values', [])
            v = (vals[0].get('value') or 0) if vals else 0
            if item.get('name') == 'views':
                views = v
            elif item.get('name') == 'shares':
                shares = v
        posts.append({
            'id': m['id'], 'platform': 'instagram',
            'title': (m.get('caption') or '')[:120],
            'posted': posted, 'permalink': m.get('permalink', ''),
            'views': views, 'likes': m.get('like_count', 0) or 0,
            'comments': m.get('comments_count', 0) or 0, 'shares': shares,
        })
    return posts


def fetch_young_facebook():
    """ספירות ציבוריות בלבד (ריאקציות/תגובות/שיתופים) - בלי insights, קריאה אחת."""
    res = http_get_json(f"{BASE}/{PAGE_ID}/published_posts", params={
        'access_token': ACCESS_TOKEN,
        'fields': 'id,message,created_time,permalink_url,shares,'
                  'comments.summary(true).limit(0),reactions.summary(true).limit(0)',
        'limit': 25,
    })
    cutoff = datetime.now(IL_TZ) - timedelta(hours=YOUNG_HOURS)
    posts = []
    for p in res.get('data', []):
        try:
            posted = _parse_ts(p.get('created_time'))
        except (ValueError, TypeError):
            continue
        if posted < cutoff:
            continue
        posts.append({
            'id': p['id'], 'platform': 'facebook',
            'title': (p.get('message') or '').replace('\n', ' ')[:120],
            'posted': posted, 'permalink': p.get('permalink_url', ''),
            'views': 0,  # צפיות FB דורשות insights; הריאקציות/תגובות מספיקות לזיהוי
            'likes': p.get('reactions', {}).get('summary', {}).get('total_count', 0),
            'comments': p.get('comments', {}).get('summary', {}).get('total_count', 0),
            'shares': (p.get('shares') or {}).get('count', 0),
        })
    return posts


def check_triggers(post, baseline):
    """אילו ספים הפוסט חצה. מחזיר רשימת תיאורים להתראה (ריק = לא חם)."""
    trig = []
    if post['comments'] >= HOT_COMMENTS[post['platform']]:
        trig.append(f"💬 {post['comments']:,.0f} תגובות (סף {HOT_COMMENTS[post['platform']]:,})")
    for key, emoji, label in (('views', '👁', 'צפיות'), ('likes', '❤️', 'לייקים'),
                              ('shares', '🔁', 'שיתופים')):
        floor = baseline[key] * BASELINE_MULT
        if floor and post[key] >= floor:
            trig.append(f"{emoji} {post[key]:,.0f} {label} (פי-{post[key] / baseline[key]:.1f} מ-p90 השבועי)")
    return trig


def main():
    now = datetime.now(IL_TZ)
    print(f"\n🔥 Hot Sniffer - {now.strftime('%Y-%m-%d %H:%M')}\n")

    if not ACCESS_TOKEN:
        print("❌ Missing FACEBOOK_TOKEN")
        sys.exit(1)

    sh = open_spreadsheet()
    baselines = get_baselines(sh)
    state_ws, alerted_ids = get_state(sh)
    print(f"📋 {len(alerted_ids)} posts already alerted\n")

    young = fetch_young_instagram() + fetch_young_facebook()
    print(f"🔎 {len(young)} posts younger than {YOUNG_HOURS}h")

    hot = []
    for post in young:
        if post['id'] in alerted_ids:
            continue
        triggers = check_triggers(post, baselines[post['platform']])
        if triggers:
            hot.append((post, triggers))

    if not hot:
        print("ℹ️ Nothing exploding right now.")
        return

    plat_he = {'instagram': '📸 אינסטגרם', 'facebook': '📘 פייסבוק'}
    blocks = []
    state_rows = []
    for post, triggers in hot:
        age_h = (now - post['posted']).total_seconds() / 3600
        blocks.append(
            f"{plat_he[post['platform']]} · לפני {age_h:.0f} שעות\n"
            f"«{post['title']}»\n" + "\n".join(triggers) +
            (f"\n{post['permalink']}" if post['permalink'] else ""))
        state_rows.append([
            post['id'], post['platform'], now.strftime('%Y-%m-%d %H:%M'),
            '; '.join(triggers), post['permalink']])

    message = "🔥 מתפוצץ עכשיו\n\n" + "\n\n".join(blocks)
    print(message)
    if send_telegram_alert(message):
        print(f"\n✅ Alerted on {len(hot)} posts")
    else:
        print("\n❌ Telegram send failed")
        sys.exit(1)
    # נרשם רק אחרי שההתראה נשלחה בפועל - שליחה שנכשלה תנוסה שוב בריצה הבאה
    state_ws.append_rows(state_rows, value_input_option='RAW')


if __name__ == "__main__":
    main()
