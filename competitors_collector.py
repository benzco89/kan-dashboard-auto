"""
Competitors Collector - צילום מצב יומי של חשבונות אינסטגרם מתחרים.

משתמש ב-Business Discovery API: נתונים ציבוריים של כל חשבון עסקי - עוקבים,
פוסטים אחרונים עם לייקים/תגובות - עם הטוקן הקיים של כאן, בלי שום הרשאה
נוספת. אין צפיות/reach (פרטיים לבעל החשבון); ההשוואה היא על עוקבים, צמיחה,
קצב פרסום ומעורבות. כל הרצה כותבת שורה-לכל-חשבון לגיליון "מתחרים"
(idempotent - הרצה חוזרת באותו יום מחליפה את שורות היום).

הרשימה אומתה חיה מול ה-API ב-2026-07-13. רשת ב (kan_reshetb) בכוונה לא כאן -
בהמתנה להרשאות אדמין שיאפשרו גישה מלאה דרך הפייפליין הרגיל.

Env: FACEBOOK_TOKEN, GCP_SERVICE_ACCOUNT.
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta

import gspread
import pandas as pd
import pytz
from google.oauth2.service_account import Credentials

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

ACCESS_TOKEN = os.environ.get('FACEBOOK_TOKEN')
API_VERSION = "v25.0"
BASE = f"https://graph.facebook.com/{API_VERSION}"

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET_NAME = "מתחרים"

IL_TZ = pytz.timezone('Asia/Jerusalem')

COMPETITORS = [
    'n12news', 'ynetgram', '13newsil', 'yedioth', 'israelhayom',
    'haaretz', 'wallanews', 'maarivonline', 'globesnews',
    'm_laradar', 'yomi_news',
]

RECENT_FOR_AVG = 10   # ממוצע מעורבות על הפוסטים האחרונים


def get_own_ig_id():
    res = http_get_json(f"{BASE}/me", params={
        'access_token': ACCESS_TOKEN, 'fields': 'instagram_business_account'})
    return (res.get('instagram_business_account') or {}).get('id')


def fetch_account(own_ig, username):
    """Business discovery לחשבון אחד; מחזיר dict שורה או None בכשל."""
    res = http_get_json(f"{BASE}/{own_ig}", params={
        'access_token': ACCESS_TOKEN,
        'fields': f"business_discovery.username({username})"
                  "{username,name,followers_count,media_count,"
                  "media.limit(25){like_count,comments_count,timestamp,caption,permalink,media_type}}",
    }, timeout=20, max_retries=2)
    if 'error' in res:
        print(f"⚠️ {username}: {res['error'].get('message', '')[:100]}")
        return None
    bd = res.get('business_discovery', {})
    media = bd.get('media', {}).get('data', [])
    now = datetime.now(IL_TZ)
    cutoff_24h = now - timedelta(hours=24)

    def _ts(m):
        try:
            return datetime.fromisoformat(
                str(m.get('timestamp', '')).replace('Z', '+00:00').replace('+0000', '+00:00')
            ).astimezone(IL_TZ)
        except ValueError:
            return None

    last_day = [m for m in media if (_ts(m) or cutoff_24h) > cutoff_24h and _ts(m)]
    recent = media[:RECENT_FOR_AVG]
    avg_likes = round(sum(m.get('like_count', 0) or 0 for m in recent) / len(recent)) if recent else 0
    avg_comments = round(sum(m.get('comments_count', 0) or 0 for m in recent) / len(recent)) if recent else 0
    followers = bd.get('followers_count', 0) or 0
    eng_per_1k = round((avg_likes + avg_comments) / followers * 1000, 2) if followers else 0

    top = max(last_day, key=lambda m: (m.get('like_count', 0) or 0) + (m.get('comments_count', 0) or 0),
              default=None)
    return {
        'date': now.strftime('%Y-%m-%d'),
        'username': bd.get('username', username),
        'name': bd.get('name', ''),
        'followers': followers,
        'followers_change': 0,  # מחושב מול השורה הקודמת בשמירה
        'media_count': bd.get('media_count', 0) or 0,
        'posts_24h': len(last_day),
        'avg_likes_recent': avg_likes,
        'avg_comments_recent': avg_comments,
        'eng_per_1k': eng_per_1k,
        'top_caption': (top.get('caption') or '').replace('\n', ' ')[:150] if top else '',
        'top_likes': (top.get('like_count', 0) or 0) if top else 0,
        'top_comments': (top.get('comments_count', 0) or 0) if top else 0,
        'top_url': top.get('permalink', '') if top else '',
        'pulled_at': now.strftime('%Y-%m-%d %H:%M'),
    }


def save(new_df):
    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT') or os.environ.get('GOOGLE_CREDENTIALS')
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'])
    sh = gspread.authorize(creds).open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_NAME)
        existing = pd.DataFrame(ws.get_all_records())
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=2000, cols=20)
        existing = pd.DataFrame()
        print(f"✅ Created new sheet: {SHEET_NAME}")

    if not existing.empty:
        # דלתא עוקבים מול המדידה האחרונה של כל חשבון (לא מהיום)
        today = new_df['date'].iloc[0]
        prior = existing[existing['date'].astype(str) != today]
        if not prior.empty:
            last_by_user = (prior.sort_values('date')
                            .groupby('username')['followers'].last().to_dict())
            new_df['followers_change'] = new_df.apply(
                lambda r: r['followers'] - int(last_by_user.get(r['username'], r['followers']) or r['followers']),
                axis=1)
        # idempotent: הרצה חוזרת באותו יום מחליפה את שורות היום
        existing = existing[~((existing['date'].astype(str) == today) &
                              (existing['username'].isin(new_df['username'])))]
        for col in new_df.columns:
            if col not in existing.columns:
                existing[col] = ""
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.sort_values(['date', 'followers'], ascending=[True, False])
    combined = combined.fillna(0).replace([float('inf'), float('-inf')], 0)
    ws.clear()
    ws.update([combined.columns.tolist()] + combined.values.tolist())
    print(f"✅ Saved {len(new_df)} snapshots ({len(combined)} total rows)")


def main():
    print(f"\n{'='*50}")
    print(f"🥊 Competitors Collector - {datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}\n")

    if not ACCESS_TOKEN:
        print("❌ Missing FACEBOOK_TOKEN")
        sys.exit(1)
    own_ig = get_own_ig_id()
    if not own_ig:
        print("❌ Could not resolve own IG account id")
        sys.exit(1)

    rows = []
    for username in COMPETITORS:
        row = fetch_account(own_ig, username)
        if row:
            rows.append(row)
            print(f"📥 @{row['username']}: {row['followers']:,} followers · "
                  f"{row['posts_24h']} posts/24h · eng/1k={row['eng_per_1k']}")
        time.sleep(0.3)

    if not rows:
        print("❌ No accounts fetched")
        sys.exit(1)
    if len(rows) < len(COMPETITORS):
        print(f"⚠️ {len(COMPETITORS) - len(rows)} accounts failed - saving the rest")

    save(pd.DataFrame(rows))
    print(f"\n✅ Done! {len(rows)}/{len(COMPETITORS)} accounts tracked.")


if __name__ == "__main__":
    main()
