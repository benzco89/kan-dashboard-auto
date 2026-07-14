"""
Demographics Collector - צילום יומי של דמוגרפיית קהל האינסטגרם.

הדמוגרפיה משתנה לאט, אבל צילום יומי בונה לאורך זמן את התשובה לשאלות מגמה
("הקהל הצעיר גדל?"). נאסף פילוח העוקבים בלבד:
  - פייסבוק: מטא הסירו את page_fans_* (נבדק ב-probe 2026-07-14)
  - engaged_audience_demographics: כל ערך timeframe נדחה עם "no longer
    supported" (נוסה 90/30 יום, 2026-07-14) - המטריקה בתהליך גסיסה. לא לרדוף.

פורמט הגיליון ("דמוגרפיה"): שורה לכל (תאריך, קהל, מימד, ערך) - long format,
~60 שורות ליום. הרצה חוזרת באותו יום מחליפה את שורות היום.

Env: FACEBOOK_TOKEN, GCP_SERVICE_ACCOUNT.
"""

import os
import sys
import json
import time
from datetime import datetime

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
SHEET_NAME = "דמוגרפיה"

IL_TZ = pytz.timezone('Asia/Jerusalem')

BREAKDOWNS = ('age', 'gender', 'city', 'country')
TOP_PER_BREAKDOWN = 15   # ערים/מדינות - זנב ארוך שלא מעניין


def fetch_breakdown(ig_id, metric, breakdown, timeframe=None):
    params = {
        'access_token': ACCESS_TOKEN,
        'metric': metric,
        'period': 'lifetime',
        'metric_type': 'total_value',
        'breakdown': breakdown,
    }
    if timeframe:
        params['timeframe'] = timeframe
    res = http_get_json(f"{BASE}/{ig_id}/insights", params=params,
                        timeout=15, max_retries=2)
    if 'error' in res:
        print(f"⚠️ {metric}[{breakdown}]: {res['error'].get('message', '')[:90]}")
        return []
    out = []
    for item in res.get('data', []):
        for b in item.get('total_value', {}).get('breakdowns', []):
            for r in b.get('results', []):
                key = '/'.join(r.get('dimension_values', []))
                out.append((key, r.get('value', 0) or 0))
    out.sort(key=lambda kv: -kv[1])
    return out[:TOP_PER_BREAKDOWN]


def main():
    print(f"\n{'='*50}")
    print(f"👥 Demographics Collector - {datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}\n")

    if not ACCESS_TOKEN:
        print("❌ Missing FACEBOOK_TOKEN")
        sys.exit(1)

    res = http_get_json(f"{BASE}/me", params={
        'access_token': ACCESS_TOKEN, 'fields': 'instagram_business_account'})
    ig_id = (res.get('instagram_business_account') or {}).get('id')
    if not ig_id:
        print("❌ Could not resolve IG account id")
        sys.exit(1)

    today = datetime.now(IL_TZ).strftime('%Y-%m-%d')
    rows = []
    for breakdown in BREAKDOWNS:
        for key, value in fetch_breakdown(ig_id, 'follower_demographics', breakdown):
            rows.append({
                'date': today, 'audience': 'followers',
                'dimension': breakdown, 'key': key, 'value': int(value),
            })
        time.sleep(0.2)
    print(f"📥 followers: {len(rows)} rows")

    if not rows:
        print("❌ Nothing fetched")
        sys.exit(1)

    new_df = pd.DataFrame(rows)
    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT') or os.environ.get('GOOGLE_CREDENTIALS')
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'])
    sh = gspread.authorize(creds).open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_NAME)
        existing = pd.DataFrame(ws.get_all_records())
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=5000, cols=8)
        existing = pd.DataFrame()
        print(f"✅ Created new sheet: {SHEET_NAME}")

    if not existing.empty:
        existing = existing[existing['date'].astype(str) != today]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df
    combined = combined.sort_values(['date', 'audience', 'dimension'], ascending=[False, True, True])
    ws.clear()
    ws.update([combined.columns.tolist()] + combined.values.tolist())
    print(f"✅ Saved {len(new_df)} rows ({len(combined)} total)")


if __name__ == "__main__":
    main()
