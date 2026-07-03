"""
Instagram Stories Collector - איסוף סטוריז ומדדיהם לגיליון ייעודי.

סטוריז חיים 24 שעות בלבד - מה שלא נאסף בבוקר אבוד לנצח. הקולקטור רץ יומית
אחרי איסוף הפוסטים ושומר כל סטורי חי עם המדדים שלו לגיליון "סטוריז אינסטגרם".
סטורי שעדיין חי בריצה הבאה מתעדכן (מיזוג לפי story_id, כמו שאר הקולקטורים).

המטריקות אומתו ב-probe מול החשבון החי (2026-07-03):
  views, reach, replies, shares, total_interactions,
  profile_visits, follows                       - עובדים על סטוריז (בניגוד לפוסטים!)
  navigation (breakdown=story_navigation_action_type) -
      tap_forward / tap_back / swipe_forward / tap_exit
"""

import os
import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time
import json
import re
import pytz

from utils import http_get_json, backfill_zero_metrics

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Config ---
ACCESS_TOKEN = os.environ.get('FACEBOOK_TOKEN')
API_VERSION = "v25.0"

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET_NAME = "סטוריז אינסטגרם"

IL_TZ = pytz.timezone('Asia/Jerusalem')

STORY_METRICS = ['views', 'reach', 'replies', 'shares', 'total_interactions',
                 'profile_visits', 'follows']


def get_instagram_account_id():
    url = f"https://graph.facebook.com/{API_VERSION}/me"
    params = {'access_token': ACCESS_TOKEN, 'fields': 'instagram_business_account'}
    try:
        res = http_get_json(url, params=params)
        return (res.get('instagram_business_account') or {}).get('id')
    except Exception as e:
        print(f"❌ Error resolving IG account: {e}")
        return None


def get_story_insights(story_id):
    """מדדי סטורי בודד: מדדי בסיס בקריאה אחת + ניווט בקריאה נפרדת."""
    result = {m: 0 for m in STORY_METRICS}
    result.update({'taps_forward': 0, 'taps_back': 0, 'swipes_forward': 0, 'exits': 0})

    base = f"https://graph.facebook.com/{API_VERSION}/{story_id}/insights"
    try:
        res = http_get_json(base, params={
            'access_token': ACCESS_TOKEN, 'metric': ','.join(STORY_METRICS),
        }, timeout=15, max_retries=2)
        if 'error' in res:
            print(f"⚠️ Story insights error for {story_id}: {res['error'].get('message', '')[:120]}")
        for item in res.get('data', []):
            name = item.get('name')
            values = item.get('values', [])
            v = values[0].get('value', 0) if values else 0
            if name in result:
                result[name] = v or 0
    except Exception as e:
        print(f"⚠️ Error fetching story insights for {story_id}: {e}")

    # ניווט: כמה דילגו קדימה/אחורה/החליקו לחשבון הבא/יצאו
    try:
        res = http_get_json(base, params={
            'access_token': ACCESS_TOKEN, 'metric': 'navigation',
            'breakdown': 'story_navigation_action_type', 'metric_type': 'total_value',
        }, timeout=15, max_retries=2)
        data = res.get('data', [])
        if data:
            tv = data[0].get('total_value', {})
            for b in tv.get('breakdowns', []):
                for r in b.get('results', []):
                    dim = '/'.join(r.get('dimension_values', []))
                    val = r.get('value', 0) or 0
                    if dim == 'tap_forward':
                        result['taps_forward'] = val
                    elif dim == 'tap_back':
                        result['taps_back'] = val
                    elif dim == 'swipe_forward':
                        result['swipes_forward'] = val
                    elif dim == 'tap_exit':
                        result['exits'] = val
    except Exception as e:
        print(f"⚠️ Error fetching story navigation for {story_id}: {e}")

    return result


def fetch_stories(ig_account_id):
    print(f"🚀 Stories Collector - {datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M')}")

    url = f"https://graph.facebook.com/{API_VERSION}/{ig_account_id}/stories"
    params = {
        'access_token': ACCESS_TOKEN,
        'fields': 'id,media_type,media_product_type,timestamp,permalink',
    }
    try:
        res = http_get_json(url, params=params)
    except Exception as e:
        print(f"❌ Stories request failed after retries: {e}")
        return None  # None = שגיאה (להבדיל מרשימה ריקה = אין סטוריז)

    if 'error' in res:
        print(f"❌ API Error: {res['error']['message']}")
        return None

    stories = res.get('data', [])
    print(f"📥 {len(stories)} live stories")

    rows = []
    for s in stories:
        story_id = s['id']
        ts = s.get('timestamp', '')
        dt = None
        if ts:
            ts_norm = re.sub(r'\+0000$', '+00:00', ts.replace('Z', '+00:00'))
            dt = datetime.fromisoformat(ts_norm).astimezone(IL_TZ)

        insights = get_story_insights(story_id)
        views = insights['views']
        # שיעור יציאה: כמה מהצופים נטשו את רצף הסטוריז כאן (exit + swipe לחשבון אחר)
        exit_rate = round((insights['exits'] + insights['swipes_forward']) / views * 100, 1) if views else 0

        rows.append({
            'story_id': story_id,
            'date': dt.strftime('%Y-%m-%d') if dt else '',
            'time': dt.strftime('%H:%M') if dt else '',
            'type': s.get('media_type', ''),
            'views': views,
            'reach': insights['reach'],
            'replies': insights['replies'],
            'shares': insights['shares'],
            'total_interactions': insights['total_interactions'],
            'profile_visits': insights['profile_visits'],
            'follows': insights['follows'],
            'taps_forward': insights['taps_forward'],
            'taps_back': insights['taps_back'],
            'swipes_forward': insights['swipes_forward'],
            'exits': insights['exits'],
            'exit_rate': exit_rate,
            'permalink': s.get('permalink', ''),
            'pulled_at': datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M'),
        })
        time.sleep(0.15)

    return pd.DataFrame(rows)


def save_to_sheets(new_df):
    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT') or os.environ.get('GOOGLE_CREDENTIALS')
    creds_dict = json.loads(creds_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = sh.worksheet(SHEET_NAME)
    except Exception:
        worksheet = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=20)
        print(f"✅ Created new sheet: {SHEET_NAME}")

    try:
        existing_df = pd.DataFrame(worksheet.get_all_records())
    except Exception as e:
        print(f"⚠️ Warning reading existing data: {e}")
        existing_df = pd.DataFrame()

    if not existing_df.empty and 'story_id' in existing_df.columns:
        new_df['story_id'] = new_df['story_id'].astype(str)
        existing_df['story_id'] = existing_df['story_id'].astype(str)

        # הגנה מפני כשלי API רגעיים: 0 חדש לא דורס ערך חיובי קיים
        new_df, suspicious_cols = backfill_zero_metrics(
            new_df, existing_df, key='story_id',
            cols=['views', 'reach', 'replies', 'shares', 'total_interactions',
                  'profile_visits', 'follows', 'taps_forward', 'taps_back',
                  'swipes_forward', 'exits', 'exit_rate']
        )

        for col in new_df.columns:
            if col not in existing_df.columns:
                existing_df[col] = ""

        combined = pd.concat([new_df, existing_df])
        final_df = combined.drop_duplicates(subset=['story_id'], keep='first')
        print(f"🔄 Merged: {len(new_df)} live + {len(existing_df)} existing -> {len(final_df)} total")
    else:
        final_df = new_df
        suspicious_cols = []

    final_df = final_df.sort_values(by='date', ascending=False)
    final_df = final_df.fillna(0).replace([float('inf'), float('-inf')], 0)

    worksheet.clear()
    worksheet.update([final_df.columns.tolist()] + final_df.values.tolist())
    print(f"✅ Saved {len(final_df)} rows to {SHEET_NAME}")
    return suspicious_cols


def main():
    print(f"\n{'='*50}")
    print(f"📱 Instagram Stories Collector - {datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}\n")

    if not ACCESS_TOKEN:
        print("❌ Missing FACEBOOK_TOKEN environment variable")
        sys.exit(1)

    ig_account_id = get_instagram_account_id()
    if not ig_account_id:
        sys.exit(1)

    df = fetch_stories(ig_account_id)
    if df is None:
        # שגיאת API אמיתית - להבדיל מבוקר בלי סטוריז חיים
        sys.exit(1)
    if df.empty:
        print("ℹ️ No live stories right now - nothing to save (not an error).")
        return

    suspicious_cols = save_to_sheets(df)
    print(f"\n✅ Done! {len(df)} stories processed.")
    if suspicious_cols:
        print(f"❌ Suspicious all-zero metrics: {suspicious_cols} - possible API breakage")
        sys.exit(1)


if __name__ == "__main__":
    main()
