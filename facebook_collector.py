import os
import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time
import json
import pytz
import re

from utils import http_get_json, backfill_zero_metrics

# Load .env file if exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Config ---
ACCESS_TOKEN = os.environ.get('FACEBOOK_TOKEN')
PAGE_ID = os.environ.get('FACEBOOK_PAGE_ID') or "220634478361516"  # or-fallback: the GitHub secret is EMPTY, and workflows set empty env vars
API_VERSION = "v25.0"  # bumped from v24: the legacy reach metric (post_impressions_unique)
                       # was removed for ALL versions on 2026-06-15; v25 exposes the
                       # unified media-view metrics (post_total_media_view_unique / post_media_view).
DAYS_BACK = 7

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET_NAME = "נתוני פייסבוק"

# --- Functions ---

def get_video_direct_metrics(video_id):
    """משיכת צפיות ישירות מאובייקט הוידאו (גיבוי)"""
    if not video_id:
        return 0
    url = f"https://graph.facebook.com/{API_VERSION}/{video_id}"
    params = {'access_token': ACCESS_TOKEN, 'fields': 'views'}
    try:
        res = http_get_json(url, params=params, timeout=15, max_retries=2)
        return res.get('views', 0)
    except:
        return 0

def _flatten_value(v):
    """post_media_view may return a number or a paid/organic dict; reduce to a total."""
    if isinstance(v, dict):
        total = 0
        for x in v.values():
            try:
                total += float(x)
            except (TypeError, ValueError):
                pass
        return int(total)
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _insight(obj_id, metric, endpoint="insights"):
    """Fetch one lifetime insight metric on its own (so one bad name can't fail the batch). 0 on error/missing."""
    params = {'access_token': ACCESS_TOKEN, 'metric': metric}
    if endpoint == "insights":
        params['period'] = 'lifetime'
    try:
        res = http_get_json(f"https://graph.facebook.com/{API_VERSION}/{obj_id}/{endpoint}", params=params, timeout=15, max_retries=2)
        if 'error' in res:
            return 0
        data = res.get('data', [])
        if not data:
            return 0
        values = data[0].get('values', [])
        return _flatten_value(values[0].get('value')) if values else 0
    except Exception:
        return 0


def get_video_id(post):
    """Extract the underlying video object id from a post's attachment (for video_insights)."""
    try:
        return post['attachments']['data'][0]['target']['id']
    except Exception:
        return None


def get_base_insights(post_id):
    """
    מדדים בסיסיים לכל סוגי הפוסטים, במטריקות העדכניות (Graph API v25):
      reach  = post_total_media_view_unique  (צופים ייחודיים; מחליף post_impressions_unique שהוסר 15.6.2026)
      views  = post_media_view               (צפיות מאוחדות לכל סוגי התוכן, כולל תמונות/טקסט)
      clicks = post_clicks
    כל מטריקה נמשכת בנפרד כדי ששם לא-תקין אחד לא יפיל את כל הקריאה.
    (שמות המדויקים מאומתים מול הדף דרך fb_metric_probe.py לפני מיזוג.)
    """
    return {
        'reach': _insight(post_id, 'post_total_media_view_unique'),
        'views': _insight(post_id, 'post_media_view'),
        'clicks': _insight(post_id, 'post_clicks'),
    }


def get_video_insights(video_id):
    """
    מדדי וידאו/רילז (watch-time) מאובייקט הוידאו דרך video_insights (Graph API v25).
    שמות מאומתים מול הדף (probe): עבור רילז עובדים post_video_* על endpoint זה,
    בעוד total_video_* מוחזרים ריקים (הם לוידאו רגיל בלבד):
      plays           = blue_reels_play_count  (fallback: total_video_views לוידאו רגיל)
      avg_watch_sec   = post_video_avg_time_watched  (ms -> sec)
      total_watch_min = post_video_view_time          (ms -> min)
      views_30s       = total_video_30s_views (זמין לוידאו רגיל; לרילז מוחזר 0)
    """
    result = {'plays': 0, 'avg_watch_sec': 0, 'views_30s': 0, 'total_watch_min': 0}
    if not video_id:
        return result

    plays = _insight(video_id, 'blue_reels_play_count', endpoint='video_insights')
    if not plays:
        plays = _insight(video_id, 'total_video_views', endpoint='video_insights')
    result['plays'] = plays

    avg_ms = _insight(video_id, 'post_video_avg_time_watched', endpoint='video_insights')
    result['avg_watch_sec'] = round(avg_ms / 1000, 1) if avg_ms else 0

    total_ms = _insight(video_id, 'post_video_view_time', endpoint='video_insights')
    result['total_watch_min'] = round(total_ms / 60000, 1) if total_ms else 0

    result['views_30s'] = _insight(video_id, 'total_video_30s_views', endpoint='video_insights')

    return result


def get_public_metrics(post_id):
    """משיכת מדדים ציבוריים - לייקים, תגובות, שיתופים"""
    url = f"https://graph.facebook.com/{API_VERSION}/{post_id}"
    params = {
        'access_token': ACCESS_TOKEN,
        'fields': 'shares,comments.summary(true).limit(0),reactions.summary(true).limit(0)'
    }
    try:
        res = http_get_json(url, params=params, timeout=15, max_retries=2)
        likes = 0
        if 'reactions' in res and 'summary' in res['reactions']:
            likes = res['reactions']['summary']['total_count']
        
        return {
            'shares': res.get('shares', {}).get('count', 0),
            'comments': res.get('comments', {}).get('summary', {}).get('total_count', 0),
            'likes': likes
        }
    except:
        return {'shares': 0, 'comments': 0, 'likes': 0}


def detect_media_type(post):
    """זיהוי סוג הפוסט"""
    permalink = post.get('permalink_url', '')
    if '/reel/' in permalink:
        return 'Reel'
    if '/videos/' in permalink:
        return 'Video'

    if 'attachments' in post and 'data' in post['attachments']:
        att = post['attachments']['data'][0]
        att_type = att.get('type', '')
        url = att.get('url', '')

        if 'reel' in url or 'reel' in att.get('target', {}).get('url', ''):
            return 'Reel'
        if att_type in ['video_inline', 'video_direct', 'video_autoplay', 'video']:
            return 'Video'
        if att_type in ['photo', 'cover_photo', 'album']:
            return 'Photo'
        if att_type in ['share', 'link']:
            return 'Link'

    return 'Status'


def fetch_facebook_data():
    print(f"🚀 Facebook Collector - {datetime.now()}")

    il_now = datetime.now(pytz.timezone('Asia/Jerusalem'))
    since_unix = int((il_now - timedelta(days=DAYS_BACK)).timestamp())
    all_posts = []

    url = f"https://graph.facebook.com/{API_VERSION}/{PAGE_ID}/feed"
    params = {
        'access_token': ACCESS_TOKEN,
        'limit': 25,
        'fields': 'id,created_time,message,permalink_url,attachments',
        'since': since_unix
    }

    while True:
        try:
            res = http_get_json(url, params=params)
        except Exception as e:
            print(f"❌ Feed request failed after retries: {e} - keeping {len(all_posts)} posts fetched so far")
            break

        if 'error' in res:
            print(f"❌ API Error: {res['error']['message']}")
            break
            
        if 'data' not in res or not res['data']:
            break

        for post in res['data']:
            post_id = post['id']
            media_type = detect_media_type(post)

            # 1. מדדים בסיסיים לכל הסוגים (reach + views + clicks במטריקות v25)
            base = get_base_insights(post_id)
            reach = base['reach']
            views = base['views']
            clicks = base['clicks']

            # 2. מדדי וידאו/רילז (watch-time) מאובייקט הוידאו
            video = {'plays': 0, 'avg_watch_sec': 0, 'views_30s': 0, 'total_watch_min': 0}
            if media_type in ['Video', 'Reel']:
                video = get_video_insights(get_video_id(post))
                # אם post_media_view לא החזיר צפיות לריל, ניפול למספר ה-plays
                if views == 0 and video['plays'] > 0:
                    views = video['plays']

            # 3. משיכת מדדים ציבוריים
            public = get_public_metrics(post_id)

            # 4. חישובים (ללא ה-fallback הישן reach=views — reach ו-views הם מדדים נפרדים)
            total_eng = clicks + public['likes'] + public['comments'] + public['shares']
            engagement_rate = round((total_eng / reach) * 100, 2) if reach > 0 else 0

            # חישוב completion rate
            completion_rate = 0
            if views > 0 and video['views_30s'] > 0:
                completion_rate = round((video['views_30s'] / views) * 100, 1)

            # 5. המרת זמן
            il_tz = pytz.timezone('Asia/Jerusalem')
            created_time = post['created_time']
            ts_normalized = re.sub(r'\+0000$', '+00:00', created_time.replace('Z', '+00:00'))
            post_datetime = datetime.fromisoformat(ts_normalized).astimezone(il_tz)
            
            all_posts.append({
                'post_id': post_id,
                'date': post_datetime.strftime('%Y-%m-%d'),
                'time': post_datetime.strftime('%H:%M'),
                'type': media_type,
                'title': (post.get('message', '') or '').replace('\n', ' ')[:500],
                'reach': reach,
                'clicks': clicks,
                'views': views,
                'views_30s': video['views_30s'],
                'total_watch_min': video['total_watch_min'],
                'avg_watch_sec': video['avg_watch_sec'],
                'completion_rate': completion_rate,
                'likes': public['likes'],
                'comments': public['comments'],
                'shares': public['shares'],
                'total_engagement': total_eng,
                'engagement_rate': engagement_rate,
                'permalink': post.get('permalink_url', ''),
                'pulled_at': datetime.now(il_tz).strftime('%Y-%m-%d %H:%M')
            })
            
            time.sleep(0.2)  # Rate limiting - קצת יותר איטי בגלל הקריאות הנוספות

        if 'paging' in res and 'next' in res['paging']:
            url = res['paging']['next']
            params = {}
        else:
            break

    print(f"📊 Fetched {len(all_posts)} posts")
    return pd.DataFrame(all_posts)


def save_to_sheets(new_df):
    """שמירה לגוגל שיטס"""
    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT')
    if not creds_json:
        creds_json = os.environ.get('GOOGLE_CREDENTIALS')

    creds_dict = json.loads(creds_json)
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = sh.worksheet(SHEET_NAME)
    except:
        worksheet = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=25)

    # קריאת היסטוריה
    try:
        existing_data = worksheet.get_all_records()
        existing_df = pd.DataFrame(existing_data)
    except Exception as e:
        print(f"⚠️ Warning reading existing data: {e}")
        existing_df = pd.DataFrame()

    # מיזוג
    if not existing_df.empty:
        new_df['post_id'] = new_df['post_id'].astype(str)
        existing_df['post_id'] = existing_df['post_id'].astype(str)

        # הגנה מפני כשלי API רגעיים: 0 חדש לא דורס ערך חיובי קיים
        new_df, suspicious_cols = backfill_zero_metrics(
            new_df, existing_df, key='post_id',
            cols=['reach', 'clicks', 'views', 'views_30s', 'total_watch_min',
                  'avg_watch_sec', 'completion_rate', 'likes', 'comments',
                  'shares', 'total_engagement', 'engagement_rate']
        )

        # חישוב דלתא לצפיות
        if 'views' in existing_df.columns:
            existing_df['views'] = pd.to_numeric(existing_df['views'], errors='coerce').fillna(0)
            view_map = existing_df.set_index('post_id')['views'].to_dict()
            new_df['views_delta'] = new_df.apply(
                lambda x: x['views'] - view_map.get(x['post_id'], x['views']), axis=1
            )
        else:
            new_df['views_delta'] = 0

        # חישוב דלתא ל-reach
        if 'reach' in existing_df.columns:
            existing_df['reach'] = pd.to_numeric(existing_df['reach'], errors='coerce').fillna(0)
            reach_map = existing_df.set_index('post_id')['reach'].to_dict()
            new_df['reach_delta'] = new_df.apply(
                lambda x: x['reach'] - reach_map.get(x['post_id'], x['reach']), axis=1
            )
        else:
            new_df['reach_delta'] = 0

        # וידוא עמודות
        for col in new_df.columns:
            if col not in existing_df.columns:
                existing_df[col] = ""

        combined = pd.concat([new_df, existing_df])
        final_df = combined.drop_duplicates(subset=['post_id'], keep='first')
        print(f"🔄 Merged: {len(new_df)} new + {len(existing_df)} existing -> {len(final_df)} total")
    else:
        new_df['views_delta'] = 0
        new_df['reach_delta'] = 0
        final_df = new_df
        suspicious_cols = []

    # ניקוי ומיון
    final_df = final_df.sort_values(by='date', ascending=False)
    final_df = final_df.fillna(0).replace([float('inf'), float('-inf')], 0)

    # שמירה
    worksheet.clear()
    worksheet.update([final_df.columns.tolist()] + final_df.values.tolist())
    print(f"✅ Saved {len(final_df)} rows to {SHEET_NAME}")
    return suspicious_cols


def main():
    if not ACCESS_TOKEN:
        print("❌ Missing FACEBOOK_TOKEN environment variable")
        sys.exit(1)

    df = fetch_facebook_data()
    if df.empty:
        # יציאה בקוד שגיאה כדי ששלב ההתראות ב-workflow ידווח על הכשל
        print("❌ No data collected.")
        sys.exit(1)

    suspicious_cols = save_to_sheets(df)
    print(f"✅ Done! {len(df)} posts processed.")
    if suspicious_cols:
        # הגיליון מוגן (ערכים קודמים נשמרו), אבל מטריקה חזרה 0 כמעט לכל
        # הפוסטים - כנראה שבירת API. מכשילים את השלב כדי שההתראה תישלח.
        print(f"❌ Suspicious all-zero metrics: {suspicious_cols} - possible API metric breakage")
        sys.exit(1)


if __name__ == "__main__":
    main()
