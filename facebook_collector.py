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


def _insight_raw(obj_id, metric, endpoint="insights"):
    """כמו _insight, אבל מחזיר את הערך כמות שהוא.

    _insight מפעיל _flatten_value, שמסכם דיקט לסכום אחד - נכון ל-paid/organic
    ולא נכון לעקומת נטישה, שהיא דיקט של מקטע->אחוז. בלי זה הקריאה הראשונה
    להריץ החזירה 0 בכל שורה.
    """
    params = {'access_token': ACCESS_TOKEN, 'metric': metric}
    if endpoint == "insights":
        params['period'] = 'lifetime'
    try:
        res = http_get_json(f"https://graph.facebook.com/{API_VERSION}/{obj_id}/{endpoint}",
                            params=params, timeout=15, max_retries=2)
        if 'error' in res:
            return None
        data = res.get('data', [])
        values = data[0].get('values', []) if data else []
        return values[0].get('value') if values else None
    except Exception:
        return None


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
      views_30s       = total_video_30s_views — הוסר מ-v25, נשאר 0 לתאימות עמודות

    נוסף 26.7.2026 אחרי probe (fb_video_probe.py) שמנה את כל מה ש-video_insights
    מחזיר היום לרילז. total_video_30s_views אינו קיים יותר, ולכן completion_rate
    היה 0 בכל שורה מאז 6.12.2025 - אבל במקומו יש משהו טוב יותר:
      retention       = post_video_retention_graph — עקומת נטישה מלאה
      replays         = fb_reels_replay_count      — צפיות חוזרות
      total_plays     = fb_reels_total_plays       — כולל חוזרות
      duration_sec    = length של אובייקט הוידאו

    העקומה מחולקת למקטעים של ~0.9 שניות, עד תקרה של 41 - כלומר רילס של 20 שניות
    מקבל מקטע לשנייה וסרטון של 14 דקות מקבל מקטע ל-21 שניות. לכן "ההפרש בין
    המקטע הראשון לשני" מודד אורך ולא איכות (בכל הרילסים הקצרים הוא 99.7->99.8),
    והמדדים הנגזרים דוגמים בשנייה קבועה: retention_3s ו-retention_end.
    """
    result = {'plays': 0, 'avg_watch_sec': 0, 'views_30s': 0, 'total_watch_min': 0,
              'replays': 0, 'total_plays': 0, 'duration_sec': 0,
              'retention_3s': 0, 'retention_end': 0}
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

    # total_video_30s_views לא קיים יותר ב-v25 - הקריאה הוסרה, העמודה נשארת 0
    # כדי לא לשנות את מבנה הגיליון ההיסטורי

    result['replays'] = _insight(video_id, 'fb_reels_replay_count', endpoint='video_insights')
    result['total_plays'] = _insight(video_id, 'fb_reels_total_plays', endpoint='video_insights')

    length = _video_length(video_id)
    result['duration_sec'] = round(length, 1) if length else 0

    curve = _insight_raw(video_id, 'post_video_retention_graph', endpoint='video_insights')
    if isinstance(curve, dict) and curve:
        r3, rend = _retention_points(curve, length)
        result['retention_3s'] = r3
        result['retention_end'] = rend

    return result


def _video_length(video_id):
    """אורך הוידאו בשניות. נדרש כדי לדעת כמה שניות יש במקטע של העקומה."""
    try:
        res = http_get_json(f"https://graph.facebook.com/{API_VERSION}/{video_id}",
                            params={'access_token': ACCESS_TOKEN, 'fields': 'length'},
                            timeout=15, max_retries=2)
        return float(res.get('length') or 0)
    except Exception:
        return 0


def _retention_points(curve, length):
    """(אחוז שעדיין צפו בשנייה ה-3, אחוז שנשארו עד הסוף).

    השנייה ה-3 ולא "המקטע השני": רוחב המקטע תלוי באורך הסרטון, ולכן השוואה לפי
    מקטע היא השוואת אורכים. בלי אורך ידוע מחזירים 0 ל-3 השניות במקום לנחש.
    """
    try:
        keys = sorted(curve, key=lambda k: int(k))
    except (TypeError, ValueError):
        return 0, 0
    pts = [float(curve[k] or 0) for k in keys]
    if not pts:
        return 0, 0
    end = round(pts[-1] * 100, 1)
    if not length or len(pts) < 2:
        return 0, end
    per_bucket = length / len(pts)
    # ברזולוציה גסה מ-3 שניות, השנייה השלישית נופלת בתוך המקטע הראשון ואי אפשר
    # לקרוא אותה - מחזירים 0 ("לא נמדד") ולא 99.8% מטעה. בפועל המקטע הוא ~0.9
    # שניות בכל סרטון קצר מ-37 שניות, כלומר בכל הרילז.
    if per_bucket > 3.0:
        return 0, end
    idx = min(len(pts) - 1, max(0, int(round(3.0 / per_bucket))))
    return round(pts[idx] * 100, 1), end


REACTION_TYPES = ['LOVE', 'HAHA', 'WOW', 'SAD', 'ANGRY']
_EMPTY_PUBLIC = {'shares': 0, 'comments': 0, 'likes': 0,
                 'love': 0, 'haha': 0, 'wow': 0, 'sad': 0, 'angry': 0}


def get_public_metrics(post_id):
    """
    מדדים ציבוריים - לייקים (סך כל הריאקציות, כמו תמיד), תגובות, שיתופים,
    ופירוק ריאקציות (love/haha/wow/sad/angry) - אומת ב-probe שהפירוק מסתכם
    בדיוק לסך. פוסט עם 113 angry הוא סיפור מערכתי שונה מפוסט עם אותו מספר
    לייקים שכולם love.
    """
    url = f"https://graph.facebook.com/{API_VERSION}/{post_id}"
    reaction_fields = ','.join(
        f"reactions.type({t}).limit(0).summary(total_count).as(r_{t.lower()})"
        for t in REACTION_TYPES
    )
    params = {
        'access_token': ACCESS_TOKEN,
        'fields': 'shares,comments.summary(true).limit(0),reactions.summary(true).limit(0),' + reaction_fields
    }
    try:
        res = http_get_json(url, params=params, timeout=15, max_retries=2)
        likes = 0
        if 'reactions' in res and 'summary' in res['reactions']:
            likes = res['reactions']['summary']['total_count']

        out = {
            'shares': res.get('shares', {}).get('count', 0),
            'comments': res.get('comments', {}).get('summary', {}).get('total_count', 0),
            'likes': likes,
        }
        for t in REACTION_TYPES:
            key = t.lower()
            out[key] = (res.get(f'r_{key}') or {}).get('summary', {}).get('total_count', 0)
        return out
    except:
        return dict(_EMPTY_PUBLIC)


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
            # פוסט שאינו וידאו - אותן מפתחות בדיוק, כדי שהשורה תיבנה תמיד.
            # שני מקומות שמייצרים את אותו דיקט הם בדיוק הסוג של הכפילות
            # שנשברת כשמוסיפים שדה, ולכן ברירת המחדל נלקחת מהפונקציה עצמה.
            video = get_video_insights(None)
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
                # 700 ולא 500: חתימת "כאן חדשות ברשת ב'" יושבת בסוף הכיתוב,
                # וב-500 היא נחתכת בפוסטים ארוכים (נמדד: החתימה עד תו ~490)
                'title': (post.get('message', '') or '').replace('\n', ' ')[:700],
                'reach': reach,
                'clicks': clicks,
                'views': views,
                'views_30s': video['views_30s'],
                'total_watch_min': video['total_watch_min'],
                'avg_watch_sec': video['avg_watch_sec'],
                'completion_rate': completion_rate,
                'duration_sec': video['duration_sec'],
                'retention_3s': video['retention_3s'],
                'retention_end': video['retention_end'],
                'replays': video['replays'],
                'total_plays': video['total_plays'],
                'likes': public['likes'],
                'love': public['love'],
                'haha': public['haha'],
                'wow': public['wow'],
                'sad': public['sad'],
                'angry': public['angry'],
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
                  'shares', 'total_engagement', 'engagement_rate',
                  'love', 'haha', 'wow', 'sad', 'angry',
                  'duration_sec', 'retention_3s', 'retention_end',
                  'replays', 'total_plays']
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
