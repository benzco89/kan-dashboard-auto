import os
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time
import json
import pytz
import re

# Load .env file if exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Config ---
ACCESS_TOKEN = os.environ.get('FACEBOOK_TOKEN')
PAGE_ID = "220634478361516"
API_VERSION = "v24.0"
DAYS_BACK = 16

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
        res = requests.get(url, params=params).json()
        return res.get('views', 0)
    except:
        return 0

def get_negative_feedback_safe(post_id):
    """משיכת פידבק שלילי בנפרד כדי לא להכשיל את שאר הנתונים"""
    try:
        url = f"https://graph.facebook.com/{API_VERSION}/{post_id}/insights"
        res = requests.get(url, params={'access_token': ACCESS_TOKEN, 'metric': 'post_negative_feedback', 'period': 'lifetime'}).json()
        if 'data' in res and res['data']:
            return res['data'][0]['values'][0]['value']
    except:
        pass
    return 0

def get_post_insights(post_id, media_type):
    """
    משיכת מדדי insights - גרסה מתוקנת (ללא המדדים שמפילים את הבקשה)
    """
    # === התיקון הקריטי כאן ===
    # הסרנו את post_impressions ואת post_engaged_users שגרמו לשגיאה 400
    
    if media_type in ['Video', 'Reel']:
        metrics = ",".join([
            "post_impressions_unique",      # Reach
            "post_clicks",                  # Clicks
            "blue_reels_play_count",        # Views (Reels)
            "post_video_avg_time_watched",  # Watch Time
            "post_media_view"               # Views (Fallback)
        ])
    else:
        # לתמונות וסטטוסים - נשארים רק עם מה שעובד בוודאות
        metrics = ",".join([
            "post_impressions_unique",      # Reach
            "post_clicks"                   # Clicks
        ])

    result = {
        'reach': 0,
        'impressions': 0,
        'clicks': 0,
        'views': 0,
        'avg_watch_sec': 0,
    }

    url = f"https://graph.facebook.com/{API_VERSION}/{post_id}/insights"
    params = {
        'access_token': ACCESS_TOKEN,
        'metric': metrics,
        'period': 'lifetime'
    }

    try:
        res = requests.get(url, params=params).json()
        
        # אם עדיין יש שגיאה, נדפיס אותה אבל לא נתרסק
        if 'error' in res:
            print(f"⚠️ API Error for {post_id} ({media_type}): {res['error'].get('message')}")
        
        data = res.get('data', [])
        for item in data:
            name = item.get('name')
            values = item.get('values', [])
            v = values[0].get('value', 0) if values else 0

            if name == 'post_impressions_unique':
                result['reach'] = v
            # post_impressions הוסר כי הוא גורם לשגיאה, נשתמש ב-reach כקירוב
            elif name == 'post_clicks':
                result['clicks'] = v
            elif name == 'blue_reels_play_count':
                result['views'] = v
            elif name == 'post_media_view' and result['views'] == 0:
                result['views'] = v
            elif name == 'post_video_avg_time_watched':
                result['avg_watch_sec'] = round(v / 1000, 1) if v else 0

    except Exception as e:
        print(f"❌ Exception fetching insights for {post_id}: {e}")

    return result

def get_public_metrics(post_id):
    """משיכת מדדים ציבוריים - לייקים, תגובות, שיתופים"""
    url = f"https://graph.facebook.com/{API_VERSION}/{post_id}"
    params = {
        'access_token': ACCESS_TOKEN,
        'fields': 'shares,comments.summary(true).limit(0),reactions.summary(true).limit(0)'
    }
    try:
        res = requests.get(url, params=params).json()
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

    # משיכת נתונים אחורה
    since_unix = int((datetime.now() - timedelta(days=DAYS_BACK)).timestamp())
    all_posts = []

    url = f"https://graph.facebook.com/{API_VERSION}/{PAGE_ID}/feed"
    params = {
        'access_token': ACCESS_TOKEN,
        'limit': 25,
        'fields': 'id,created_time,message,permalink_url,attachments',
        'since': since_unix
    }

    while True:
        res = requests.get(url, params=params).json()
        
        if 'error' in res:
            print(f"❌ API Error: {res['error']['message']}")
            break
            
        if 'data' not in res or not res['data']:
            break

        for post in res['data']:
            post_id = post['id']
            media_type = detect_media_type(post)

            # 1. משיכת נתונים רגילים
            insights = get_post_insights(post_id, media_type)
            public = get_public_metrics(post_id)
            
            # 2. משיכת נתונים שליליים (בזהירות)
            neg_feedback = get_negative_feedback_safe(post_id)

            # 3. תיקוני נתונים
            views = insights.get('views', 0)
            if views == 0 and media_type in ['Video', 'Reel']:
                try:
                    if 'attachments' in post:
                        vid_id = post['attachments']['data'][0]['target']['id']
                        views = get_video_direct_metrics(vid_id)
                except:
                    pass

            reach = insights.get('reach', 0)
            # אם אין impressions (כי הסרנו את המדד), נשתמש ב-reach כברירת מחדל
            impressions = insights.get('impressions', 0) or reach
            if reach == 0:
                reach = impressions or views

            # 4. חישוב מדד מעורבות משוקלל
            clicks = insights.get('clicks', 0)
            total_eng = (
                clicks +
                public['likes'] +
                public['comments'] +
                public['shares']
            )
            
            engagement_rate = 0
            if reach > 0:
                engagement_rate = round((total_eng / reach) * 100, 2)

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
                'impressions': impressions,
                'clicks': clicks,
                'engaged_users': total_eng,
                'negative_feedback': neg_feedback,
                'views': views,
                'avg_watch_sec': insights.get('avg_watch_sec', 0),
                'likes': public['likes'],
                'comments': public['comments'],
                'shares': public['shares'],
                'total_engagement': total_eng,
                'engagement_rate': engagement_rate,
                'permalink': post.get('permalink_url', ''),
                'pulled_at': datetime.now(il_tz).strftime('%Y-%m-%d %H:%M')
            })
            
            time.sleep(0.15)

        if 'paging' in res and 'next' in res['paging']:
            url = res['paging']['next']
            params = {}
        else:
            break

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

        # חישוב דלתא לצפיות
        if 'views' in existing_df.columns:
            existing_df['views'] = pd.to_numeric(existing_df['views'], errors='coerce').fillna(0)
            view_map = existing_df.set_index('post_id')['views'].to_dict()
            new_df['views_delta'] = new_df.apply(
                lambda x: x['views'] - view_map.get(x['post_id'], x['views']), axis=1
            )
        else:
            new_df['views_delta'] = 0

        # וידוא עמודות
        for col in new_df.columns:
            if col not in existing_df.columns:
                existing_df[col] = ""

        combined = pd.concat([new_df, existing_df])
        final_df = combined.drop_duplicates(subset=['post_id'], keep='first')
        print(f"🔄 Merged: {len(new_df)} new + {len(existing_df)} existing -> {len(final_df)} total")
    else:
        new_df['views_delta'] = 0
        final_df = new_df

    # ניקוי ומיון
    final_df = final_df.sort_values(by='date', ascending=False)
    final_df = final_df.fillna(0).replace([float('inf'), float('-inf')], 0)

    # שמירה
    worksheet.clear()
    worksheet.update([final_df.columns.tolist()] + final_df.values.tolist())
    print(f"✅ Saved {len(final_df)} rows to {SHEET_NAME}")

def main():
    if not ACCESS_TOKEN:
        print("❌ Missing FACEBOOK_TOKEN environment variable")
        return

    df = fetch_facebook_data()
    if not df.empty:
        save_to_sheets(df)
        print(f"✅ Done! {len(df)} posts processed.")
    else:
        print("❌ No data collected.")

if __name__ == "__main__":
    main()
