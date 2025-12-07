import os
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time
import json

# Load .env file if exists (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# --- Config ---
ACCESS_TOKEN = os.environ.get('FACEBOOK_TOKEN')
PAGE_ID = "220634478361516"
API_VERSION = "v24.0"
DAYS_BACK = 3  # לאוטומציה יומית

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET_NAME = "נתוני פייסבוק"

# --- Functions ---

def get_video_direct_metrics(video_id):
    """משיכת צפיות ישירות מאובייקט הוידאו"""
    if not video_id:
        return 0
    url = f"https://graph.facebook.com/{API_VERSION}/{video_id}"
    params = {'access_token': ACCESS_TOKEN, 'fields': 'views'}
    try:
        res = requests.get(url, params=params).json()
        return res.get('views', 0)
    except:
        return 0


def get_post_insights(post_id, media_type):
    """
    משיכת מדדי insights לפוסט - עובד לכל סוגי הפוסטים.
    מחזיר:
      - reach: reach אמיתי אם קיים (בלי חישוב סינתטי)
      - impressions
      - clicks
      - engaged_users
      - views: צפיות וידאו/רילס (blue_reels_play_count / וידאו)
      - media_views: post_media_view – תצוגות לכל סוגי הפוסטים
      - avg_watch_sec
      - total_watch_min
      - views_30s: צפיות של 30+ שניות (רק וידאו/רילס)
    """
    # מדדים בסיסיים - עובדים לכל סוגי הפוסטים (תמונות, לינקים)
    # הערה: post_media_view ו-post_engaged_users לא עובדים לתמונות ב-New Page Experience
    if media_type in ['Video', 'Reel']:
        base_metrics = ",".join([
            "post_impressions",
            "post_impressions_unique",
            "post_engaged_users",
            "post_clicks",
            "post_media_view",
        ])
    else:
        # Photos - using metrics from meta_api.md reference
        base_metrics = ",".join([
            "post_impressions",
            "post_impressions_unique",  # This is reach
            "post_consumptions",        # Total interactions
        ])

    # מדדי וידאו - רק לוידאו ורילס
    video_metrics = ",".join([
        "blue_reels_play_count",
        "post_video_avg_time_watched",
        "post_video_view_time",
        "post_video_complete_views_30s",
    ])

    result = {
        'reach': 0,
        'impressions': 0,
        'clicks': 0,
        'engaged_users': 0,
        'views': 0,
        'media_views': 0,
        'avg_watch_sec': 0,
        'total_watch_min': 0,
        'views_30s': 0
    }

    # שלב 1: שליפת מדדים בסיסיים (לכל סוגי הפוסטים)
    url = f"https://graph.facebook.com/{API_VERSION}/{post_id}/insights"
    params = {
        'access_token': ACCESS_TOKEN,
        'metric': base_metrics,
        'period': 'lifetime'
    }

    try:
        res = requests.get(url, params=params).json()
        
        # Debug: הדפסת תוכן התגובה לפוסטים מסוג Photo
        if media_type == 'Photo':
            if 'error' in res:
                print(f"❌ Photo insights error for {post_id}: {res['error'].get('message', 'Unknown')}")
            elif not res.get('data'):
                print(f"⚠️ Photo insights empty for {post_id}")
        
        data = res.get('data', [])
        for item in data:
            name = item.get('name')
            values = item.get('values', [])
            v = values[0].get('value', 0) if values else 0

            if name == 'post_impressions_unique':
                result['reach'] = v
            elif name == 'post_impressions':
                result['impressions'] = v
            elif name == 'post_engaged_users':
                result['engaged_users'] = v
            elif name == 'post_clicks':
                result['clicks'] = v
            elif name == 'post_consumptions':
                # For photos - total clicks/interactions
                if result['clicks'] == 0:
                    result['clicks'] = v
            elif name == 'post_media_view':
                result['media_views'] = v
    except Exception as e:
        print(f"Error fetching base metrics for {post_id}: {e}")

    # שלב 2: שליפת מדדי וידאו (רק לוידאו ורילס)
    if media_type in ['Video', 'Reel']:
        params_video = {
            'access_token': ACCESS_TOKEN,
            'metric': video_metrics,
            'period': 'lifetime'
        }
        try:
            res_video = requests.get(url, params=params_video).json()
            data_video = res_video.get('data', [])
            for item in data_video:
                name = item.get('name')
                values = item.get('values', [])
                v = values[0].get('value', 0) if values else 0

                if name == 'blue_reels_play_count':
                    result['views'] = v
                elif name == 'post_video_avg_time_watched':
                    # המטריקה במילישניות – ממירים לשניות
                    result['avg_watch_sec'] = round(v / 1000, 1) if v else 0
                elif name == 'post_video_view_time':
                    # ממילישניות לדקות
                    result['total_watch_min'] = round(v / 1000 / 60, 1) if v else 0
                elif name == 'post_video_complete_views_30s':
                    result['views_30s'] = v
        except Exception as e:
            print(f"Error fetching video metrics for {post_id}: {e}")

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


def get_video_id_from_post(post):
    """חילוץ video_id מפוסט"""
    if 'attachments' in post and 'data' in post['attachments']:
        att = post['attachments']['data'][0]
        if 'target' in att and 'id' in att['target']:
            return att['target']['id']
    return None


def fetch_facebook_data():
    print(f"🚀 Facebook Collector - {datetime.now()}")

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

            # משיכת כל המדדים
            insights = get_post_insights(post_id, media_type)
            public = get_public_metrics(post_id)

            # עבור וידאו/רילס - fallback לצפיות ישירות
            views = insights.get('views', 0)
            if views == 0 and media_type in ['Video', 'Reel']:
                vid_id = get_video_id_from_post(post)
                if vid_id:
                    views = get_video_direct_metrics(vid_id)

            media_views = insights.get('media_views', 0)

            # views_all: מדד תצוגות אחיד לכל סוגי הפוסטים
            if media_type in ['Video', 'Reel']:
                views_all = views or media_views
            else:
                views_all = media_views

            # --- חישוב impressions עם fallbackים ---
            impressions = insights.get('impressions', 0)

            # אם אין impressions אבל יש reach מה-API – נשתמש בו
            if impressions == 0 and insights.get('reach', 0) > 0:
                impressions = insights['reach']

            # אם עדיין 0 ויש views_all – נניח שהם משקפים חשיפה
            if impressions == 0 and views_all:
                impressions = views_all

            # --- חישוב reach "מחושב" (synthetic) ---
            reach_val = insights.get('reach', 0)
            if reach_val == 0:
                if impressions > 0:
                    reach_val = impressions
                elif views_all:
                    reach_val = views_all

            # --- חישוב engagement ---
            total_eng = (
                public.get('likes', 0)
                + public.get('comments', 0)
                + public.get('shares', 0)
            )

            # נחשב rate לפי reach המחושב; אם גם הוא 0 – נשתמש ב-impressions
            denom = reach_val or impressions
            eng_rate = round((total_eng / denom) * 100, 2) if denom > 0 else 0

            all_posts.append({
                'post_id': post_id,
                'date': post['created_time'][:10],
                'time': post['created_time'][11:16],
                'type': media_type,
                'title': (post.get('message', '') or '').replace('\n', ' ')[:500],
                'reach': reach_val,
                'impressions': impressions,
                'clicks': insights.get('clicks', 0),
                'engaged_users': insights.get('engaged_users', 0),
                'views': views,
                'media_views': media_views,
                'avg_watch_sec': insights.get('avg_watch_sec', 0),
                'total_watch_min': insights.get('total_watch_min', 0),
                'views_30s': insights.get('views_30s', 0),
                'likes': public.get('likes', 0),
                'comments': public.get('comments', 0),
                'shares': public.get('shares', 0),
                'total_engagement': total_eng,
                'engagement_rate': eng_rate,
                'permalink': post.get('permalink_url', ''),
                'pulled_at': datetime.now().strftime('%Y-%m-%d %H:%M')
            })
            time.sleep(0.15)

        if 'paging' in res and 'next' in res['paging']:
            url = res['paging']['next']
            params = {}
        else:
            break

    return pd.DataFrame(all_posts)


def save_to_sheets(new_df):
    """שמירה חכמה לגוגל שיטס עם מיזוג נתונים"""
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
        # תיקון: 25 עמודות (23 מהנתונים + 2 עתיד) במקום 30
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
            existing_df['views'] = pd.to_numeric(
                existing_df['views'], errors='coerce'
            ).fillna(0)
            view_map = existing_df.set_index('post_id')['views'].to_dict()
            new_df['views_delta'] = new_df.apply(
                lambda x: x['views'] - view_map.get(x['post_id'], x['views']),
                axis=1
            )
        else:
            new_df['views_delta'] = 0

        # חישוב דלתא ל-reach
        if 'reach' in existing_df.columns:
            existing_df['reach'] = pd.to_numeric(
                existing_df['reach'], errors='coerce'
            ).fillna(0)
            reach_map = existing_df.set_index('post_id')['reach'].to_dict()
            new_df['reach_delta'] = new_df.apply(
                lambda x: x['reach'] - reach_map.get(x['post_id'], x['reach']),
                axis=1
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

    # ניקוי ומיון
    final_df = final_df.sort_values(by='date', ascending=False)
    final_df = final_df.fillna(0).replace([float('inf'), float('-inf')], 0)

    # שמירה
    worksheet.clear()
    worksheet.update([final_df.columns.tolist()] + final_df.values.tolist())
    print(f"✅ Saved {len(final_df)} rows to {SHEET_NAME}")


if __name__ == "__main__":
    df = fetch_facebook_data()
    if not df.empty:
        save_to_sheets(df)
        print(f"✅ Done! {len(df)} posts processed.")
    else:
        print("❌ No data collected.")
