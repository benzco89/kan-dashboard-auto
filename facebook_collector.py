import os
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time
import json

# --- Config ---
# הותאם לשם שאתה רגיל אליו
ACCESS_TOKEN = os.environ.get('FACEBOOK_TOKEN') 
PAGE_ID = "220634478361516"
API_VERSION = "v24.0"
DAYS_BACK = 3  # לאוטומציה יומית

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET_NAME = "נתוני פייסבוק"

# --- Functions (הלוגיקה שעבדה לך ב-100%) ---

def get_video_direct_metrics(video_id):
    """משיכת צפיות ישירות מאובייקט הוידאו"""
    if not video_id: return 0
    url = f"https://graph.facebook.com/{API_VERSION}/{video_id}"
    params = {'access_token': ACCESS_TOKEN, 'fields': 'views'}
    try:
        res = requests.get(url, params=params).json()
        return res.get('views', 0)
    except: return 0

def get_post_insights(post_id, media_type):
    # הסרנו את views_10s הבעייתי
    base_metrics = "post_impressions,post_impressions_unique,post_engaged_users"
    video_metrics = "blue_reels_play_count,post_video_avg_time_watched,post_video_view_time"
    
    metrics = f"{base_metrics},{video_metrics}" 
    
    url = f"https://graph.facebook.com/{API_VERSION}/{post_id}/insights"
    params = {'access_token': ACCESS_TOKEN, 'metric': metrics, 'period': 'lifetime'}
    
    result = {'reach': 0, 'clicks': 0, 'views': 0, 'avg_watch_sec': 0, 'total_watch_min': 0, 'impressions': 0}
    
    try:
        res = requests.get(url, params=params).json()
        
        # Fallback
        if 'error' in res:
             params['metric'] = base_metrics
             res = requests.get(url, params=params).json()

        if 'data' in res:
            for item in res['data']:
                name = item['name']
                v = 0
                if 'values' in item and len(item['values']) > 0:
                    v = item['values'][0]['value']
                
                if name == 'post_impressions_unique': result['reach'] = v
                elif name == 'post_impressions': result['impressions'] = v
                elif name == 'blue_reels_play_count': result['views'] = v
                elif name == 'post_video_avg_time_watched': result['avg_watch_sec'] = round(v/1000, 1)
                elif name == 'post_video_view_time': result['total_watch_min'] = round(v/1000/60, 1)
                
    except: pass
    return result

def get_public_metrics(post_id):
    url = f"https://graph.facebook.com/{API_VERSION}/{post_id}"
    params = {'access_token': ACCESS_TOKEN, 'fields': 'shares,comments.summary(true).limit(0),reactions.summary(true).limit(0)'}
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
    permalink = post.get('permalink_url', '')
    if '/reel/' in permalink: return 'Reel'
    if '/videos/' in permalink: return 'Video'
    
    if 'attachments' in post and 'data' in post['attachments']:
        att = post['attachments']['data'][0]
        att_type = att.get('type', '')
        url = att.get('url', '')
        
        if 'reel' in url or 'reel' in att.get('target', {}).get('url', ''): return 'Reel'
        if att_type in ['video_inline', 'video_direct', 'video_autoplay', 'video']: return 'Video'
        if att_type in ['photo', 'cover_photo', 'album']: return 'Photo'
        
    return 'Link'

def get_video_id_from_post(post):
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
    params = {'access_token': ACCESS_TOKEN, 'limit': 25, 
              'fields': 'id,created_time,message,permalink_url,attachments', 'since': since_unix}
    
    while True:
        res = requests.get(url, params=params).json()
        if 'error' in res:
            print(f"❌ API Error: {res['error']['message']}")
            break
        if 'data' not in res or not res['data']: break
        
        for post in res['data']:
            post_id = post['id']
            media_type = detect_media_type(post)
            insights = get_post_insights(post_id, media_type)
            public = get_public_metrics(post_id)
            
            views = insights['views']
            if views == 0 and media_type in ['Video', 'Reel']:
                vid_id = get_video_id_from_post(post)
                if vid_id:
                    views = get_video_direct_metrics(vid_id)
            
            impressions = insights['impressions']
            if impressions == 0: impressions = insights['reach']
            if impressions == 0 and views > 0: impressions = views

            total_eng = public['likes'] + public['comments'] + public['shares']
            eng_rate = round((total_eng / insights['reach']) * 100, 2) if insights['reach'] > 0 else 0
            
            all_posts.append({
                'post_id': post_id,
                'date': post['created_time'][:10],
                'time': post['created_time'][11:16],
                'type': media_type,
                'title': (post.get('message', '') or '').replace('\n', ' ')[:200], # בלי מגבלה קצרה מדי
                'reach': insights['reach'], 
                'impressions': impressions,
                'views': views, 
                'avg_watch_sec': insights['avg_watch_sec'],
                'likes': public['likes'], 
                'comments': public['comments'],
                'shares': public['shares'], 
                'total_engagement': total_eng, 
                'engagement_rate': eng_rate,
                'permalink': post.get('permalink_url', ''), 
                'pulled_at': datetime.now().strftime('%Y-%m-%d %H:%M')
            })
            time.sleep(0.15)
        
        if 'paging' in res and 'next' in res['paging']:
            url = res['paging']['next']
            params = {}
        else: break
    
    return pd.DataFrame(all_posts)

# --- פונקציית השמירה החכמה (Upsert) ---
def save_to_sheets(new_df):
    # שימוש ב-GCP_SERVICE_ACCOUNT (כי זה מה שיש לנו בגיטהאב שעובד)
    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT') 
    
    # במקרה שאתה מריץ מקומית ויש לך משתנה אחר
    if not creds_json: 
        creds_json = os.environ.get('GOOGLE_CREDENTIALS')

    creds_dict = json.loads(creds_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    
    try: worksheet = sh.worksheet(SHEET_NAME)
    except: worksheet = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=25)
    
    # 1. קריאת היסטוריה
    existing_data = worksheet.get_all_records()
    existing_df = pd.DataFrame(existing_data)
    
    # 2. מיזוג
    if not existing_df.empty:
        new_df['post_id'] = new_df['post_id'].astype(str)
        existing_df['post_id'] = existing_df['post_id'].astype(str)
        
        # חישוב דלתא (כמה צפיות נוספו היום)
        if 'views' in existing_df.columns:
            # מנקים וממירים למספר
            existing_df['views'] = pd.to_numeric(existing_df['views'], errors='coerce').fillna(0)
            view_map = existing_df.set_index('post_id')['views'].to_dict()
            new_df['views_delta'] = new_df.apply(lambda x: x['views'] - view_map.get(x['post_id'], x['views']), axis=1)
        else:
            new_df['views_delta'] = 0

        for col in new_df.columns:
            if col not in existing_df.columns: existing_df[col] = ""
            
        combined = pd.concat([new_df, existing_df])
        final_df = combined.drop_duplicates(subset=['post_id'], keep='first')
        print(f"🔄 Merged: {len(new_df)} new + {len(existing_df)} existing -> {len(final_df)} total")
    else:
        new_df['views_delta'] = 0
        final_df = new_df
    
    # 3. ניקוי ומיון
    final_df = final_df.sort_values(by='date', ascending=False)
    final_df = final_df.fillna(0).replace([float('inf'), float('-inf')], 0)
    
    # 4. שמירה
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
