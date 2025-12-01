import os
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time
import json

# --- Config ---
ACCESS_TOKEN = os.environ.get('FACEBOOK_TOKEN')
PAGE_ID = "220634478361516"
API_VERSION = "v24.0"
DAYS_BACK = 3  # יומי - שבוע אחורה

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET_NAME = "Facebook"

# --- Functions (same as before) ---

def get_post_insights(post_id, media_type):
    base_metrics = "post_impressions_unique,post_clicks,post_reactions_by_type_total"
    video_metrics = "post_video_views,post_video_avg_time_watched,post_video_view_time"
    metrics = f"{base_metrics},{video_metrics}" if media_type in ['Video', 'Reel'] else base_metrics
    
    url = f"https://graph.facebook.com/{API_VERSION}/{post_id}/insights"
    params = {'access_token': ACCESS_TOKEN, 'metric': metrics}
    
    result = {'reach': 0, 'clicks': 0, 'views': 0, 'avg_watch_sec': 0, 'total_watch_min': 0,
              'like': 0, 'love': 0, 'wow': 0, 'haha': 0, 'sorry': 0, 'anger': 0}
    
    try:
        res = requests.get(url, params=params).json()
        if 'data' in res:
            for item in res['data']:
                if item.get('period') != 'lifetime':
                    continue
                name = item['name']
                v = item['values'][0]['value']
                
                if name == 'post_impressions_unique': result['reach'] = v
                elif name == 'post_clicks': result['clicks'] = v
                elif name == 'post_video_views': result['views'] = v
                elif name == 'post_video_avg_time_watched': result['avg_watch_sec'] = round(v/1000, 1)
                elif name == 'post_video_view_time': result['total_watch_min'] = round(v/1000/60, 1)
                elif name == 'post_reactions_by_type_total' and isinstance(v, dict):
                    for k in ['like', 'love', 'wow', 'haha', 'sorry', 'anger']:
                        result[k] = v.get(k, 0)
    except: pass
    return result

def get_public_metrics(post_id):
    url = f"https://graph.facebook.com/{API_VERSION}/{post_id}"
    params = {'access_token': ACCESS_TOKEN, 'fields': 'shares,comments.summary(true).limit(0),reactions.summary(true).limit(0)'}
    try:
        res = requests.get(url, params=params).json()
        return {
            'total_reactions': res.get('reactions', {}).get('summary', {}).get('total_count', 0),
            'comments': res.get('comments', {}).get('summary', {}).get('total_count', 0),
            'shares': res.get('shares', {}).get('count', 0)
        }
    except:
        return {'total_reactions': 0, 'comments': 0, 'shares': 0}

def detect_media_type(post):
    permalink = post.get('permalink_url', '')
    if '/reel/' in permalink: return 'Reel'
    if '/videos/' in permalink: return 'Video'
    if 'attachments' in post and 'data' in post['attachments']:
        att_type = post['attachments']['data'][0].get('type', '')
        if 'video' in att_type: return 'Video'
        if att_type in ['photo', 'cover_photo', 'album']: return 'Photo'
    return 'Link'

def fetch_facebook_data():
    print(f"🚀 Facebook Collector - {datetime.now()}")
    
    since_unix = int((datetime.now() - timedelta(days=DAYS_BACK)).timestamp())
    all_posts = []
    
    url = f"https://graph.facebook.com/{API_VERSION}/{PAGE_ID}/feed"
    params = {'access_token': ACCESS_TOKEN, 'limit': 50, 
              'fields': 'id,created_time,message,permalink_url,attachments', 'since': since_unix}
    
    while True:
        res = requests.get(url, params=params).json()
        if 'error' in res:
            print(f"❌ {res['error']['message']}")
            break
        if 'data' not in res or not res['data']: break
        
        for post in res['data']:
            post_id = post['id']
            media_type = detect_media_type(post)
            insights = get_post_insights(post_id, media_type)
            public = get_public_metrics(post_id)
            
            total_eng = public['total_reactions'] + public['comments'] + public['shares']
            eng_rate = round((total_eng / insights['reach']) * 100, 2) if insights['reach'] > 0 else 0
            
            all_posts.append({
                'post_id': post_id,
                'date': post['created_time'][:10],
                'time': post['created_time'][11:16],
                'type': media_type,
                'title': (post.get('message', '') or '').split('\n')[0][:80],
                'reach': insights['reach'], 'clicks': insights['clicks'],
                'views': insights['views'], 'avg_watch_sec': insights['avg_watch_sec'],
                'like': insights['like'], 'love': insights['love'], 'wow': insights['wow'],
                'haha': insights['haha'], 'sorry': insights['sorry'], 'anger': insights['anger'],
                'total_reactions': public['total_reactions'], 'comments': public['comments'],
                'shares': public['shares'], 'total_engagement': total_eng, 'engagement_rate': eng_rate,
                'permalink': post.get('permalink_url', ''), 'pulled_at': datetime.now().strftime('%Y-%m-%d %H:%M')
            })
            time.sleep(0.15)
        
        if 'paging' in res and 'next' in res['paging']:
            url = res['paging']['next']
            params = {}
        else: break
    
    return pd.DataFrame(all_posts)

def save_to_sheets(df):
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    
    try:
        worksheet = sh.worksheet(SHEET_NAME)
    except:
        worksheet = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=25)
    
    worksheet.clear()
    worksheet.update([df.columns.tolist()] + df.values.tolist())
    print(f"✅ Saved {len(df)} rows to {SHEET_NAME}")

# --- Main ---
if __name__ == "__main__":
    df = fetch_facebook_data()
    if not df.empty:
        save_to_sheets(df)
        print(f"✅ Done! {len(df)} posts")
