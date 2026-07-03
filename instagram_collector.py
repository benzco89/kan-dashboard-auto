"""
Instagram Collector - איסוף נתוני פוסטים ורילסים מאינסטגרם
משתמש ב-Instagram Graph API דרך Facebook Token
"""

import os
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time
import json
import re  # for timestamp parsing
import pytz  # for Israel timezone

from utils import http_get_json, backfill_zero_metrics

# Load .env file if exists (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Config ---
ACCESS_TOKEN = os.environ.get('FACEBOOK_TOKEN')
API_VERSION = "v24.0"

# ימים אחורה: 16 להרצה ראשונה, 3 לאוטומציה יומית
# לשנות ל-3 אחרי ההרצה הראשונה
DAYS_BACK = 7

SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET_NAME = "נתוני אינסטגרם"

# --- Functions ---

def get_instagram_account_id():
    """משיכת ה-Instagram Business Account ID מהדף המחובר"""
    
    # נסיון 1: אם יש לנו Page Token, ננסה לשלוף ישירות את ה-IG account
    # קודם נגלה את ה-Page ID מה-token
    url = f"https://graph.facebook.com/{API_VERSION}/me"
    params = {
        'access_token': ACCESS_TOKEN,
        'fields': 'id,name,instagram_business_account'
    }
    
    try:
        res = http_get_json(url, params=params)

        if 'error' in res:
            print(f"❌ Error: {res['error']['message']}")
            return None
        
        # בדיקה אם יש לנו Instagram Business Account ישירות
        ig_account = res.get('instagram_business_account')
        if ig_account:
            print(f"✅ Found Instagram account: {ig_account['id']} (Page: {res.get('name', 'Unknown')})")
            return ig_account['id']
        
        # אם לא, ננסה לחפש דרך accounts (User Token)
        page_id = res.get('id')
        if page_id:
            # ננסה לשלוף את ה-Instagram account מה-Page
            page_url = f"https://graph.facebook.com/{API_VERSION}/{page_id}"
            page_params = {
                'access_token': ACCESS_TOKEN,
                'fields': 'instagram_business_account'
            }
            page_res = http_get_json(page_url, params=page_params)
            
            ig_account = page_res.get('instagram_business_account')
            if ig_account:
                print(f"✅ Found Instagram account: {ig_account['id']}")
                return ig_account['id']
        
        print("❌ No Instagram Business Account found.")
        print("   Make sure your Instagram is connected to the Facebook Page.")
        print(f"   Token is for: {res.get('name', 'Unknown')} (ID: {res.get('id', 'Unknown')})")
        return None
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def get_media_insights(media_id, media_type):
    """
    משיכת מדדי insights למדיה (פוסט/רילס)
    עודכן לתמיכה ב-API v24 (views במקום plays הישן)
    """
    # מדדים לפי סוג מדיה - עודכן לגרסה 24
    if media_type == 'VIDEO' or media_type == 'REELS':
        metrics = [
            'views',              # צפיות (החליף את plays)
            'reach',              # משתמשים ייחודיים
            'saved',              # שמירות
            'shares',             # שיתופים
            'total_interactions', # סה"כ אינטראקציות
            'ig_reels_avg_watch_time',  # זמן צפייה ממוצע (ms)
        ]
    elif media_type == 'CAROUSEL_ALBUM':
        metrics = [
            'views',
            'reach',
            'saved',
            'shares',
            'total_interactions',
        ]
    else:  # IMAGE
        metrics = [
            'views',
            'reach',
            'saved',
            'shares',
            'total_interactions',
        ]
    
    url = f"https://graph.facebook.com/{API_VERSION}/{media_id}/insights"
    params = {
        'access_token': ACCESS_TOKEN,
        'metric': ','.join(metrics)
    }
    
    result = {
        'views': 0,
        'reach': 0,
        'saved': 0,
        'shares': 0,
        'total_interactions': 0,
        'avg_watch_sec': 0,
    }
    
    try:
        res = http_get_json(url, params=params)

        if 'error' in res:
            # הדפסת השגיאה כדי להבין מה לא עובד
            print(f"⚠️ Insights error for {media_id}: {res['error'].get('message', 'Unknown error')}")
            return result
        
        for item in res.get('data', []):
            name = item.get('name')
            values = item.get('values', [])
            v = values[0].get('value', 0) if values else 0
            
            if name == 'views':
                result['views'] = v
            elif name == 'reach':
                result['reach'] = v
            elif name == 'saved':
                result['saved'] = v
            elif name == 'shares':
                result['shares'] = v
            elif name == 'total_interactions':
                result['total_interactions'] = v
            elif name == 'ig_reels_avg_watch_time':
                result['avg_watch_sec'] = round(v / 1000, 2) if v else 0
                
    except Exception as e:
        print(f"⚠️ Error fetching insights for {media_id}: {e}")
    
    return result


def fetch_instagram_media(ig_account_id):
    """משיכת פוסטים ורילסים מאינסטגרם"""
    print(f"🚀 Instagram Collector - Fetching last {DAYS_BACK} days")
    
    since_date = datetime.now(pytz.timezone('Asia/Jerusalem')) - timedelta(days=DAYS_BACK)
    since_unix = int(since_date.timestamp())
    
    all_media = []
    
    # שליפת מדיה
    url = f"https://graph.facebook.com/{API_VERSION}/{ig_account_id}/media"
    params = {
        'access_token': ACCESS_TOKEN,
        'fields': 'id,caption,media_type,media_url,permalink,thumbnail_url,timestamp,like_count,comments_count',
        'limit': 50,
    }
    
    while True:
        try:
            res = http_get_json(url, params=params)
        except Exception as e:
            print(f"❌ Media request failed after retries: {e} - keeping {len(all_media)} items fetched so far")
            break

        if 'error' in res:
            print(f"❌ API Error: {res['error']['message']}")
            break
            
        if 'data' not in res or not res['data']:
            break
        
        for media in res['data']:
            # בדיקת תאריך
            timestamp = media.get('timestamp', '')
            if timestamp:
                # Handle both '+0000' and 'Z' formats
                ts_normalized = re.sub(r'\+0000$', '+00:00', timestamp.replace('Z', '+00:00'))
                media_date = datetime.fromisoformat(ts_normalized)
                if media_date.timestamp() < since_unix:
                    # יצאנו מטווח התאריכים
                    break
            
            media_id = media['id']
            media_type = media.get('media_type', 'IMAGE')
            
            # משיכת insights
            insights = get_media_insights(media_id, media_type)
            
            # קביעת סוג תוכן
            if media_type == 'VIDEO':
                content_type = 'Reel'  # ברוב המקרים וידאו באינסטגרם זה רילס
            elif media_type == 'CAROUSEL_ALBUM':
                content_type = 'Carousel'
            else:
                content_type = 'Photo'
            
            all_media.append({
                'media_id': media_id,
                'date': media_date.astimezone(pytz.timezone('Asia/Jerusalem')).strftime('%Y-%m-%d') if timestamp else '',
                'time': media_date.astimezone(pytz.timezone('Asia/Jerusalem')).strftime('%H:%M') if timestamp else '',
                'type': content_type,
                'caption': (media.get('caption', '') or '')[:500].replace('\n', ' '),
                'likes': media.get('like_count', 0),
                'comments': media.get('comments_count', 0),
                'views': insights.get('views', 0),
                'reach': insights.get('reach', 0),
                'saved': insights.get('saved', 0),
                'shares': insights.get('shares', 0),
                'total_interactions': insights.get('total_interactions', 0),
                'avg_watch_sec': insights.get('avg_watch_sec', 0),
                'engagement_rate': 0,  # יחושב אחר כך
                'permalink': media.get('permalink', ''),
                'pulled_at': datetime.now(pytz.timezone('Asia/Jerusalem')).strftime('%Y-%m-%d %H:%M')
            })
            
            time.sleep(0.15)  # Rate limiting
        
        # בדיקה אם הגענו לתאריך היעד
        if res['data']:
            last_timestamp = res['data'][-1].get('timestamp', '')
            if last_timestamp:
                # Handle both '+0000' and 'Z' formats
                ts_normalized = re.sub(r'\+0000$', '+00:00', last_timestamp.replace('Z', '+00:00'))
                last_date = datetime.fromisoformat(ts_normalized)
                if last_date.timestamp() < since_unix:
                    break
        
        # דף הבא
        if 'paging' in res and 'next' in res['paging']:
            url = res['paging']['next']
            params = {}
        else:
            break
    
    print(f"📊 Fetched {len(all_media)} media items")
    
    # חישוב engagement rate
    for item in all_media:
        reach = item.get('reach', 0)
        if reach > 0:
            total_eng = item['likes'] + item['comments'] + item['saved'] + item['shares']
            item['engagement_rate'] = round((total_eng / reach) * 100, 2)
    
    return pd.DataFrame(all_media)


def save_to_sheets(new_df):
    """שמירה חכמה לגוגל שיטס עם מיזוג נתונים"""
    if new_df.empty:
        print("⚠️ No data to save")
        return
    
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
        # יצירת גיליון חדש
        worksheet = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=20)
        print(f"✅ Created new sheet: {SHEET_NAME}")

    # קריאת היסטוריה
    try:
        existing_data = worksheet.get_all_records()
        existing_df = pd.DataFrame(existing_data)
    except Exception as e:
        print(f"⚠️ Warning reading existing data: {e}")
        existing_df = pd.DataFrame()

    # מיזוג
    if not existing_df.empty:
        new_df['media_id'] = new_df['media_id'].astype(str)
        existing_df['media_id'] = existing_df['media_id'].astype(str)

        # הגנה מפני כשלי API רגעיים: 0 חדש לא דורס ערך חיובי קיים
        new_df = backfill_zero_metrics(
            new_df, existing_df, key='media_id',
            cols=['likes', 'comments', 'views', 'reach', 'saved', 'shares',
                  'total_interactions', 'avg_watch_sec', 'engagement_rate']
        )

        # חישוב דלתא לצפיות
        if 'views' in existing_df.columns:
            existing_df['views'] = pd.to_numeric(existing_df['views'], errors='coerce').fillna(0)
            views_map = existing_df.set_index('media_id')['views'].to_dict()
            new_df['views_delta'] = new_df.apply(
                lambda x: x['views'] - views_map.get(x['media_id'], x['views']),
                axis=1
            )
        else:
            new_df['views_delta'] = 0

        # חישוב דלתא ל-reach
        if 'reach' in existing_df.columns:
            existing_df['reach'] = pd.to_numeric(existing_df['reach'], errors='coerce').fillna(0)
            reach_map = existing_df.set_index('media_id')['reach'].to_dict()
            new_df['reach_delta'] = new_df.apply(
                lambda x: x['reach'] - reach_map.get(x['media_id'], x['reach']),
                axis=1
            )
        else:
            new_df['reach_delta'] = 0

        # וידוא עמודות
        for col in new_df.columns:
            if col not in existing_df.columns:
                existing_df[col] = ""

        combined = pd.concat([new_df, existing_df])
        final_df = combined.drop_duplicates(subset=['media_id'], keep='first')
        print(f"🔄 Merged: {len(new_df)} new/updated + {len(existing_df)} existing -> {len(final_df)} total")
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


def main():
    print(f"\n{'='*50}")
    print(f"📸 Instagram Collector - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}\n")
    
    if not ACCESS_TOKEN:
        print("❌ Missing FACEBOOK_TOKEN environment variable")
        return
    
    # מציאת ה-Instagram Account ID
    ig_account_id = get_instagram_account_id()
    if not ig_account_id:
        return
    
    # משיכת נתונים
    df = fetch_instagram_media(ig_account_id)
    
    if not df.empty:
        save_to_sheets(df)
        print(f"\n✅ Done! {len(df)} media items processed.")
    else:
        print("❌ No data collected.")


if __name__ == "__main__":
    main()
