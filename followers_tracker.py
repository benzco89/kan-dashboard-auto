"""
Followers Tracker - מעקב אחרי עוקבים בכל הפלטפורמות
שומר נתון יומי של מספר העוקבים ביוטיוב ופייסבוק לגיליון נפרד
"""

import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime
import pytz

# --- Config ---
SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET_NAME = "מעקב עוקבים"

# YouTube
YOUTUBE_CHANNEL_ID = 'UC_HwfTAcjBESKZRJq6BTCpg'

# Facebook
FACEBOOK_PAGE_ID = "220634478361516"
FACEBOOK_API_VERSION = "v24.0"

# --- Helper Functions ---

def get_israel_date():
    """מחזיר את התאריך הנוכחי בישראל"""
    il_tz = pytz.timezone('Asia/Jerusalem')
    return datetime.now(il_tz).strftime('%Y-%m-%d')

def get_israel_datetime():
    """מחזיר תאריך ושעה בישראל"""
    il_tz = pytz.timezone('Asia/Jerusalem')
    return datetime.now(il_tz).strftime('%Y-%m-%d %H:%M')

# --- YouTube Functions ---

def get_youtube_stats():
    """
    משיכת סטטיסטיקות ערוץ יוטיוב
    מחזיר: subscribers, total_views, video_count
    """
    api_key = os.environ.get('YOUTUBE_API_KEY')
    if not api_key:
        print("⚠️ Missing YOUTUBE_API_KEY")
        return None
    
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        request = youtube.channels().list(
            part="statistics,snippet",
            id=YOUTUBE_CHANNEL_ID
        )
        response = request.execute()
        
        if 'items' in response and len(response['items']) > 0:
            stats = response['items'][0]['statistics']
            snippet = response['items'][0]['snippet']
            
            return {
                'platform': 'YouTube',
                'channel_name': snippet.get('title', 'כאן חדשות'),
                'subscribers': int(stats.get('subscriberCount', 0)),
                'total_views': int(stats.get('viewCount', 0)),
                'video_count': int(stats.get('videoCount', 0)),
                'hidden_subscriber_count': stats.get('hiddenSubscriberCount', False)
            }
    except Exception as e:
        print(f"❌ YouTube API Error: {e}")
    
    return None

# --- Facebook Functions ---

def get_facebook_stats():
    """
    משיכת סטטיסטיקות דף פייסבוק
    מחזיר: followers, fan_count (likes), posts_count
    """
    access_token = os.environ.get('FACEBOOK_TOKEN')
    if not access_token:
        print("⚠️ Missing FACEBOOK_TOKEN")
        return None
    
    try:
        # שליפת מידע בסיסי על הדף
        url = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}/{FACEBOOK_PAGE_ID}"
        params = {
            'access_token': access_token,
            'fields': 'name,fan_count,followers_count'
        }
        
        res = requests.get(url, params=params).json()
        
        if 'error' in res:
            print(f"❌ Facebook API Error: {res['error']['message']}")
            return None
        
        # ניסיון לשלוף followers_count מה-insights
        followers_count = res.get('followers_count', 0)
        
        # אם אין followers_count, ננסה דרך insights
        if followers_count == 0:
            insights_url = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}/{FACEBOOK_PAGE_ID}/insights"
            insights_params = {
                'access_token': access_token,
                'metric': 'page_follows',
                'period': 'day'
            }
            try:
                insights_res = requests.get(insights_url, params=insights_params).json()
                if 'data' in insights_res and len(insights_res['data']) > 0:
                    values = insights_res['data'][0].get('values', [])
                    if values:
                        followers_count = values[-1].get('value', 0)
            except:
                pass
        
        return {
            'platform': 'Facebook',
            'page_name': res.get('name', 'כאן חדשות'),
            'followers': followers_count,
            'fan_count': res.get('fan_count', 0),  # לייקים לדף
            'total_views': 0,  # לא זמין ברמת הדף
            'video_count': 0   # לא זמין ברמת הדף
        }
        
    except Exception as e:
        print(f"❌ Facebook Error: {e}")

    return None

def get_facebook_daily_insights():
    """
    משיכת נתונים יומיים ברמת הדף - עוקבים חדשים, reach, אינטראקציות
    מחזיר: fan_adds, fan_removes, daily_reach, daily_engagements, daily_video_views
    """
    access_token = os.environ.get('FACEBOOK_TOKEN')
    if not access_token:
        print("⚠️ Missing FACEBOOK_TOKEN")
        return None

    try:
        url = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}/{FACEBOOK_PAGE_ID}/insights"
        params = {
            'access_token': access_token,
            'metric': ','.join([
                'page_fan_adds',              # עוקבים חדשים
                'page_fan_removes',           # עוקבים שעזבו
                'page_impressions_unique',    # reach יומי
                'page_post_engagements',      # אינטראקציות כוללות
                'page_video_views'            # צפיות וידאו כוללות
            ]),
            'period': 'day',
            'date_preset': 'yesterday'
        }

        res = requests.get(url, params=params).json()

        result = {
            'fan_adds': 0,
            'fan_removes': 0,
            'daily_reach': 0,
            'daily_engagements': 0,
            'daily_video_views': 0
        }

        if 'data' in res:
            for item in res['data']:
                name = item.get('name')
                values = item.get('values', [])
                value = values[0].get('value', 0) if values else 0

                if name == 'page_fan_adds':
                    result['fan_adds'] = value
                elif name == 'page_fan_removes':
                    result['fan_removes'] = value
                elif name == 'page_impressions_unique':
                    result['daily_reach'] = value
                elif name == 'page_post_engagements':
                    result['daily_engagements'] = value
                elif name == 'page_video_views':
                    result['daily_video_views'] = value

        return result

    except Exception as e:
        print(f"❌ Facebook Daily Insights Error: {e}")
        return None

# --- Google Sheets Functions ---

def get_sheet_client():
    """יצירת חיבור לגוגל שיטס"""
    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT')
    if not creds_json:
        creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    
    creds_dict = json.loads(creds_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

def save_followers_data(youtube_stats, facebook_stats):
    """שמירת נתוני העוקבים לגיליון"""
    gc = get_sheet_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    
    # יצירת/פתיחת הגיליון
    try:
        worksheet = sh.worksheet(SHEET_NAME)
    except:
        # יצירת גיליון חדש עם כותרות - תיקון: 14 עמודות בדיוק
        worksheet = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=14)
        headers = [
            'date', 'pulled_at', 'platform',
            'followers', 'fan_count', 'total_views', 'video_count',
            'followers_change', 'views_change',
            'fan_adds', 'fan_removes', 'daily_reach', 'daily_engagements', 'daily_video_views'
        ]
        worksheet.update('A1:N1', [headers])
        print(f"✅ Created new sheet: {SHEET_NAME}")
    
    # קריאת נתונים קיימים לחישוב שינוי
    try:
        existing_data = worksheet.get_all_records()
    except Exception as e:
        print(f"⚠️ Warning reading existing data: {e}")
        # אם יש בעיה בקריאה, נסה לנקות ולהתחיל מחדש
        worksheet.clear()
        headers = [
            'date', 'pulled_at', 'platform',
            'followers', 'fan_count', 'total_views', 'video_count',
            'followers_change', 'views_change',
            'fan_adds', 'fan_removes', 'daily_reach', 'daily_engagements', 'daily_video_views'
        ]
        worksheet.update('A1:N1', [headers])
        existing_data = []
    
    today = get_israel_date()
    pulled_at = get_israel_datetime()
    rows_to_add = []
    
    # עיבוד YouTube
    if youtube_stats:
        # חיפוש הרשומה האחרונה של YouTube
        prev_yt = None
        for row in reversed(existing_data):
            if row.get('platform') == 'YouTube' and row.get('date') != today:
                prev_yt = row
                break
        
        followers_change = 0
        views_change = 0
        if prev_yt:
            prev_followers = int(prev_yt.get('followers', 0) or 0)
            prev_views = int(prev_yt.get('total_views', 0) or 0)
            followers_change = youtube_stats['subscribers'] - prev_followers
            views_change = youtube_stats['total_views'] - prev_views
        
        rows_to_add.append([
            today,
            pulled_at,
            'YouTube',
            youtube_stats['subscribers'],
            0,  # fan_count לא רלוונטי ליוטיוב
            youtube_stats['total_views'],
            youtube_stats['video_count'],
            followers_change,
            views_change,
            0, 0, 0, 0, 0  # fan_adds, fan_removes, daily_reach, daily_engagements, daily_video_views - לא רלוונטי ליוטיוב
        ])
        print(f"📺 YouTube: {youtube_stats['subscribers']:,} subscribers (+{followers_change:,})")
    
    # עיבוד Facebook
    if facebook_stats:
        # חיפוש הרשומה האחרונה של Facebook
        prev_fb = None
        for row in reversed(existing_data):
            if row.get('platform') == 'Facebook' and row.get('date') != today:
                prev_fb = row
                break

        followers_change = 0
        if prev_fb:
            prev_followers = int(prev_fb.get('followers', 0) or 0)
            if prev_followers == 0:
                prev_followers = int(prev_fb.get('fan_count', 0) or 0)
            current_followers = facebook_stats['followers'] or facebook_stats['fan_count']
            followers_change = current_followers - prev_followers

        # משיכת נתונים יומיים נוספים
        daily_insights = get_facebook_daily_insights()

        # הכנת ערכים (אם אין נתונים יומיים - נשים 0)
        fan_adds = daily_insights.get('fan_adds', 0) if daily_insights else 0
        fan_removes = daily_insights.get('fan_removes', 0) if daily_insights else 0
        daily_reach = daily_insights.get('daily_reach', 0) if daily_insights else 0
        daily_engagements = daily_insights.get('daily_engagements', 0) if daily_insights else 0
        daily_video_views = daily_insights.get('daily_video_views', 0) if daily_insights else 0

        rows_to_add.append([
            today,
            pulled_at,
            'Facebook',
            facebook_stats['followers'],
            facebook_stats['fan_count'],
            0,  # total_views לא זמין
            0,  # video_count לא זמין
            followers_change,
            0,  # views_change לא זמין
            fan_adds,
            fan_removes,
            daily_reach,
            daily_engagements,
            daily_video_views
        ])
        print(f"📘 Facebook: {facebook_stats['fan_count']:,} likes, {facebook_stats['followers']:,} followers (+{followers_change:,})")
        if daily_insights:
            print(f"   Daily: +{fan_adds:,} adds, -{fan_removes:,} removes, {daily_reach:,} reach, {daily_engagements:,} engagements")
    
    # בדיקה אם כבר יש נתונים להיום - עדכון במקום הוספה
    updated_today = False
    for i, row in enumerate(existing_data):
        if row.get('date') == today:
            # מחיקת שורות קיימות להיום ושמירה מחדש
            updated_today = True
            break
    
    if updated_today:
        # מחיקת שורות היום הקיימות
        all_data = worksheet.get_all_values()
        new_data = [all_data[0]]  # headers
        for row in all_data[1:]:
            if row[0] != today:
                new_data.append(row)
        
        # הוספת הנתונים החדשים
        for new_row in rows_to_add:
            new_data.append(new_row)
        
        worksheet.clear()
        worksheet.update(new_data)
        print(f"🔄 Updated existing data for {today}")
    else:
        # הוספת שורות חדשות
        if rows_to_add:
            worksheet.append_rows(rows_to_add)
            print(f"✅ Added {len(rows_to_add)} rows for {today}")
    
    return True

# --- Main ---

def main():
    print(f"\n{'='*50}")
    print(f"📊 Followers Tracker - {get_israel_datetime()}")
    print(f"{'='*50}\n")
    
    # משיכת נתונים מכל הפלטפורמות
    youtube_stats = get_youtube_stats()
    facebook_stats = get_facebook_stats()
    
    # בדיקה שיש לפחות פלטפורמה אחת עם נתונים
    if not youtube_stats and not facebook_stats:
        print("❌ No data collected from any platform!")
        return
    
    # שמירה לשיטס
    save_followers_data(youtube_stats, facebook_stats)
    
    print(f"\n{'='*50}")
    print("✅ Followers tracking complete!")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
