"""
Followers Tracker - מעקב אחרי עוקבים בכל הפלטפורמות
מבנה Wide Format: שורה אחת לכל תאריך עם עמודות לכל פלטפורמה
"""

import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta, timezone
import pytz

from utils import http_get_json

# Load .env file if exists (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Config ---
SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SHEET_NAME = "מעקב עוקבים"

# YouTube
YOUTUBE_CHANNEL_ID = 'UC_HwfTAcjBESKZRJq6BTCpg'

# Facebook
FACEBOOK_PAGE_ID = "220634478361516"
FACEBOOK_API_VERSION = "v24.0"

# --- Wide Format Headers ---
HEADERS = [
    'date',
    'pulled_at',
    # YouTube
    'yt_subscribers',
    'yt_subscribers_change',
    'yt_total_views',
    'yt_views_change',
    'yt_video_count',
    # Facebook
    'fb_followers',
    'fb_followers_change',
    'fb_fan_count',
    'fb_fan_adds',
    'fb_fan_removes',
    'fb_daily_reach',
    'fb_daily_engagements',
    'fb_daily_video_views',
    # Instagram (לעתיד)
    'ig_followers',
    'ig_followers_change',
    'ig_daily_reach',
    'ig_daily_views',
    # Twitter / X
    'tw_followers',
    'tw_followers_change',
    'tw_tweet_count',
    # TikTok
    'tt_followers',
    'tt_followers_change',
    'tt_video_count',
    # חדשות נכנסות רק בסוף: השורות נכתבות לפי מיקום, אז עמודה
    # שנדחפת באמצע מזיזה את כל הנתונים ההיסטוריים לכותרת השכנה.
    'ig_website_clicks',
    # היום שאליו מתייחסים המדדים היומיים. התאריך בעמודה A הוא יום ההרצה,
    # והם אף פעם לא אותו יום — ראו _latest_daily_insight.
    'insights_day',
    'fb_page_views',
    'ig_accounts_engaged',
    'ig_profile_views',
]

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
    """משיכת סטטיסטיקות ערוץ יוטיוב"""
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
            return {
                'subscribers': int(stats.get('subscriberCount', 0)),
                'total_views': int(stats.get('viewCount', 0)),
                'video_count': int(stats.get('videoCount', 0)),
            }
    except Exception as e:
        print(f"❌ YouTube API Error: {e}")
    
    return None

# --- Facebook Functions ---

def get_facebook_stats():
    """משיכת סטטיסטיקות דף פייסבוק"""
    access_token = os.environ.get('FACEBOOK_TOKEN')
    if not access_token:
        print("⚠️ Missing FACEBOOK_TOKEN")
        return None
    
    try:
        url = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}/{FACEBOOK_PAGE_ID}"
        params = {
            'access_token': access_token,
            'fields': 'name,fan_count,followers_count'
        }
        
        res = http_get_json(url, params=params)
        
        if 'error' in res:
            print(f"❌ Facebook API Error: {res['error']['message']}")
            return None
        
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
                insights_res = http_get_json(insights_url, params=insights_params)
                if 'data' in insights_res and len(insights_res['data']) > 0:
                    values = insights_res['data'][0].get('values', [])
                    if values:
                        followers_count = values[-1].get('value', 0)
            except:
                pass
        
        return {
            'followers': followers_count,
            'fan_count': res.get('fan_count', 0),
        }
        
    except Exception as e:
        print(f"❌ Facebook Error: {e}")

    return None

def _latest_daily_insight(obj_id, metric, extra=None, days=5):
    """המדד היומי האחרון שכבר נסגר — ואיזה יום הוא מתאר.

    לא date_preset=yesterday. היום של מטא נסגר ב-07:00 UTC, שהם 10:00 בבוקר
    שעון ישראל, והטראקר רץ ב-08:30 — שעה וחצי לפני. אז "אתמול" החזיר את היום
    שלפניו, ואותו מספר בדיוק נכתב בשתי שורות עוקבות ב-26 וב-27 ביולי
    (1,080,464 חשיפה, 304,274 מעורבות). אותה קריאה בצהריים החזירה מספר אחר.

    בקשה של טווח ולקיחת הנקודה האחרונה לא תלויה בשעה שבה ה-workflow רץ, ומטא
    מחזירה end_time לכל נקודה — כך שהיום מגיע מהתשובה במקום להיות מנוחש.
    הדלי שנגמר ב-end_time מתאר את היממה שלפניו, ולכן היום = end_time פחות יום.

    מחזירה (value, day). day הוא None כשאין תשובה — הקורא לא ימציא תאריך.
    """
    token = os.environ.get('FACEBOOK_TOKEN')
    if not token:
        return 0, None
    now = datetime.now(timezone.utc)
    params = {'access_token': token, 'metric': metric, 'period': 'day',
              'since': (now - timedelta(days=days)).strftime('%Y-%m-%d'),
              'until': now.strftime('%Y-%m-%d')}
    params.update(extra or {})
    try:
        res = http_get_json(f"https://graph.facebook.com/{FACEBOOK_API_VERSION}/{obj_id}/insights",
                            params=params)
        if 'error' in res:
            print(f"   ⚠️ {metric}: {res['error'].get('message', '')[:80]}")
            return 0, None
        points = []
        for block in res.get('data') or []:
            points.extend(block.get('values') or [])
        if not points:
            return 0, None
        last = points[-1]
        value = last.get('value')
        if isinstance(value, dict):     # ריאקציות וכד' מגיעות כפירוק
            value = sum(v for v in value.values() if isinstance(v, (int, float)))
        end = str(last.get('end_time') or '')[:10]
        day = ((datetime.strptime(end, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
               if end else None)
        return (value or 0), day
    except Exception as e:
        print(f"   ⚠️ {metric}: {e}")
        return 0, None


def get_facebook_daily_insights():
    """נתונים יומיים ברמת הדף, מטריקה-מטריקה.

    שמות מאומתים מול הדף ב-v25 (followers_probe.py, 26.7.2026):
      page_daily_follows            מחליף את page_fan_adds שהוסר
      page_total_media_view_unique  מחליף את page_impressions_unique שהוסר
      page_post_engagements         עבד כל הזמן — נפל רק כי היה באותה קריאה
      page_video_views              כנ"ל
      page_views_total              כניסות לעמוד עצמו (חדש; 15K-29.5K ביום)
    ל-page_fan_removes לא נמצא מחליף; הסרות עוקבים כנראה כבר לא נחשפות.

    כל המדדים נמשכים לאותו יום — היום שמטא כבר סגרה — והוא מוחזר ב-insights_day
    כדי שהדשבורד יקרא את היום מהנתון ולא מתאריך השורה.
    """
    if not os.environ.get('FACEBOOK_TOKEN'):
        return None
    # מטריקה-מטריקה, לא בקשה אחת: מטא מפילה את כל הקריאה כששם אחד בטל.
    # page_impressions_unique הוסר ב-15.6.2026 ולקח איתו את page_fan_adds,
    # page_fan_removes, page_post_engagements ו-page_video_views — ארבע עמודות
    # ריקות ב-230 שורות, שמונה חודשים, בלי שגיאה בשום מקום.
    out, day = {}, None
    for key, metric in (('fan_adds', 'page_daily_follows'),
                        ('daily_reach', 'page_total_media_view_unique'),
                        ('daily_engagements', 'page_post_engagements'),
                        ('daily_video_views', 'page_video_views'),
                        ('page_views', 'page_views_total')):
        out[key], d = _latest_daily_insight(FACEBOOK_PAGE_ID, metric)
        day = day or d
    out['fan_removes'] = 0
    out['insights_day'] = day
    return out


# --- Instagram Functions ---

def get_instagram_account_id():
    """משיכת ה-Instagram Business Account ID מהדף המחובר"""
    access_token = os.environ.get('FACEBOOK_TOKEN')
    if not access_token:
        return None
    
    url = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}/me"
    params = {
        'access_token': access_token,
        'fields': 'id,name,instagram_business_account'
    }
    
    try:
        res = http_get_json(url, params=params)
        
        if 'error' in res:
            print(f"❌ Instagram Error: {res['error']['message']}")
            return None
        
        ig_account = res.get('instagram_business_account')
        if ig_account:
            return ig_account['id']
        
        return None
        
    except Exception as e:
        print(f"❌ Instagram Error: {e}")
        return None


def get_instagram_stats():
    """משיכת סטטיסטיקות חשבון אינסטגרם"""
    access_token = os.environ.get('FACEBOOK_TOKEN')
    if not access_token:
        print("⚠️ Missing FACEBOOK_TOKEN for Instagram")
        return None
    
    ig_account_id = get_instagram_account_id()
    if not ig_account_id:
        print("⚠️ No Instagram Business Account found")
        return None
    
    try:
        url = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}/{ig_account_id}"
        params = {
            'access_token': access_token,
            'fields': 'followers_count,media_count'
        }
        
        res = http_get_json(url, params=params)
        
        if 'error' in res:
            print(f"❌ Instagram API Error: {res['error']['message']}")
            return None
        
        return {
            'followers': res.get('followers_count', 0),
            'media_count': res.get('media_count', 0),
        }
        
    except Exception as e:
        print(f"❌ Instagram Error: {e}")
    
    return None


def _one_day_total(obj_id, metric, day, extra=None):
    """מדד ברמת חשבון ליום מסוים. total_value עונה לטווח ולא ליום, אז מבקשים
    טווח של יום אחד — ואת אותו יום שפייסבוק כבר סגרה, כדי ששתי הפלטפורמות
    יתארו את אותה יממה. בלי יום ידוע לא ננחש: מחזירים 0."""
    token = os.environ.get('FACEBOOK_TOKEN')
    if not token or not day:
        return 0
    nxt = (datetime.strptime(day, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    params = {'access_token': token, 'metric': metric, 'period': 'day',
              'since': day, 'until': nxt}
    params.update(extra or {})
    try:
        res = http_get_json(f"https://graph.facebook.com/{FACEBOOK_API_VERSION}/{obj_id}/insights",
                            params=params)
        if 'error' in res:
            print(f"   ⚠️ ig {metric}: {res['error'].get('message', '')[:80]}")
            return 0
        for block in res.get('data') or []:
            tv = block.get('total_value')
            if isinstance(tv, dict) and tv.get('value') is not None:
                return tv.get('value') or 0
            for v in block.get('values') or []:
                return v.get('value') or 0
        return 0
    except Exception as e:
        print(f"   ⚠️ ig {metric}: {e}")
        return 0


def get_instagram_daily_insights(day=None):
    """נתונים יומיים ברמת החשבון, מטריקה-מטריקה.

    `impressions` הוסר, וכיוון שהוא נמשך באותה קריאה עם `reach` — גם reach חזר
    ריק. המחליף הוא `views`.

    צירוף הפרמטרים אינו שרירותי: date_preset=yesterday עם metric_type=total_value
    מחזיר מספר לכל המטריקות, בעוד since/until מחזיר רק חלק מהן (נמדד בפרוב).

    website_clicks הוא היחיד שאומר כמה אנשים עברו מאינסטגרם לאתר — הוא לא קיים
    ברמת פוסט בשום סוג מדיה, רק כאן.
    """
    if not os.environ.get('FACEBOOK_TOKEN'):
        return None
    ig = get_instagram_account_id()
    if not ig:
        return None
    extra = {'metric_type': 'total_value'}
    out = {
        'daily_reach': _one_day_total(ig, 'reach', day, extra),
        'daily_views': _one_day_total(ig, 'views', day, extra),
        'website_clicks': _one_day_total(ig, 'website_clicks', day, extra),
        # חדשים. accounts_engaged סופר חשבונות ייחודיים ולא פעולות, וזו שאלה
        # שאף סכום של לייקים ותגובות לא עונה עליה. profile_views הוא כמה אנשים
        # באו לחפש אותנו — הוא הכפיל את עצמו ל-8,274 ביום שיובל קוגן נעדר.
        'accounts_engaged': _one_day_total(ig, 'accounts_engaged', day, extra),
        'profile_views': _one_day_total(ig, 'profile_views', day, extra),
    }
    return out


# --- Twitter / X Functions ---

def get_twitter_stats():
    """משיכת מספר עוקבים וציוצים מטוויטר דרך GetXAPI. מוגן: בלי מפתח -> None."""
    api_key = os.environ.get('GETXAPI_KEY')
    if not api_key:
        print("⚠️ Missing GETXAPI_KEY - skipping Twitter")
        return None

    username = os.environ.get('TWITTER_USERNAME', 'kann_news')
    try:
        res = requests.get(
            "https://api.getxapi.com/twitter/user/info",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"userName": username},
            timeout=30,
        ).json()

        data = res.get('data', res) if isinstance(res, dict) else {}

        # שמות שדה אפשריים (תלוי בגרסת GetXAPI)
        followers = (
            data.get('followers')
            or data.get('followers_count')
            or data.get('followersCount')
            or 0
        )
        tweet_count = (
            data.get('statusesCount')
            or data.get('tweet_count')
            or data.get('statuses_count')
            or 0
        )

        return {
            'followers': int(followers or 0),
            'tweet_count': int(tweet_count or 0),
        }
    except Exception as e:
        print(f"❌ Twitter (GetXAPI) Error: {e}")
        return None

# --- TikTok Functions ---

def get_tiktok_stats():
    """משיכת מספר עוקבים וסרטונים מטיקטוק דרך TikHub. מוגן: בלי טוקן -> None."""
    api_key = os.environ.get('TIKHUB_TOKEN')
    if not api_key:
        print("⚠️ Missing TIKHUB_TOKEN - skipping TikTok")
        return None

    username = os.environ.get('TIKTOK_USERNAME', 'kan_news')
    try:
        res = requests.get(
            "https://api.tikhub.io/api/v1/tiktok/web/fetch_user_profile",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"uniqueId": username},
            timeout=30,
        ).json()

        stats = ((res.get('data') or {}).get('userInfo') or {}).get('stats') or {}
        followers = stats.get('followerCount', 0)
        video_count = stats.get('videoCount', 0)

        if not followers:
            print(f"❌ TikTok (TikHub) returned no follower count: {str(res)[:150]}")
            return None

        return {
            'followers': int(followers or 0),
            'video_count': int(video_count or 0),
        }
    except Exception as e:
        print(f"❌ TikTok (TikHub) Error: {e}")
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

def save_followers_data(youtube_stats, facebook_stats, instagram_stats, twitter_stats=None, tiktok_stats=None):
    """שמירת נתוני העוקבים לגיליון בפורמט Wide"""
    gc = get_sheet_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    
    # יצירת/פתיחת הגיליון
    try:
        worksheet = sh.worksheet(SHEET_NAME)
    except:
        worksheet = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=len(HEADERS))
        worksheet.update('A1', [HEADERS])
        print(f"✅ Created new sheet: {SHEET_NAME}")
    
    # קריאת נתונים קיימים
    try:
        all_values = worksheet.get_all_values()
        if not all_values or all_values[0] != HEADERS:
            # עדכון כותרות אם השתנו
            worksheet.update('A1', [HEADERS])
            all_values = worksheet.get_all_values()
    except Exception as e:
        print(f"⚠️ Warning reading existing data: {e}")
        worksheet.clear()
        worksheet.update('A1', [HEADERS])
        all_values = [HEADERS]
    
    today = get_israel_date()
    pulled_at = get_israel_datetime()
    
    # מציאת השורה הקודמת (לא של היום) לחישוב שינוי
    prev_row = None
    for row in reversed(all_values[1:]):
        if row and len(row) > 0 and row[0] != today:
            prev_row = row
            break
    
    # חישוב שינויים
    yt_subscribers_change = 0
    yt_views_change = 0
    fb_followers_change = 0
    ig_followers_change = 0
    tw_followers_change = 0
    tt_followers_change = 0

    if prev_row and len(prev_row) >= 10:
        try:
            if youtube_stats and prev_row[2]:
                prev_yt_subs = int(prev_row[2] or 0)
                yt_subscribers_change = youtube_stats['subscribers'] - prev_yt_subs
            if youtube_stats and prev_row[4]:
                prev_yt_views = int(prev_row[4] or 0)
                yt_views_change = youtube_stats['total_views'] - prev_yt_views
            if facebook_stats and prev_row[7]:
                prev_fb_followers = int(prev_row[7] or 0)
                fb_followers_change = (facebook_stats['followers'] or facebook_stats['fan_count']) - prev_fb_followers
            if instagram_stats and len(prev_row) > 15 and prev_row[15]:
                prev_ig_followers = int(prev_row[15] or 0)
                ig_followers_change = instagram_stats['followers'] - prev_ig_followers
            if twitter_stats and len(prev_row) > 19 and prev_row[19]:
                prev_tw_followers = int(prev_row[19] or 0)
                tw_followers_change = twitter_stats['followers'] - prev_tw_followers
            if tiktok_stats and len(prev_row) > 22 and prev_row[22]:
                prev_tt_followers = int(prev_row[22] or 0)
                tt_followers_change = tiktok_stats['followers'] - prev_tt_followers
        except (ValueError, IndexError):
            pass
    
    # משיכת נתונים יומיים של פייסבוק
    fb_daily = get_facebook_daily_insights() or {}
    # אותו יום לשתי הפלטפורמות — הוא מגיע מ-end_time שמטא החזירה, לא מהשעון שלנו
    ig_daily = get_instagram_daily_insights(fb_daily.get('insights_day')) or {}
    
    # בניית שורה חדשה
    new_row = [
        today,
        pulled_at,
        # YouTube
        youtube_stats['subscribers'] if youtube_stats else '',
        yt_subscribers_change if youtube_stats else '',
        youtube_stats['total_views'] if youtube_stats else '',
        yt_views_change if youtube_stats else '',
        youtube_stats['video_count'] if youtube_stats else '',
        # Facebook
        facebook_stats['followers'] if facebook_stats else '',
        fb_followers_change if facebook_stats else '',
        facebook_stats['fan_count'] if facebook_stats else '',
        fb_daily.get('fan_adds', ''),
        fb_daily.get('fan_removes', ''),
        fb_daily.get('daily_reach', ''),
        fb_daily.get('daily_engagements', ''),
        fb_daily.get('daily_video_views', ''),
        # Instagram
        instagram_stats['followers'] if instagram_stats else '',
        ig_followers_change if instagram_stats else '',
        ig_daily.get('daily_reach', ''),
        ig_daily.get('daily_views', ''),
        # Twitter / X
        twitter_stats['followers'] if twitter_stats else '',
        tw_followers_change if twitter_stats else '',
        twitter_stats['tweet_count'] if twitter_stats else '',
        # TikTok
        tiktok_stats['followers'] if tiktok_stats else '',
        tt_followers_change if tiktok_stats else '',
        tiktok_stats['video_count'] if tiktok_stats else '',
        # בסוף השורה, בהתאמה לסוף ה-HEADERS
        ig_daily.get('website_clicks', ''),
        fb_daily.get('insights_day', ''),
        fb_daily.get('page_views', ''),
        ig_daily.get('accounts_engaged', ''),
        ig_daily.get('profile_views', ''),
    ]
    
    # בדיקה אם כבר יש שורה להיום
    row_index = None
    for i, row in enumerate(all_values[1:], start=2):
        if row and len(row) > 0 and row[0] == today:
            row_index = i
            break
    
    if row_index:
        # עדכון שורה קיימת
        worksheet.update(f'A{row_index}', [new_row])
        print(f"🔄 Updated existing row for {today}")
    else:
        # הוספת שורה חדשה
        worksheet.append_row(new_row)
        print(f"✅ Added new row for {today}")
    
    # הדפסת סיכום
    if youtube_stats:
        print(f"📺 YouTube: {youtube_stats['subscribers']:,} subscribers ({yt_subscribers_change:+,})")
    if facebook_stats:
        fb_count = facebook_stats['followers'] or facebook_stats['fan_count']
        print(f"📘 Facebook: {fb_count:,} followers ({fb_followers_change:+,})")
        if fb_daily:
            print(f"   Daily: +{fb_daily.get('fan_adds', 0):,} adds, {fb_daily.get('daily_reach', 0):,} reach")
    if instagram_stats:
        print(f"📸 Instagram: {instagram_stats['followers']:,} followers ({ig_followers_change:+,})")
        if ig_daily:
            print(f"   Daily: {ig_daily.get('daily_reach', 0):,} reach, {ig_daily.get('daily_views', 0):,} views, "
                  f"{ig_daily.get('accounts_engaged', 0):,} accounts engaged, {ig_daily.get('profile_views', 0):,} profile views")
    print(f"📅 המדדים היומיים מתארים את {fb_daily.get('insights_day') or '??'} "
          f"(תאריך השורה: {today}) — היום של מטא נסגר ב-10:00 שעון ישראל")
    if twitter_stats:
        print(f"🐦 Twitter: {twitter_stats['followers']:,} followers ({tw_followers_change:+,})")
    if tiktok_stats:
        print(f"🎵 TikTok: {tiktok_stats['followers']:,} followers ({tt_followers_change:+,})")

    return True

# --- Main ---

def main():
    print(f"\n{'='*50}")
    print(f"📊 Followers Tracker (Wide Format) - {get_israel_datetime()}")
    print(f"{'='*50}\n")
    
    # משיכת נתונים מכל הפלטפורמות
    youtube_stats = get_youtube_stats()
    facebook_stats = get_facebook_stats()
    instagram_stats = get_instagram_stats()
    twitter_stats = get_twitter_stats()
    tiktok_stats = get_tiktok_stats()

    # בדיקה שיש לפחות פלטפורמה אחת עם נתונים
    if not youtube_stats and not facebook_stats and not instagram_stats and not twitter_stats and not tiktok_stats:
        print("❌ No data collected from any platform!")
        return

    # שמירה לשיטס
    save_followers_data(youtube_stats, facebook_stats, instagram_stats, twitter_stats, tiktok_stats)
    
    print(f"\n{'='*50}")
    print("✅ Followers tracking complete!")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
