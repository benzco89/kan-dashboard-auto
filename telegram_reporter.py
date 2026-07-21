"""
Telegram Reporter - דוח AI מאוחד לכל הפלטפורמות
קורא נתונים מכל הגיליונות (YouTube, Facebook, Instagram) ויוצר דוח עם Gemini
"""

import os
import sys
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import pytz
import requests
from google import genai
from google.genai import types

# Fix encoding for Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# Load .env file if exists (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, using environment variables directly

# --- הגדרות ---
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c/edit"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


def get_sheet_client():
    creds_json = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    return gspread.authorize(creds)


def _clean_title(t, limit=150):
    """כותרת בטוחה לפרומפט: בלי סוגריים מרובעים/עגולים שמפרקים את קישורי
    ה-Markdown שהדוח בונה ([כותרת](LINK)), ובלי קפשנים באורך קילומטר."""
    t = str(t or '').replace('[', '').replace(']', '').replace('(', '').replace(')', '').strip()
    return t[:limit] + ('…' if len(t) > limit else '')


def _daily_baseline(df, date_col, value_col, today_str, days=7):
    """ממוצע יומי של מטריקה בשבוע האחרון - בסיס להשוואה עבור ה-AI."""
    if df.empty or date_col not in df.columns or value_col not in df.columns:
        return 0
    cutoff = (datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=days)).strftime('%Y-%m-%d')
    recent = df[(df[date_col].astype(str) >= cutoff) & (df[date_col].astype(str) < today_str)]
    return int(recent[value_col].sum() / days) if not recent.empty else 0


def get_youtube_data():
    """שליפת נתוני יוטיוב מהגיליון"""
    try:
        gc = get_sheet_client()
        sh = gc.open_by_url(SPREADSHEET_URL)
        worksheet = sh.worksheet('נתוני יוטיוב')
        df = pd.DataFrame(worksheet.get_all_records())
        if not df.empty:
            df['views'] = pd.to_numeric(df['views'], errors='coerce').fillna(0)
            if 'views_delta' in df.columns:
                df['views_delta'] = pd.to_numeric(df['views_delta'], errors='coerce').fillna(0)
        return df
    except Exception as e:
        print(f"Error fetching YouTube data: {e}")
        return pd.DataFrame()


def get_facebook_data():
    """שליפת נתוני פייסבוק מהגיליון"""
    try:
        gc = get_sheet_client()
        sh = gc.open_by_url(SPREADSHEET_URL)
        worksheet = sh.worksheet('נתוני פייסבוק')
        df = pd.DataFrame(worksheet.get_all_records())
        if not df.empty:
            df['views'] = pd.to_numeric(df['views'], errors='coerce').fillna(0)
            df['reach'] = pd.to_numeric(df['reach'], errors='coerce').fillna(0)
            if 'views_delta' in df.columns:
                df['views_delta'] = pd.to_numeric(df['views_delta'], errors='coerce').fillna(0)
        return df
    except Exception as e:
        print(f"Error fetching Facebook data: {e}")
        return pd.DataFrame()


def get_instagram_data():
    """שליפת נתוני אינסטגרם מהגיליון"""
    try:
        gc = get_sheet_client()
        sh = gc.open_by_url(SPREADSHEET_URL)
        worksheet = sh.worksheet('נתוני אינסטגרם')
        df = pd.DataFrame(worksheet.get_all_records())
        if not df.empty:
            df['views'] = pd.to_numeric(df['views'], errors='coerce').fillna(0)
            df['reach'] = pd.to_numeric(df['reach'], errors='coerce').fillna(0)
            for col in ('views_delta', 'skip_rate', 'fb_views'):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df
    except Exception as e:
        print(f"Error fetching Instagram data: {e}")
        return pd.DataFrame()


def get_tiktok_data():
    """שליפת נתוני טיקטוק מהגיליון"""
    try:
        gc = get_sheet_client()
        sh = gc.open_by_url(SPREADSHEET_URL)
        worksheet = sh.worksheet('נתוני טיקטוק')
        df = pd.DataFrame(worksheet.get_all_records())
        if not df.empty:
            for col in ('views', 'views_delta', 'likes', 'comments', 'shares',
                        'whatsapp_shares', 'saves', 'engagement_rate'):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df
    except Exception as e:
        print(f"Error fetching TikTok data: {e}")
        return pd.DataFrame()


def get_followers_data():
    """שליפת נתוני עוקבים מהגיליון"""
    try:
        gc = get_sheet_client()
        sh = gc.open_by_url(SPREADSHEET_URL)
        worksheet = sh.worksheet('מעקב עוקבים')
        df = pd.DataFrame(worksheet.get_all_records())
        return df
    except Exception as e:
        print(f"Error fetching followers data: {e}")
        return pd.DataFrame()


def summarize_youtube(df, yesterday_date):
    """יצירת סיכום יוטיוב לפרומפט - כולל מטריקות מעורבות לניתוח AI"""
    if df.empty:
        return "אין נתונים"
    
    # סרטונים חדשים מאתמול
    new_yesterday = df[df['published_at'] == yesterday_date].copy()
    new_count = len(new_yesterday)
    total_views_new = int(new_yesterday['views'].sum()) if not new_yesterday.empty else 0
    total_likes_new = int(new_yesterday['likes'].sum()) if 'likes' in new_yesterday.columns and not new_yesterday.empty else 0
    total_comments_new = int(new_yesterday['comments'].sum()) if 'comments' in new_yesterday.columns and not new_yesterday.empty else 0
    
    # חישוב ממוצע like_rate לסרטונים החדשים
    avg_like_rate = 0
    if 'like_rate' in new_yesterday.columns and not new_yesterday.empty:
        avg_like_rate = round(new_yesterday['like_rate'].mean(), 2)
    
    # טופ 5 מאתמול - עם מטריקות מעורבות לניתוח
    top_new = ""
    if not new_yesterday.empty:
        new_yesterday = new_yesterday.sort_values('views', ascending=False)
        for _, row in new_yesterday.head(5).iterrows():
            likes = int(row.get('likes', 0))
            comments = int(row.get('comments', 0))
            like_rate = round(row.get('like_rate', 0), 1)
            # פורמט מורחב לניתוח AI
            video_url = row.get('video_url', '')
            top_new += f"• {_clean_title(row['title'])} | {row.get('video_type', 'רגיל')} | {int(row['views']):,} צפיות | {likes:,} לייקים ({like_rate}%) | {comments} תגובות | LINK: {video_url}\n"
    
    # סרטונים ישנים עם דלתא גבוהה
    top_delta = ""
    if 'views_delta' in df.columns:
        old_videos = df[df['published_at'] < yesterday_date].copy()
        # רק שורות שהקולקטור באמת רענן בריצה האחרונה: סרטון שיצא מחלון
        # 30 הימים מפסיק להתעדכן, והדלתא האחרונה שלו קופאת בגיליון לנצח.
        # בלי הסינון הזה סרטון "תומר בר" מאפריל (דלתא קפואה +26K) ככב
        # בכל דוח יומי במשך חודשים.
        if 'last_updated' in old_videos.columns:
            old_videos = old_videos[old_videos['last_updated'].astype(str).str[:10] >= yesterday_date]
        if not old_videos.empty:
            # רק צבירה משמעותית: כמעט כל סרטון ישן צובר שאריות (חציון ~67
            # צפיות/יום על 269 סרטונים, נמדד 2026-07-14) - בלי רצפה הסקשן
            # מתמלא בזוטות בימים שקטים. אין מה לציין? עדיף לומר שאין.
            old_videos = old_videos[old_videos['views_delta'] >= 5000].sort_values('views_delta', ascending=False)
            for _, row in old_videos.head(3).iterrows():
                video_url = row.get('video_url', '')
                top_delta += f"• {_clean_title(row['title'])} | מ-{row['published_at']} | +{int(row['views_delta']):,} צפיות חדשות | LINK: {video_url}\n"
    
    return f"""סרטונים חדשים: {new_count}
סה"כ צפיות חדשות: {total_views_new:,}
סה"כ לייקים: {total_likes_new:,} | תגובות: {total_comments_new:,} | ממוצע like rate: {avg_like_rate}%

טופ מאתמול (כולל מעורבות):
{top_new if top_new else "אין סרטונים חדשים"}

סרטונים ישנים שממשיכים לצבור צפיות (5,000+ ביום; אם ריק - אל תזכיר סרטונים ישנים בכלל):
{top_delta if top_delta else "אין - אף סרטון ישן לא צבר צפיות משמעותיות אתמול"}"""


def summarize_facebook(df, yesterday_date):
    """יצירת סיכום פייסבוק לפרומפט - כולל מטריקות מעורבות לניתוח AI"""
    if df.empty:
        return "אין נתונים"
    
    # פוסטים מאתמול
    new_yesterday = df[df['date'] == yesterday_date].copy()
    new_count = len(new_yesterday)
    total_reach = int(new_yesterday['reach'].sum()) if not new_yesterday.empty else 0
    total_views = int(new_yesterday['views'].sum()) if not new_yesterday.empty else 0
    
    # מטריקות מעורבות מצטברות
    total_likes = int(new_yesterday['likes'].sum()) if 'likes' in new_yesterday.columns and not new_yesterday.empty else 0
    total_comments = int(new_yesterday['comments'].sum()) if 'comments' in new_yesterday.columns and not new_yesterday.empty else 0
    total_shares = int(new_yesterday['shares'].sum()) if 'shares' in new_yesterday.columns and not new_yesterday.empty else 0
    total_clicks = int(new_yesterday['clicks'].sum()) if 'clicks' in new_yesterday.columns and not new_yesterday.empty else 0
    
    # ממוצע engagement rate
    avg_engagement = 0
    if 'engagement_rate' in new_yesterday.columns and not new_yesterday.empty:
        avg_engagement = round(new_yesterday['engagement_rate'].mean(), 2)
    
    # טופ 5 לפי reach - עם מטריקות מעורבות לניתוח
    top_posts = ""
    if not new_yesterday.empty:
        new_yesterday = new_yesterday.sort_values('reach', ascending=False)
        for _, row in new_yesterday.head(5).iterrows():
            title = _clean_title(row.get('title', ''))
            likes = int(row.get('likes', 0))
            comments = int(row.get('comments', 0))
            shares = int(row.get('shares', 0))
            eng_rate = round(row.get('engagement_rate', 0), 1)
            # פורמט מורחב לניתוח AI
            permalink = row.get('permalink', '')
            top_posts += f"• {title} | {row.get('type', '')} | {int(row['reach']):,} reach | {int(row['views']):,} views | לייקים: {likes:,} | תגובות: {comments} | שיתופים: {shares} | מעורבות: {eng_rate}% | LINK: {permalink}\n"
    
    return f"""פוסטים חדשים: {new_count}
סה"כ Reach: {total_reach:,} | צפיות וידאו: {total_views:,}
מעורבות: {total_likes:,} לייקים | {total_comments:,} תגובות | {total_shares:,} שיתופים | {total_clicks:,} קליקים
ממוצע engagement rate: {avg_engagement}%

טופ פוסטים (כולל מעורבות):
{top_posts if top_posts else "אין פוסטים חדשים"}"""


def summarize_instagram(df, yesterday_date):
    """יצירת סיכום אינסטגרם לפרומפט - כולל מטריקות מעורבות לניתוח AI"""
    if df.empty:
        return "אין נתונים"
    
    # פוסטים מאתמול
    new_yesterday = df[df['date'] == yesterday_date].copy()
    new_count = len(new_yesterday)
    total_views = int(new_yesterday['views'].sum()) if not new_yesterday.empty else 0
    total_reach = int(new_yesterday['reach'].sum()) if not new_yesterday.empty else 0
    
    # מטריקות מעורבות מצטברות
    total_likes = int(new_yesterday['likes'].sum()) if 'likes' in new_yesterday.columns and not new_yesterday.empty else 0
    total_comments = int(new_yesterday['comments'].sum()) if 'comments' in new_yesterday.columns and not new_yesterday.empty else 0
    total_saved = int(new_yesterday['saved'].sum()) if 'saved' in new_yesterday.columns and not new_yesterday.empty else 0
    total_shares = int(new_yesterday['shares'].sum()) if 'shares' in new_yesterday.columns and not new_yesterday.empty else 0
    
    # ממוצע engagement rate
    avg_engagement = 0
    if 'engagement_rate' in new_yesterday.columns and not new_yesterday.empty:
        avg_engagement = round(new_yesterday['engagement_rate'].mean(), 2)
    
    # טופ 5 לפי views - עם מטריקות מעורבות לניתוח
    top_posts = ""
    if not new_yesterday.empty:
        new_yesterday = new_yesterday.sort_values('views', ascending=False)
        for _, row in new_yesterday.head(5).iterrows():
            caption = _clean_title(row.get('caption', ''))
            likes = int(row.get('likes', 0))
            comments = int(row.get('comments', 0))
            saved = int(row.get('saved', 0))
            shares = int(row.get('shares', 0))
            eng_rate = round(row.get('engagement_rate', 0), 1)
            # מדדי v25 לרילס: הוק (skip ב-3 שניות) + צפיות מפייסבוק דרך crosspost
            extra = ""
            skip = row.get('skip_rate', 0) or 0
            fb_v = int(row.get('fb_views', 0) or 0)
            if skip:
                extra += f" | Skip 3ש': {skip}%"
            if fb_v:
                extra += f" | צפיות בפייסבוק (crosspost): {fb_v:,}"
            # פורמט מורחב לניתוח AI
            permalink = row.get('permalink', '')
            top_posts += f"• {caption} | {row.get('type', '')} | {int(row['views']):,} views | {int(row['reach']):,} reach | לייקים: {likes:,} | תגובות: {comments} | שמירות: {saved} | שיתופים: {shares} | מעורבות: {eng_rate}%{extra} | LINK: {permalink}\n"
    
    return f"""פוסטים חדשים: {new_count}
סה"כ צפיות: {total_views:,} | Reach: {total_reach:,}
מעורבות: {total_likes:,} לייקים | {total_comments:,} תגובות | {total_saved:,} שמירות | {total_shares:,} שיתופים
ממוצע engagement rate: {avg_engagement}%

טופ פוסטים (כולל מעורבות):
{top_posts if top_posts else "אין פוסטים חדשים"}"""


def summarize_tiktok(df, yesterday_date):
    """יצירת סיכום טיקטוק לפרומפט - כולל מטריקות מעורבות לניתוח AI"""
    if df.empty:
        return "אין נתונים"

    new_yesterday = df[df['date'] == yesterday_date].copy()
    new_count = len(new_yesterday)
    total_views = int(new_yesterday['views'].sum()) if not new_yesterday.empty else 0
    total_likes = int(new_yesterday['likes'].sum()) if 'likes' in new_yesterday.columns and not new_yesterday.empty else 0
    total_comments = int(new_yesterday['comments'].sum()) if 'comments' in new_yesterday.columns and not new_yesterday.empty else 0
    total_shares = int(new_yesterday['shares'].sum()) if 'shares' in new_yesterday.columns and not new_yesterday.empty else 0
    total_wa = int(new_yesterday['whatsapp_shares'].sum()) if 'whatsapp_shares' in new_yesterday.columns and not new_yesterday.empty else 0
    total_saves = int(new_yesterday['saves'].sum()) if 'saves' in new_yesterday.columns and not new_yesterday.empty else 0

    avg_engagement = 0
    if 'engagement_rate' in new_yesterday.columns and not new_yesterday.empty:
        avg_engagement = round(new_yesterday['engagement_rate'].mean(), 2)

    # טופ 5 לפי views - עם מטריקות מעורבות לניתוח
    top_posts = ""
    if not new_yesterday.empty:
        new_yesterday = new_yesterday.sort_values('views', ascending=False)
        for _, row in new_yesterday.head(5).iterrows():
            title = _clean_title(row.get('title', ''))
            likes = int(row.get('likes', 0))
            comments = int(row.get('comments', 0))
            shares = int(row.get('shares', 0))
            wa = int(row.get('whatsapp_shares', 0))
            saves = int(row.get('saves', 0))
            eng_rate = round(row.get('engagement_rate', 0), 1)
            permalink = row.get('permalink', '')
            top_posts += f"• {title} | {int(row['views']):,} views | לייקים: {likes:,} | תגובות: {comments} | שיתופים: {shares} (מתוכם {wa} לוואטסאפ) | שמירות: {saves} | מעורבות: {eng_rate}% | LINK: {permalink}\n"

    # סרטונים ישנים (2-7 ימים, חלון האיסוף) שממשיכים לצבור צפיות.
    # רצפה 25K: סרטון טיקטוק ממוצע אצלנו עושה ~130K כוללות, כך שדלתא יומית
    # של 25K+ על סרטון ישן היא באמת המשך-הדהוד ולא שאריות אלגוריתם.
    top_delta = ""
    if 'views_delta' in df.columns:
        old_videos = df[df['date'] < yesterday_date].copy()
        if 'pulled_at' in old_videos.columns:
            old_videos = old_videos[old_videos['pulled_at'].astype(str).str[:10] >= yesterday_date]
        if not old_videos.empty:
            old_videos = old_videos[old_videos['views_delta'] >= 25000].sort_values('views_delta', ascending=False)
            for _, row in old_videos.head(3).iterrows():
                permalink = row.get('permalink', '')
                top_delta += f"• {row.get('title', '')} | מ-{row['date']} | +{int(row['views_delta']):,} צפיות חדשות | LINK: {permalink}\n"

    return f"""סרטונים חדשים: {new_count}
סה"כ צפיות: {total_views:,}
מעורבות: {total_likes:,} לייקים | {total_comments:,} תגובות | {total_shares:,} שיתופים (מתוכם {total_wa:,} לוואטסאפ) | {total_saves:,} שמירות
ממוצע engagement rate: {avg_engagement}%

טופ סרטונים (כולל מעורבות):
{top_posts if top_posts else "אין סרטונים חדשים"}

סרטונים ישנים שממשיכים לצבור צפיות (25,000+ ביום; אם ריק - אל תזכיר סרטונים ישנים בכלל):
{top_delta if top_delta else "אין - אף סרטון ישן לא צבר צפיות משמעותיות אתמול"}"""


def get_followers_summary(df):
    """יצירת סיכום עוקבים"""
    if df.empty:
        return "אין נתוני עוקבים"
    
    # לוקחים את השורה האחרונה (הכי עדכנית)
    latest = df.iloc[-1] if len(df) > 0 else {}
    
    yt = latest.get('yt_subscribers', 0)
    fb = latest.get('fb_followers', 0)
    ig = latest.get('ig_followers', 0)
    tt = latest.get('tt_followers', 0) or 0

    return f"YouTube: {int(yt):,} | Facebook: {int(fb):,} | Instagram: {int(ig):,} | TikTok: {int(tt):,}"


def analyze_all_platforms_with_gemini(youtube_summary, facebook_summary, instagram_summary,
                                       tiktok_summary, followers_summary, baseline_summary,
                                       yesterday_date, report_time):
    """ניתוח מאוחד של כל הפלטפורמות עם Gemini"""
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key: 
        return "⚠️ חסר מפתח ל-Gemini."

    client = genai.Client(api_key=api_key)
    
    today_date = datetime.now(pytz.timezone('Asia/Jerusalem')).strftime('%d/%m/%Y')
    
    prompt = f"""אתה מנתח ביצועי רשתות חברתיות של כאן חדשות. התאריך: {today_date}.

=== ⚠️ הקשר חשוב - YouTube ===
סרטוני המהדורה עולים ב-20:00-21:00 בערב. לכן:
- סרטון מאתמול בערב קיבל רוב הצפיות שלו היום בבוקר - זה המחזור הטבעי שלו, לא הפתעה
- רק סרטונים מ-3+ ימים אחורה שצוברים דלתא גבוהה הם באמת "ממשיכים להדהד"
- אל תתלהב מסרטונים "ישנים" של יום-יומיים - זה פשוט איך YouTube עובד אצלנו

=== 📊 נתונים (כולל מטריקות מעורבות לניתוח) ===

📺 YouTube:
{youtube_summary}

📘 Facebook:
{facebook_summary}

📷 Instagram:
{instagram_summary}

🎵 TikTok:
{tiktok_summary}

📊 עוקבים:
{followers_summary}

=== 📐 בסיס להשוואה - ממוצע יומי בשבוע האחרון ===
{baseline_summary}
כשאתה שופט אם יום/פוסט "הצליח" - השווה למספרים האלה, לא לתחושת בטן.
יום שעקף את הממוצע השבועי משמעותית = יום חזק; יום מתחתיו = חלש, וזה בסדר לומר.

=== 📝 מבנה הדוח ===

**כתוב דוח תמציתי וקריא.** הנתונים שקיבלת כוללים מטריקות מעורבות (לייקים, תגובות, שיתופים) - השתמש בהם לתובנות, אבל **אל תציג את כולם** ברשימת המובילים.

🏆 ההצלחה של היום
━━━━━━━━━━━━━━━━━
2-3 משפטים: מה הסיפור/תוכן שהצליח הכי טוב? אם הצליח בכמה פלטפורמות - ציין.
**אם יש מעורבות חריגה (הרבה לייקים/תגובות/שיתופים יחסית לצפיות) - ציין זאת כאן.**

📺 YouTube
━━━━━━━━━━━━━━━━━
- כמה סרטונים | כמה צפיות חדשות
- מוביל 1: [שם](LINK) | סוג | צפיות
- מוביל 2: [שם](LINK) | סוג | צפיות
- מוביל 3: [שם](LINK) | סוג | צפיות
💡 תובנה במשפט אחד (אפשר להזכיר מעורבות חריגה אם יש)

📘 Facebook
━━━━━━━━━━━━━━━━━
- כמה פוסטים | reach כולל
- מוביל 1: [שם](LINK) | סוג | reach
- מוביל 2: [שם](LINK) | סוג | reach
- מוביל 3: [שם](LINK) | סוג | reach
💡 תובנה במשפט אחד (אפשר להזכיר מעורבות/שיתופים חריגים אם יש)

📷 Instagram
━━━━━━━━━━━━━━━━━
- כמה פוסטים | צפיות כולל
- מוביל 1: [שם](LINK) | סוג | views
- מוביל 2: [שם](LINK) | סוג | views
- מוביל 3: [שם](LINK) | סוג | views
💡 תובנה במשפט אחד (אפשר להזכיר שמירות/תגובות חריגות אם יש)

🎵 TikTok
━━━━━━━━━━━━━━━━━
- כמה סרטונים | צפיות כולל
- מוביל 1: [שם](LINK) | views
- מוביל 2: [שם](LINK) | views
- מוביל 3: [שם](LINK) | views
💡 תובנה במשפט אחד (שיתופים לוואטסאפ הם מדד ייחודי לטיקטוק - אם יש סרטון עם הרבה שיתופי וואטסאפ, זה סימן שהסיפור "עובר בקבוצות" - שווה לציין)

🔥 3 תובנות חוצות פלטפורמות
━━━━━━━━━━━━━━━━━
בחר 3 תובנות מעניינות מהנתונים - דברים שמפתיעים או שווה לשים לב אליהם.

**בחר 3 מתוך האפשרויות (או תן תובנה אחרת שמצאת):**
📊 סיפור שהצליח בכמה פלטפורמות - איפה יותר ולמה?
⚡ הפתעה - תוכן שהצליח/נכשל מעבר לצפוי
🎬 פער בין פורמטים - Reels vs תמונות vs Shorts
👥 פער בין קהלים - התנהגות שונה בין הפלטפורמות
📈 מגמה או נושא שחוזר על עצמו
🔄 שינוי מימים קודמים - משהו חריג או מעניין
💡 הזדמנות - תוכן שאפשר לשכפל או להתאים
🤔 שאלה פתוחה - משהו ששווה לבדוק לעומק
❤️ מעורבות חריגה - תוכן שקיבל הרבה לייקים/תגובות/שיתופים יחסית לצפיות

פורמט:
• [אימוג'י] תובנה קצרה ב-1-2 משפטים
• [אימוג'י] תובנה קצרה ב-1-2 משפטים
• [אימוג'י] תובנה קצרה ב-1-2 משפטים

**חשוב:** 
- כל תובנה ב-1-2 משפטים קצרים בלבד
- התובנות חייבות להיות על נושאים שונים
- אל תכתוב משהו שכבר ברור מהמספרים למעלה

=== ⚙️ כללים קריטיים ===

**חשוב - לינקים:**
- הנתונים שקיבלת כוללים LINK: לכל פריט
- כשאתה מציג מוביל, השתמש בפורמט Markdown: [כותרת](LINK)
- דוגמה: ["זה היה מרחץ דמים": ארסן](https://youtube.com/watch?v=xyz)
- אל תשנה את LINK - העתק אותו בדיוק כפי שהוא
**תובנות מבוססות נתונים:**
✅ "פוסט X הגיע ל-Y reach, פי Z יותר מהמוביל השני" ← טוב, ספציפי
✅ "הסרטון על X קיבל 5% like rate, פי 2 מהממוצע" ← טוב, מבוסס נתונים
❌ "נראה שהאלגוריתם מעדיף תוכן ביטחוני" ← רע, השערה כללית
❌ "הקהל אוהב תוכן דרמטי" ← רע, לא מבוסס על הנתונים הספציפיים

**YouTube Timing:**
✅ "סרטון מאתמול בערב הוביל כצפוי במחזור הטבעי שלו" ← טוב
❌ "סרטון מלפני יומיים מפתיע וממשיך להצליח" ← רע, זה לא מפתיע

**כללי:**
- התחל ישר מ-🏆 בלי הקדמה
- השתמש בקווי ━━━ להפרדה
- שמור על bullet points קצרים וקריאים
- אורך הדוח כולו: עד 3,000 תווים - דוח ארוך יותר נחתך במגבלת טלגרם ומאבד את סוף התובנות
- אם הכל רגיל/שגרתי - אל תמציא תובנות מלאכותיות
- אל תמציא נתונים
"""

    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text=prompt),
            ],
        ),
    ]
    
    # Try primary model first, fallback to secondary if it fails
    models_to_try = ["gemini-3.1-pro-preview", "gemini-2.5-pro"]
    
    for model_name in models_to_try:
        try:
            print(f"   Trying model: {model_name}")
            
            response_text = ""
            for chunk in client.models.generate_content_stream(
                model=model_name,
                contents=contents,
            ):
                if chunk.text:
                    response_text += chunk.text
            
            if response_text:
                return response_text
                
        except Exception as e:
            print(f"   Model {model_name} failed: {e}")
            continue
    
    return "שגיאה: לא הצלחתי לייצר את הדוח. נסו שוב מאוחר יותר."


def extract_cross_platform_insights(report_text):
    """חילוץ קטע התובנות החוצות מהדוח.

    הכותרת מיוצרת ע"י Gemini ולכן הניסוח המדויק זז מיום ליום (אימוג'י,
    המספר "3", רווחים, מקף). התאמה מדויקת של מחרוזת קבועה נשברה בשקט והפסיקה
    לשמור תובנות לגיליון. לכן מזהים את שורת הכותרת לפי הליבה "תובנות חוצות"
    בלבד — סובלני לניסוח — ומשם ממשיכים כרגיל.
    """
    lines = report_text.split('\n')

    # איתור שורת הכותרת של קטע התובנות החוצות (סובלני לניסוח)
    header_idx = next(
        (i for i, line in enumerate(lines) if 'תובנות חוצות' in line),
        None,
    )
    if header_idx is None:
        return ""

    # מסיר את הכותרת ואת קו ההפרדה שאחריה
    clean_lines = []
    skip_next_separator = True
    for line in lines[header_idx + 1:]:  # דולג על הכותרת
        if skip_next_separator and '━' in line:
            skip_next_separator = False
            continue
        if line.strip():
            clean_lines.append(line.strip())

    return '\n'.join(clean_lines[:10])  # מקסימום 10 שורות


def save_daily_insights_to_sheets(report_text, report_date):
    """
    שמירת התובנות היומיות לגיליון נפרד לטובת הדוח השבועי.
    הגיליון ייווצר אוטומטית בריצה הראשונה.
    """
    try:
        gc = get_sheet_client()
        sh = gc.open_by_url(SPREADSHEET_URL)
        
        # נסיון לפתוח את הגיליון, אם לא קיים - יצירה
        try:
            worksheet = sh.worksheet("תובנות יומיות")
        except:
            print("   Creating 'תובנות יומיות' worksheet...")
            worksheet = sh.add_worksheet(title="תובנות יומיות", rows=500, cols=3)
            # הוספת כותרות
            worksheet.update('A1', [['date', 'insights', 'timestamp']])
        
        # חילוץ התובנות
        insights = extract_cross_platform_insights(report_text)
        
        if not insights:
            print("   ⚠️ No insights found to save")
            return False
        
        # הוספת שורה חדשה
        il_tz = pytz.timezone('Asia/Jerusalem')
        timestamp = datetime.now(il_tz).strftime('%Y-%m-%d %H:%M')
        
        worksheet.append_row([report_date, insights, timestamp])
        print(f"   ✅ Saved insights for {report_date}")
        return True
        
    except Exception as e:
        print(f"   ⚠️ Failed to save insights: {e}")
        return False


def send_telegram_message(message):
    """שליחת הודעה לטלגרם"""
    token = os.environ.get('TELEGRAM_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    
    if not token or not chat_id:
        print("⚠️ Skipping Telegram - missing credentials.")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    # Telegram message limit is 4096 characters
    if len(message) > 4000:
        print(f"⚠️ Message too long ({len(message)} chars), truncating...")
        message = message[:3900] + "\n\n... (הדוח קוצר עקב מגבלת אורך)"
    
    payload = {
        "chat_id": chat_id, 
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try: 
        response = requests.post(url, json=payload)
        print(f"Telegram response: {response.status_code}")
        if response.status_code != 200:
            print(f"   Error details: {response.text[:200]}")
        return response.status_code == 200
    except Exception as e: 
        print(f"Telegram Error: {e}")
        return False


def generate_unified_report():
    """יצירת ושליחת דוח מאוחד לכל הפלטפורמות"""
    il_tz = pytz.timezone('Asia/Jerusalem')
    now = datetime.now(il_tz)
    yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    report_time = now.strftime('%H:%M')
    
    print(f"\n{'='*60}")
    print(f"📊 Unified Social Report - {now.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")
    
    # שליפת נתונים מכל הפלטפורמות
    print("📺 Fetching YouTube data...")
    youtube_df = get_youtube_data()
    print(f"   Found {len(youtube_df)} videos")
    
    print("📘 Fetching Facebook data...")
    facebook_df = get_facebook_data()
    print(f"   Found {len(facebook_df)} posts")
    
    print("📷 Fetching Instagram data...")
    instagram_df = get_instagram_data()
    print(f"   Found {len(instagram_df)} posts")
    
    print("🎵 Fetching TikTok data...")
    tiktok_df = get_tiktok_data()
    print(f"   Found {len(tiktok_df)} videos")

    print("📊 Fetching followers data...")
    followers_df = get_followers_data()
    print(f"   Found {len(followers_df)} rows")

    # יצירת סיכומים
    print("\n📝 Creating summaries...")
    youtube_summary = summarize_youtube(youtube_df, yesterday)
    facebook_summary = summarize_facebook(facebook_df, yesterday)
    instagram_summary = summarize_instagram(instagram_df, yesterday)
    tiktok_summary = summarize_tiktok(tiktok_df, yesterday)
    followers_summary = get_followers_summary(followers_df)

    today = now.strftime('%Y-%m-%d')
    baseline_summary = (
        f"YouTube: {_daily_baseline(youtube_df, 'published_at', 'views', today):,} צפיות/יום | "
        f"Facebook: {_daily_baseline(facebook_df, 'date', 'reach', today):,} reach/יום | "
        f"Instagram: {_daily_baseline(instagram_df, 'date', 'views', today):,} צפיות/יום | "
        f"TikTok: {_daily_baseline(tiktok_df, 'date', 'views', today):,} צפיות/יום"
    )

    # ניתוח עם Gemini
    print("\n🤖 Analyzing with Gemini...")
    report = analyze_all_platforms_with_gemini(
        youtube_summary,
        facebook_summary,
        instagram_summary,
        tiktok_summary,
        followers_summary,
        baseline_summary,
        yesterday,
        report_time
    )
    
    # הוספת כותרת
    header = f"📊 *דוח רשתות חברתיות יומי - כאן חדשות*\n{now.strftime('%d/%m/%Y')} | נוצר ב-{report_time}\n\n"
    full_report = header + report
    
    # שליחה לטלגרם
    print("\n📨 Sending to Telegram...")
    success = send_telegram_message(full_report)
    
    if success:
        print("✅ Unified report sent successfully!")
        
        # שמירת התובנות היומיות לגיליון לטובת הדוח השבועי
        print("\n💾 Saving daily insights...")
        save_daily_insights_to_sheets(full_report, yesterday)
    else:
        print("⚠️ Failed to send report")
        print("\n--- Report Preview ---")
        print(full_report[:1000])


if __name__ == "__main__":
    generate_unified_report()

