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
            if 'views_delta' in df.columns:
                df['views_delta'] = pd.to_numeric(df['views_delta'], errors='coerce').fillna(0)
        return df
    except Exception as e:
        print(f"Error fetching Instagram data: {e}")
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
    """יצירת סיכום יוטיוב לפרומפט"""
    if df.empty:
        return "אין נתונים"
    
    # סרטונים חדשים מאתמול
    new_yesterday = df[df['published_at'] == yesterday_date].copy()
    new_count = len(new_yesterday)
    total_views_new = int(new_yesterday['views'].sum()) if not new_yesterday.empty else 0
    
    # טופ 5 מאתמול
    top_new = ""
    if not new_yesterday.empty:
        new_yesterday = new_yesterday.sort_values('views', ascending=False)
        for _, row in new_yesterday.head(5).iterrows():
            top_new += f"• {row['title'][:60]} | {row.get('video_type', 'רגיל')} | {int(row['views']):,} צפיות\n"
    
    # סרטונים ישנים עם דלתא גבוהה
    top_delta = ""
    if 'views_delta' in df.columns:
        old_videos = df[df['published_at'] < yesterday_date].copy()
        if not old_videos.empty:
            old_videos = old_videos[old_videos['views_delta'] > 0].sort_values('views_delta', ascending=False)
            for _, row in old_videos.head(3).iterrows():
                top_delta += f"• {row['title'][:50]} | מ-{row['published_at']} | +{int(row['views_delta']):,} צפיות חדשות\n"
    
    return f"""סרטונים חדשים: {new_count}
סה"כ צפיות חדשות: {total_views_new:,}

טופ מאתמול:
{top_new if top_new else "אין סרטונים חדשים"}

סרטונים ישנים שממשיכים לצבור צפיות:
{top_delta if top_delta else "אין מידע"}"""


def summarize_facebook(df, yesterday_date):
    """יצירת סיכום פייסבוק לפרומפט"""
    if df.empty:
        return "אין נתונים"
    
    # פוסטים מאתמול
    new_yesterday = df[df['date'] == yesterday_date].copy()
    new_count = len(new_yesterday)
    total_reach = int(new_yesterday['reach'].sum()) if not new_yesterday.empty else 0
    total_views = int(new_yesterday['views'].sum()) if not new_yesterday.empty else 0
    
    # טופ 5 לפי reach
    top_posts = ""
    if not new_yesterday.empty:
        new_yesterday = new_yesterday.sort_values('reach', ascending=False)
        for _, row in new_yesterday.head(5).iterrows():
            title = (row.get('title', '') or '')[:50]
            top_posts += f"• {title} | {row.get('type', '')} | {int(row['reach']):,} reach | {int(row['views']):,} views\n"
    
    return f"""פוסטים חדשים: {new_count}
סה"כ Reach: {total_reach:,}
סה"כ צפיות וידאו: {total_views:,}

טופ פוסטים:
{top_posts if top_posts else "אין פוסטים חדשים"}"""


def summarize_instagram(df, yesterday_date):
    """יצירת סיכום אינסטגרם לפרומפט"""
    if df.empty:
        return "אין נתונים"
    
    # פוסטים מאתמול
    new_yesterday = df[df['date'] == yesterday_date].copy()
    new_count = len(new_yesterday)
    total_views = int(new_yesterday['views'].sum()) if not new_yesterday.empty else 0
    total_reach = int(new_yesterday['reach'].sum()) if not new_yesterday.empty else 0
    
    # טופ 5 לפי views
    top_posts = ""
    if not new_yesterday.empty:
        new_yesterday = new_yesterday.sort_values('views', ascending=False)
        for _, row in new_yesterday.head(5).iterrows():
            caption = (row.get('caption', '') or '')[:50]
            top_posts += f"• {caption} | {row.get('type', '')} | {int(row['views']):,} views | {int(row['reach']):,} reach\n"
    
    return f"""פוסטים חדשים: {new_count}
סה"כ צפיות: {total_views:,}
סה"כ Reach: {total_reach:,}

טופ פוסטים:
{top_posts if top_posts else "אין פוסטים חדשים"}"""


def get_followers_summary(df):
    """יצירת סיכום עוקבים"""
    if df.empty:
        return "אין נתוני עוקבים"
    
    # לוקחים את השורה האחרונה (הכי עדכנית)
    latest = df.iloc[-1] if len(df) > 0 else {}
    
    yt = latest.get('yt_subscribers', 0)
    fb = latest.get('fb_followers', 0)
    ig = latest.get('ig_followers', 0)
    
    return f"YouTube: {int(yt):,} | Facebook: {int(fb):,} | Instagram: {int(ig):,}"


def analyze_all_platforms_with_gemini(youtube_summary, facebook_summary, instagram_summary, 
                                       followers_summary, yesterday_date, report_time):
    """ניתוח מאוחד של כל הפלטפורמות עם Gemini"""
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key: 
        return "⚠️ חסר מפתח ל-Gemini."

    client = genai.Client(api_key=api_key)
    
    today_date = datetime.now(pytz.timezone('Asia/Jerusalem')).strftime('%d/%m/%Y')
    
    prompt = f"""אתה מנתח ביצועי רשתות חברתיות של כאן חדשות. התאריך: {today_date}.
הדוח נוצר ב-{report_time}.

=== נתונים ===

📺 YouTube (נתונים עד עכשיו - סרטוני המהדורה עולים אחרי 20:00 וצוברים צפיות בעיקר בבוקר):
{youtube_summary}

📘 Facebook:
{facebook_summary}

📷 Instagram:
{instagram_summary}

📊 עוקבים:
{followers_summary}

=== מבנה הדוח ===

כתוב דוח מסודר ונקי לקריאה. השתמש בקווי הפרדה (━━━) בין סעיפים.

🏆 ההצלחה של היום
━━━━━━━━━━━━━━━━━
2-3 משפטים: מה הסיפור/תוכן שהצליח הכי טוב? אם הצליח בכמה פלטפורמות - ציין.

📺 YouTube
━━━━━━━━━━━━━━━━━
• כמה סרטונים | כמה צפיות חדשות
• מוביל 1: שם קצר | סוג | צפיות
• מוביל 2: שם קצר | סוג | צפיות
• מוביל 3: שם קצר | סוג | צפיות
💡 תובנה במשפט אחד

📘 Facebook
━━━━━━━━━━━━━━━━━
• כמה פוסטים | reach כולל
• מוביל 1: שם קצר | סוג | reach
• מוביל 2: שם קצר | סוג | reach
• מוביל 3: שם קצר | סוג | reach
💡 תובנה במשפט אחד

📷 Instagram
━━━━━━━━━━━━━━━━━
• כמה פוסטים | צפיות כולל
• מוביל 1: שם קצר | סוג | views
• מוביל 2: שם קצר | סוג | views
• מוביל 3: שם קצר | סוג | views
💡 תובנה במשפט אחד

🔥 תובנות חוצות פלטפורמות
━━━━━━━━━━━━━━━━━
בחר 3 תובנות מעניינות מהנתונים - דברים שמפתיעים או שווה לשים לב אליהם.

**חשוב:** 
- כל תובנה ב-1-2 משפטים קצרים בלבד
- התובנות חייבות להיות על נושאים שונים
- אל תכתוב משהו שכבר ברור מהמספרים למעלה

**בחר 3 מתוך האפשרויות (או תן תובנה אחרת שמצאת):**
📊 סיפור שהצליח בכמה פלטפורמות - איפה יותר ולמה?
⚡ הפתעה - תוכן שהצליח/נכשל מעבר לצפוי
🎬 פער בין פורמטים - Reels vs תמונות vs Shorts
👥 פער בין קהלים - התנהגות שונה בין הפלטפורמות
📈 מגמה או נושא שחוזר על עצמו
🔄 שינוי מימים קודמים - משהו חריג או מעניין
💡 הזדמנות - תוכן שאפשר לשכפל או להתאים
🤔 שאלה פתוחה - משהו ששווה לבדוק לעומק

פורמט:
• [אימוג'י] תובנה קצרה ב-1-2 משפטים
• [אימוג'י] תובנה קצרה ב-1-2 משפטים
• [אימוג'י] תובנה קצרה ב-1-2 משפטים

=== סגנון ===
- התחל ישר מ-🏆 בלי הקדמה
- השתמש בקווי ━━━ להפרדה
- שמור על bullet points קצרים וקריאים
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
    models_to_try = ["gemini-3-pro-preview", "gemini-2.5-pro"]
    
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
    
    payload = {"chat_id": chat_id, "text": message}
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
    
    print("📊 Fetching followers data...")
    followers_df = get_followers_data()
    print(f"   Found {len(followers_df)} rows")
    
    # יצירת סיכומים
    print("\n📝 Creating summaries...")
    youtube_summary = summarize_youtube(youtube_df, yesterday)
    facebook_summary = summarize_facebook(facebook_df, yesterday)
    instagram_summary = summarize_instagram(instagram_df, yesterday)
    followers_summary = get_followers_summary(followers_df)
    
    # ניתוח עם Gemini
    print("\n🤖 Analyzing with Gemini...")
    report = analyze_all_platforms_with_gemini(
        youtube_summary, 
        facebook_summary, 
        instagram_summary,
        followers_summary,
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
    else:
        print("⚠️ Failed to send report")
        print("\n--- Report Preview ---")
        print(full_report[:1000])


if __name__ == "__main__":
    generate_unified_report()
