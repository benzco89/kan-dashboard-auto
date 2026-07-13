"""
Weekly Reporter - דוח שבועי מבוסס נתונים + תובנות יומיות
רץ כל יום ראשון ב-10:00 ישראל
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
    pass

# --- הגדרות ---
SPREADSHEET_ID = "1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


def get_sheet_client():
    creds_json = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    return gspread.authorize(creds)


def get_weekly_data(sheet_name, date_column, days_back=7):
    """משיכת נתונים של X ימים אחרונים"""
    try:
        gc = get_sheet_client()
        sh = gc.open_by_key(SPREADSHEET_ID)
        worksheet = sh.worksheet(sheet_name)
        df = pd.DataFrame(worksheet.get_all_records())
        
        if df.empty:
            return pd.DataFrame()
        
        # סינון לפי תאריך
        cutoff = (datetime.now(pytz.timezone('Asia/Jerusalem')) - timedelta(days=days_back)).strftime('%Y-%m-%d')
        df = df[df[date_column] >= cutoff]
        
        return df
    except Exception as e:
        print(f"Error fetching {sheet_name}: {e}")
        return pd.DataFrame()


def get_daily_insights(days_back=7):
    """משיכת התובנות היומיות של השבוע"""
    try:
        gc = get_sheet_client()
        sh = gc.open_by_key(SPREADSHEET_ID)
        
        try:
            worksheet = sh.worksheet("תובנות יומיות")
        except:
            print("   ⚠️ No 'תובנות יומיות' worksheet found")
            return []
            
        df = pd.DataFrame(worksheet.get_all_records())
        
        if df.empty:
            return []
        
        # סינון ל-7 ימים אחרונים
        cutoff = (datetime.now(pytz.timezone('Asia/Jerusalem')) - timedelta(days=days_back)).strftime('%Y-%m-%d')
        df = df[df['date'] >= cutoff]
        
        # מיון לפי תאריך
        df = df.sort_values('date')
        
        # החזרת רשימה של (תאריך, תובנות)
        return [(row['date'], row['insights']) for _, row in df.iterrows()]
        
    except Exception as e:
        print(f"⚠️ Error fetching daily insights: {e}")
        return []


def calculate_weekly_stats(yt_df, fb_df, ig_df):
    """חישוב סטטיסטיקות שבועיות"""
    stats = {}
    
    # YouTube
    if not yt_df.empty:
        # המרת עמודות למספרים
        for col in ['views', 'likes', 'comments']:
            if col in yt_df.columns:
                yt_df[col] = pd.to_numeric(yt_df[col], errors='coerce').fillna(0)
        
        stats['yt_total_videos'] = len(yt_df)
        stats['yt_total_views'] = int(yt_df['views'].sum())
        stats['yt_total_likes'] = int(yt_df['likes'].sum()) if 'likes' in yt_df.columns else 0
        
        # Shorts vs רגיל
        if 'video_type' in yt_df.columns:
            shorts_count = len(yt_df[yt_df['video_type'] == 'Shorts'])
            stats['yt_shorts_pct'] = round(shorts_count / len(yt_df) * 100, 1) if len(yt_df) > 0 else 0
            
            shorts_views = yt_df[yt_df['video_type'] == 'Shorts']['views'].sum()
            stats['yt_shorts_views_pct'] = round(shorts_views / stats['yt_total_views'] * 100, 1) if stats['yt_total_views'] > 0 else 0
        
        # טופ 5
        stats['yt_top_5'] = yt_df.nlargest(5, 'views')[['title', 'video_type', 'views']].to_dict('records') if 'video_type' in yt_df.columns else []
    
    # Facebook
    if not fb_df.empty:
        # המרת עמודות למספרים
        for col in ['reach', 'views', 'likes', 'comments', 'shares']:
            if col in fb_df.columns:
                fb_df[col] = pd.to_numeric(fb_df[col], errors='coerce').fillna(0)
        
        stats['fb_total_posts'] = len(fb_df)
        stats['fb_total_reach'] = int(fb_df['reach'].sum())
        stats['fb_total_likes'] = int(fb_df['likes'].sum()) if 'likes' in fb_df.columns else 0
        stats['fb_total_shares'] = int(fb_df['shares'].sum()) if 'shares' in fb_df.columns else 0
        
        # סוגי פוסטים
        if 'type' in fb_df.columns:
            by_type = fb_df.groupby('type')['reach'].sum().sort_values(ascending=False)
            stats['fb_best_format'] = by_type.index[0] if len(by_type) > 0 else "N/A"
            stats['fb_format_breakdown'] = by_type.head(3).to_dict()
        
        # טופ 5
        stats['fb_top_5'] = fb_df.nlargest(5, 'reach')[['title', 'type', 'reach']].to_dict('records') if 'type' in fb_df.columns else []
    
    # Instagram
    if not ig_df.empty:
        # המרת עמודות למספרים
        for col in ['views', 'reach', 'likes', 'comments', 'saved']:
            if col in ig_df.columns:
                ig_df[col] = pd.to_numeric(ig_df[col], errors='coerce').fillna(0)
        
        stats['ig_total_posts'] = len(ig_df)
        stats['ig_total_views'] = int(ig_df['views'].sum())
        stats['ig_total_likes'] = int(ig_df['likes'].sum()) if 'likes' in ig_df.columns else 0
        stats['ig_total_saved'] = int(ig_df['saved'].sum()) if 'saved' in ig_df.columns else 0
        
        # סוגי פוסטים
        if 'type' in ig_df.columns:
            by_type = ig_df.groupby('type')['views'].sum().sort_values(ascending=False)
            stats['ig_best_format'] = by_type.index[0] if len(by_type) > 0 else "N/A"
        
        # טופ 5
        stats['ig_top_5'] = ig_df.nlargest(5, 'views')[['caption', 'type', 'views']].to_dict('records') if 'type' in ig_df.columns else []
    
    return stats


def format_stats_for_prompt(stats):
    """המרת הסטטיסטיקות לטקסט לפרומפט"""
    text = ""
    
    # YouTube
    if 'yt_total_videos' in stats:
        text += f"""
**YouTube:**
- {stats['yt_total_videos']} סרטונים | {stats['yt_total_views']:,} צפיות | {stats['yt_total_likes']:,} לייקים
- Shorts: {stats.get('yt_shorts_pct', 0)}% מהסרטונים → {stats.get('yt_shorts_views_pct', 0)}% מהצפיות
- טופ 5 סרטונים:
"""
        for i, video in enumerate(stats.get('yt_top_5', []), 1):
            title = str(video.get('title', ''))[:50]
            text += f"  {i}. {title} | {video.get('video_type', '')} | {int(video.get('views', 0)):,}\n"
    
    # Facebook
    if 'fb_total_posts' in stats:
        text += f"""
**Facebook:**
- {stats['fb_total_posts']} פוסטים | {stats['fb_total_reach']:,} reach | {stats['fb_total_likes']:,} לייקים | {stats['fb_total_shares']:,} שיתופים
- פורמט מוביל: {stats.get('fb_best_format', 'N/A')}
- טופ 5 פוסטים:
"""
        for i, post in enumerate(stats.get('fb_top_5', []), 1):
            title = str(post.get('title', '') or '')[:50]
            text += f"  {i}. {title} | {post.get('type', '')} | {int(post.get('reach', 0)):,}\n"
    
    # Instagram
    if 'ig_total_posts' in stats:
        text += f"""
**Instagram:**
- {stats['ig_total_posts']} פוסטים | {stats['ig_total_views']:,} צפיות | {stats['ig_total_likes']:,} לייקים | {stats['ig_total_saved']:,} שמירות
- פורמט מוביל: {stats.get('ig_best_format', 'N/A')}
- טופ 5 פוסטים:
"""
        for i, post in enumerate(stats.get('ig_top_5', []), 1):
            caption = str(post.get('caption', '') or '')[:50]
            text += f"  {i}. {caption} | {post.get('type', '')} | {int(post.get('views', 0)):,}\n"
    
    return text


def format_daily_insights(insights_list):
    """פורמט התובנות היומיות"""
    if not insights_list:
        return "לא נמצאו תובנות יומיות לשבוע זה."
    
    text = ""
    for date, insights in insights_list:
        if insights and str(insights).strip():
            # פורמט התאריך לקריא יותר
            try:
                date_obj = datetime.strptime(date, '%Y-%m-%d')
                date_formatted = date_obj.strftime('%d/%m')
            except:
                date_formatted = date
            text += f"\n**{date_formatted}:**\n{str(insights)[:300]}\n"
    
    return text if text else "לא נמצאו תובנות יומיות."


def analyze_weekly_with_gemini(stats_text, daily_insights_text, week_start, week_end):
    """ניתוח שבועי עם Gemini"""
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        return "⚠️ חסר מפתח ל-Gemini."
    
    client = genai.Client(api_key=api_key)
    
    prompt = f"""אתה מנתח ביצועי רשתות חברתיות של כאן חדשות. כתוב דוח שבועי.

=== 📊 נתונים שבועיים ===
תקופה: {week_start} עד {week_end}

{stats_text}

=== 💡 תובנות שזיהינו בכל יום ===
{daily_insights_text}

=== 📝 מבנה הדוח ===

כתוב דוח של 1200-1500 תווים בפורמט הזה:

🏆 הדגשים של השבוע
━━━━━━━━━━━━━━━━━━
2-3 משפטים על הסיפורים/תכנים הכי בולטים שהצליחו השבוע.

📈 ביצועים שבועיים
━━━━━━━━━━━━━━━━━━

*📺 YouTube:*
- X סרטונים | Y צפיות
- Shorts: X% מהסרטונים → Y% מהצפיות
- מסקנה במשפט

*📘 Facebook:*
- X פוסטים | Y reach
- פורמט מוביל: [Reels/Photos/Videos]
- מסקנה במשפט

*📷 Instagram:*
- X פוסטים | Y views
- פורמט מוביל: [Reels/Photos]
- מסקנה במשפט

🎯 מגמות שבועיות
━━━━━━━━━━━━━━━━━━
בחר 3-4 מגמות שחזרו על עצמן או בלטו השבוע:

- **נושאים:** איזה נושאים הצליחו? (ביטחון, פוליטיקה, מזג אוויר...)
- **פורמטים:** איזה פורמט דומיננטי? (Shorts, Reels, Photos...)
- **מעורבות:** מה קיבל הכי הרבה לייקים/תגובות/שיתופים?
- **פערים בין פלטפורמות:** משהו שעבד טוב במקום אחד ולא באחר?

כל מגמה ב-1-2 משפטים, מבוססת על הנתונים.

💡 המלצות לשבוע הבא
━━━━━━━━━━━━━━━━━━
2-3 המלצות פרקטיות מה כדאי:
- לעשות יותר (מה עבד)
- לנסות (הזדמנויות)
- לבדוק (שאלות שעלו)

=== ✅ כללים ===
- התחל ישר מ-🏆 ללא הקדמה
- השתמש בנתונים הספציפיים (מספרים, אחוזים)
- שלב את התובנות היומיות אם רלוונטי
- מגמות = דברים שחזרו 2-3 פעמים, לא דבר חד-פעמי
- המלצות מבוססות נתונים, לא כלליות
- תמציתי ואסטרטגי
- אל תמציא נתונים
"""

    try:
        contents = [
            types.Content(
                role="user",
                parts=[types.Part.from_text(text=prompt)],
            ),
        ]
        
        # Try models in order
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
        
        return "שגיאה: לא הצלחתי לייצר את הדוח."
        
    except Exception as e:
        return f"שגיאה בניתוח: {e}"


def send_telegram_message(message):
    """שליחת הודעה לטלגרם"""
    token = os.environ.get('TELEGRAM_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    
    if not token or not chat_id:
        print("⚠️ Skipping Telegram - missing credentials.")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    if len(message) > 4000:
        print(f"⚠️ Message too long ({len(message)} chars), truncating...")
        message = message[:3900] + "\n\n... (הדוח קוצר עקב מגבלת אורך)"
    
    payload = {"chat_id": chat_id, "text": message}
    try:
        response = requests.post(url, json=payload)
        print(f"Telegram response: {response.status_code}")
        return response.status_code == 200
    except Exception as e:
        print(f"Telegram Error: {e}")
        return False


def main():
    print(f"\n{'='*60}")
    print(f"📊 Weekly Social Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")
    
    il_tz = pytz.timezone('Asia/Jerusalem')
    today = datetime.now(il_tz)
    week_start = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    week_end = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    
    week_start_display = (today - timedelta(days=7)).strftime('%d/%m')
    week_end_display = (today - timedelta(days=1)).strftime('%d/%m/%Y')
    
    # 1. משיכת נתונים
    print("📺 Fetching YouTube data...")
    yt_df = get_weekly_data('נתוני יוטיוב', 'published_at', days_back=7)
    print(f"   Found {len(yt_df)} videos")
    
    print("📘 Fetching Facebook data...")
    fb_df = get_weekly_data('נתוני פייסבוק', 'date', days_back=7)
    print(f"   Found {len(fb_df)} posts")
    
    print("📷 Fetching Instagram data...")
    ig_df = get_weekly_data('נתוני אינסטגרם', 'date', days_back=7)
    print(f"   Found {len(ig_df)} posts")
    
    # 2. משיכת תובנות יומיות
    print("💡 Fetching daily insights...")
    daily_insights = get_daily_insights(days_back=7)
    print(f"   Found {len(daily_insights)} daily insights")
    
    # 3. חישוב סטטיסטיקות
    print("📊 Calculating stats...")
    stats = calculate_weekly_stats(yt_df, fb_df, ig_df)
    stats_text = format_stats_for_prompt(stats)
    insights_text = format_daily_insights(daily_insights)
    
    # 4. ניתוח עם Gemini
    print("🤖 Analyzing with Gemini...")
    report = analyze_weekly_with_gemini(stats_text, insights_text, week_start_display, week_end_display)
    
    # 5. הוספת כותרת
    header = f"📊 *דוח שבועי - כאן חדשות*\n📅 {week_start_display} - {week_end_display}\n\n"
    full_report = header + report
    
    # 6. שליחה לטלגרם
    print("📨 Sending to Telegram...")
    success = send_telegram_message(full_report)
    
    if success:
        print("✅ Weekly report sent successfully!")
    else:
        print("⚠️ Failed to send report")
        print("\n--- Report Preview ---")
        print(full_report[:1500])


if __name__ == "__main__":
    main()
