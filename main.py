import os
import json
import pandas as pd
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import isodate
from datetime import datetime, timedelta
import pytz
import numpy as np
import requests
# --- הספרייה החדשה ---
from google import genai
from google.genai import types

# --- הגדרות ---
CHANNEL_ID = 'UC_HwfTAcjBESKZRJq6BTCpg'
SHEET_NAME = 'נתוני יוטיוב'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

def get_youtube_service():
    api_key = os.environ['YOUTUBE_API_KEY']
    return build('youtube', 'v3', developerKey=api_key)

def get_sheet_client():
    creds_json = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    return gspread.authorize(creds)

def format_duration(seconds):
    if seconds == 0: return "0s"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{int(h)}h {int(m)}m {int(s)}s"
    elif m > 0: return f"{int(m)}m {int(s)}s"
    else: return f"{int(s)}s"

def get_uploads_playlist_id(youtube):
    try:
        request = youtube.channels().list(part="contentDetails", id=CHANNEL_ID)
        response = request.execute()
        return response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    except Exception as e:
        print(f"Error finding uploads ID: {e}")
        return None

def get_existing_data():
    try:
        gc = get_sheet_client()
        sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c/edit")
        try: worksheet = sh.worksheet(SHEET_NAME)
        except: worksheet = sh.get_worksheet(0)
        existing_df = pd.DataFrame(worksheet.get_all_records())
        if not existing_df.empty:
            existing_df['video_id'] = existing_df['video_id'].astype(str)
        return existing_df
    except Exception as e:
        print(f"Error fetching existing data: {e}")
        return pd.DataFrame()

# --- ניתוח AI עם Gemini 3 Pro (הגרסה החדשה) ---
def analyze_with_gemini(df, yesterday_date):
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key: return "⚠️ חסר מפתח ל-Gemini."

    # 1. אתחול הלקוח החדש (הרבה יותר נקי!)
    client = genai.Client(api_key=api_key)

    # --- הכנת הנתונים (לוגיקה זהה, רק הוספת נתונים לפרומפט) ---
    new_yesterday = df[df['published_at'] == yesterday_date].copy()
    new_yesterday_str = ""
    if not new_yesterday.empty:
        new_yesterday = new_yesterday.sort_values('views', ascending=False)
        for _, row in new_yesterday.head(5).iterrows():
            new_yesterday_str += f"• {row['title'][:60]} | {row['video_type']} | {row['views']:,} צפיות\n"
    
    top_delta = ""
    if 'views_delta' in df.columns:
        old_videos = df[df['published_at'] < yesterday_date].copy()
        if not old_videos.empty:
            old_videos['views_delta'] = pd.to_numeric(old_videos['views_delta'], errors='coerce').fillna(0)
            old_videos = old_videos[old_videos['views_delta'] > 0].sort_values('views_delta', ascending=False)
            for _, row in old_videos.head(3).iterrows():
                top_delta += f"• {row['title'][:60]} | מ-{row['published_at']} | +{int(row['views_delta']):,} צפיות\n"
    
    total_new = len(new_yesterday)
    total_views_new = new_yesterday['views'].sum() if not new_yesterday.empty else 0
    
    top5_overall = ""
    for _, row in df.nlargest(5, 'views').iterrows():
        marker = "🆕" if row['published_at'] == yesterday_date else ""
        top5_overall += f"• {marker}{row['title'][:50]} | {row['video_type']} | {row['views']:,}\n"

    today_date = datetime.now(pytz.timezone('Asia/Jerusalem')).strftime('%d/%m/%Y')
    
        prompt = f"""אתה כותב דוח ביצועי יוטיוב יומי לערוץ כאן חדשות. התאריך: {today_date}.

=== נתונים ===

📰 סרטונים חדשים (עלו אתמול {yesterday_date}):
כמות: {total_new}
סה"כ צפיות: {total_views_new:,}
הסרטונים:
{new_yesterday_str if new_yesterday_str else "אין סרטונים חדשים"}

🔥 סרטונים ישנים שצברו צפיות אתמול (דלתא - צפיות חדשות ביממה האחרונה):
{top_delta if top_delta else "אין מידע על דלתא"}

📊 טופ 5 כללי בערוץ (לפי סה״כ צפיות מצטבר):
{top5_overall}

=== מבנה הדוח ===

כתוב סיכום של 180-220 מילים. התחל ישר מהחלק הקבוע, בלי הקדמה או פסקת פתיחה.

**חלק קבוע (חובה):**

📊 **המספרים:** משפט אחד - כמה סרטונים עלו אתמול וכמה צפיות צברו.

🏆 **טופ 3 מאתמול:**
1. [שם מקוצר] | [Shorts/רגיל] | [צפיות]
2. [שם מקוצר] | [Shorts/רגיל] | [צפיות]
3. [שם מקוצר] | [Shorts/רגיל] | [צפיות]

🔥 **ממשיך להדהד:** הסרטון הישן (לא מאתמול) שצבר הכי הרבה צפיות אתמול.
פורמט: [שם] | פורסם ב-[תאריך] | +[דלתא] צפיות אתמול
אם הדלתא מעל 5,000 - הוסף משפט קצר למה זה כנראה עדיין רלוונטי.
אם אין מידע על דלתא או שהיא נמוכה מ-500 - כתוב "אין סרטון ישן בולט היום".

**חלק חופשי (חובה לבחור 2 בנושאים שונים):**

תסתכל על הנתונים ותבחר 2 תובנות מעניינות. חשוב: התובנות חייבות להיות על נושאים שונים לגמרי.

אפשרויות:
- 📈 מגמה או נושא חם
- ⚡ הפתעה - סרטון שהצליח/נכשל מעבר לצפוי
- 🎬 תצפית על Shorts vs רגיל
- 👤 קרדיט ליוצר/כתב שמוזכר ב-description
- 🔄 השוואה לימים קודמים

כתוב כל תובנה במשפט-שניים.

=== סגנון ===
- התחל ישר מ-📊
- קצר ועובדתי
- ציין תמיד אם סרטון הוא Shorts
- אל תמציא ואל תנחש
"""


    try:
        # 2. הקריאה החדשה והקצרה
        response = client.models.generate_content(
            model="gemini-3-pro-preview", # המודל החדש שביקשת
            contents=prompt,
            config=types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(
                    include_thoughts=True # הפעלת יכולת החשיבה
                ),
                tools=[types.Tool(google_search=types.GoogleSearch())], # הוספת חיפוש אם צריך השלמת מידע
                temperature=0.7
            )
        )
        return response.text
    except Exception as e:
        return f"שגיאה בניתוח AI: {e}"

def send_telegram_report(df):
    token = os.environ.get('TELEGRAM_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    
    if not token or not chat_id:
        print("Skipping Telegram - missing credentials.")
        return

    il_tz = pytz.timezone('Asia/Jerusalem')
    yesterday = (datetime.now(il_tz) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"Generating AI report with Gemini 3 Pro for {yesterday}...")
    analysis_text = analyze_with_gemini(df, yesterday)

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": analysis_text, "parse_mode": "Markdown"}
    try: 
        response = requests.post(url, json=payload)
        print(f"Telegram response: {response.status_code}")
    except Exception as e: 
        print(f"Telegram Error: {e}")

# --- הפונקציה הראשית ---
def fetch_videos():
    youtube = get_youtube_service()
    uploads_id = get_uploads_playlist_id(youtube)
    if not uploads_id: return pd.DataFrame()

    il_tz = pytz.timezone('Asia/Jerusalem')
    current_time = datetime.now(il_tz).strftime('%Y-%m-%d %H:%M')
    cutoff_date = datetime.now(pytz.utc) - timedelta(days=30)
    
    videos = []
    next_page = None
    should_stop = False
    
    print("Fetching videos...")
    while True:
        req = youtube.playlistItems().list(part="snippet,contentDetails", playlistId=uploads_id, maxResults=50, pageToken=next_page)
        res = req.execute()
        
        ids_to_fetch = []
        for item in res['items']:
            pub = datetime.strptime(item['contentDetails']['videoPublishedAt'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
            if pub < cutoff_date: 
                should_stop = True
                break
            ids_to_fetch.append(item['contentDetails']['videoId'])
        
        if not ids_to_fetch: break

        stats_res = youtube.videos().list(part="snippet,contentDetails,statistics,topicDetails", id=','.join(ids_to_fetch)).execute()
        
        for item in stats_res['items']:
            dur = item['contentDetails']['duration']
            try: sec = isodate.parse_duration(dur).total_seconds()
            except: sec = 0
            
            is_short = sec <= 60 and sec > 0
            views = int(item['statistics'].get('viewCount', 0))
            likes = int(item['statistics'].get('likeCount', 0))
            comments = int(item['statistics'].get('commentCount', 0))
            
            thumb = item['snippet']['thumbnails']
            thumb_url = thumb.get('maxres', thumb.get('high', thumb.get('medium')))['url']

            videos.append({
                'video_id': item['id'],
                'published_at': item['snippet']['publishedAt'][:10],
                'published_time': item['snippet']['publishedAt'][11:16],
                'title': item['snippet']['title'],
                'description': item['snippet']['description'],
                'thumbnail_url': thumb_url,
                'tags': ",".join(item['snippet'].get('tags', [])),
                'video_type': 'Shorts' if is_short else 'רגיל',
                'views': views,
                'likes': likes,
                'comments': comments,
                'duration_seconds': sec,
                'duration_formatted': format_duration(sec),
                'like_rate': round((likes/views*100) if views > 0 else 0, 2),
                'comment_rate': round((comments/views*100) if views > 0 else 0, 4),
                'video_url': f"https://www.youtube.com/watch?v={item['id']}",
                'last_updated': current_time
            })
            
        if should_stop or 'nextPageToken' not in res: break
        next_page = res['nextPageToken']
            
    print(f"Fetched {len(videos)} videos.")
    return pd.DataFrame(videos)

def update_google_sheet(new_data_df):
    print("Updating Google Sheets...")
    existing_df = get_existing_data()
    
    if not existing_df.empty and 'views' in existing_df.columns:
        existing_df['views'] = pd.to_numeric(existing_df['views'], errors='coerce').fillna(0)
        existing_views = existing_df.set_index('video_id')['views'].to_dict()
        new_data_df['views_delta'] = new_data_df.apply(
            lambda row: row['views'] - existing_views.get(row['video_id'], row['views']), axis=1
        )
    else:
        new_data_df['views_delta'] = 0
    
    gc = get_sheet_client()
    sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c/edit")
    try: worksheet = sh.worksheet(SHEET_NAME)
    except: worksheet = sh.get_worksheet(0)
    
    if existing_df.empty: final_df = new_data_df
    else:
        new_data_df['video_id'] = new_data_df['video_id'].astype(str)
        existing_df['video_id'] = existing_df['video_id'].astype(str)
        for col in new_data_df.columns:
            if col not in existing_df.columns: existing_df[col] = ""
        combined = pd.concat([new_data_df, existing_df])
        final_df = combined.drop_duplicates(subset=['video_id'], keep='first')
    
    final_df = final_df.sort_values(by='published_at', ascending=False)
    final_df = final_df.fillna(0).replace([np.inf, -np.inf], 0)
    
    for col in ['description', 'tags', 'thumbnail_url', 'published_time', 'duration_formatted']:
        if col in final_df.columns: final_df[col] = final_df[col].replace(0, "")

    worksheet.clear()
    worksheet.update([final_df.columns.values.tolist()] + final_df.values.tolist(), value_input_option='RAW')
    print("Sheet updated successfully!")
    return final_df

if __name__ == "__main__":
    new_videos = fetch_videos()
    if not new_videos.empty:
        updated_df = update_google_sheet(new_videos)
        send_telegram_report(updated_df)
    else:
        print("No videos found.")
