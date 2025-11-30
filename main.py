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
import google.generativeai as genai

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

# --- ניתוח AI לטלגרם ---
def analyze_with_gemini(df_recent):
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key: return "⚠️ חסר מפתח ל-Gemini."

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.0-flash')

    # שולחים כותרת, תיאור, תגיות ומספרים
    data_str = df_recent[['title', 'description', 'tags', 'views', 'likes', 'video_type', 'published_at']].to_string(index=False)
    today = datetime.now(pytz.timezone('Asia/Jerusalem')).strftime('%d/%m/%Y')

    prompt = f"""
    אתה העורך הראשי ואנליסט הדיגיטל של "כאן חדשות". התאריך היום: {today}.
    מטרה: דוח בוקר מבצעי לצוות על ביצועי האתמול.

    הנה הנתונים מהימים האחרונים (כולל תיאורים ותגיות):
    {data_str}

    כתוב דוח קצר (עד 180 מילים) הכולל:
    1. 📈 **השורה התחתונה:** סיכום ביצועים ב-24 שעות האחרונות.
    2. 🧩 **הנושאים החמים:** איזה נושא (צבא/כלכלה/פוליטיקה) עניין את הקהל אתמול? זהה מכנה משותף.
    3. 🏆 **המנצח היומי:** הסרטון הכי נצפה ולמה הוא תפס.
    4. 🎙️ **גזרת הכתבים:** חפש בתיאורים שמות כתבים. האם כתב ספציפי הביא מספרים חריגים?
    5. 🔥 **Evergreen:** האם יש סרטון ישן (מלפני 2-3 ימים) שעדיין בראש הטבלה?

    סגנון: מקצועי, חדשותי, עם אימוג'ים, קצר ולעניין.
    """
    
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"שגיאה בניתוח: {e}"

def send_telegram_report(df):
    token = os.environ.get('TELEGRAM_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    
    if not token or not chat_id:
        print("Skipping Telegram.")
        return

    il_tz = pytz.timezone('Asia/Jerusalem')
    # לוקחים הקשר של 3 ימים אחרונים לניתוח
    cutoff = (datetime.now(il_tz) - timedelta(days=3)).strftime('%Y-%m-%d')
    df['date_obj'] = pd.to_datetime(df['published_at'])
    recent_context = df[df['published_at'] >= cutoff].head(25)

    print("Analyzing with Gemini...")
    analysis_text = analyze_with_gemini(recent_context)

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": analysis_text, "parse_mode": "Markdown"}
    try: requests.post(url, json=payload)
    except Exception as e: print(f"Telegram Error: {e}")

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
                'like_rate': round((likes/views*100) if views>0 else 0, 2),
                'comment_rate': round((comments/views*100) if views>0 else 0, 4),
                'video_url': f"https://www.youtube.com/watch?v={item['id']}",
                'last_updated': current_time
            })
            
        if should_stop or 'nextPageToken' not in res: break
        next_page = res['nextPageToken']
            
    return pd.DataFrame(videos)

def update_google_sheet(new_data_df):
    print("Updating Google Sheets...")
    gc = get_sheet_client()
    sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c/edit")
    try: worksheet = sh.worksheet(SHEET_NAME)
    except: worksheet = sh.get_worksheet(0)
    
    existing_df = pd.DataFrame(worksheet.get_all_records())
    
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
    print("Success!")

if __name__ == "__main__":
    new_videos = fetch_videos()
    if not new_videos.empty:
        update_google_sheet(new_videos)
        send_telegram_report(new_videos)
    else:
        print("No videos found.")
