"""
YouTube Collector - איסוף נתוני סרטונים מיוטיוב
שומר לגוגל שיטס בלבד, בלי ניתוח AI או שליחה לטלגרם
"""

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

# Load .env file if exists (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, using environment variables directly

# --- הגדרות ---
CHANNEL_ID = 'UC_HwfTAcjBESKZRJq6BTCpg'
SHEET_NAME = 'נתוני יוטיוב'
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c/edit"
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
    """שואב את הנתונים הקיימים מה-Sheet כדי לחשב דלתא"""
    try:
        gc = get_sheet_client()
        sh = gc.open_by_url(SPREADSHEET_URL)
        try:
            worksheet = sh.worksheet(SHEET_NAME)
        except:
            worksheet = sh.get_worksheet(0)
        
        existing_df = pd.DataFrame(worksheet.get_all_records())
        if not existing_df.empty:
            existing_df['video_id'] = existing_df['video_id'].astype(str)
        return existing_df
    except Exception as e:
        print(f"Error fetching existing data: {e}")
        return pd.DataFrame()


def fetch_videos():
    """שאיבת סרטונים מיוטיוב"""
    youtube = get_youtube_service()
    uploads_id = get_uploads_playlist_id(youtube)
    if not uploads_id: 
        return pd.DataFrame()

    il_tz = pytz.timezone('Asia/Jerusalem')
    current_time = datetime.now(il_tz).strftime('%Y-%m-%d %H:%M')
    cutoff_date = datetime.now(pytz.utc) - timedelta(days=30)
    
    videos = []
    next_page = None
    should_stop = False
    
    print("Fetching videos from YouTube API...")
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
        
        if not ids_to_fetch: 
            break

        stats_res = youtube.videos().list(part="snippet,contentDetails,statistics,topicDetails", id=','.join(ids_to_fetch)).execute()
        
        for item in stats_res['items']:
            dur = item['contentDetails']['duration']
            try: 
                sec = isodate.parse_duration(dur).total_seconds()
            except: 
                sec = 0
            
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
            
        if should_stop or 'nextPageToken' not in res: 
            break
        next_page = res['nextPageToken']
    
    print(f"Fetched {len(videos)} videos.")
    return pd.DataFrame(videos)


def update_google_sheet(new_data_df):
    """עדכון הגיליון בגוגל שיטס"""
    print("Updating Google Sheets...")
    
    # שליפת הנתונים הקיימים
    existing_df = get_existing_data()
    
    # חישוב דלתא - כמה צפיות נוספו מאז ההרצה הקודמת
    if not existing_df.empty and 'views' in existing_df.columns:
        existing_df['views'] = pd.to_numeric(existing_df['views'], errors='coerce').fillna(0)
        existing_views = existing_df.set_index('video_id')['views'].to_dict()
        new_data_df['views_delta'] = new_data_df.apply(
            lambda row: row['views'] - existing_views.get(row['video_id'], row['views']), 
            axis=1
        )
    else:
        new_data_df['views_delta'] = 0
    
    # מיזוג הנתונים
    gc = get_sheet_client()
    sh = gc.open_by_url(SPREADSHEET_URL)
    try: 
        worksheet = sh.worksheet(SHEET_NAME)
    except: 
        worksheet = sh.get_worksheet(0)
    
    if existing_df.empty: 
        final_df = new_data_df
    else:
        new_data_df['video_id'] = new_data_df['video_id'].astype(str)
        existing_df['video_id'] = existing_df['video_id'].astype(str)
        
        # וידוא שכל העמודות קיימות
        for col in new_data_df.columns:
            if col not in existing_df.columns: 
                existing_df[col] = ""
        
        combined = pd.concat([new_data_df, existing_df])
        final_df = combined.drop_duplicates(subset=['video_id'], keep='first')
    
    final_df = final_df.sort_values(by='published_at', ascending=False)
    final_df = final_df.fillna(0).replace([np.inf, -np.inf], 0)
    
    # ניקוי עמודות טקסט
    for col in ['description', 'tags', 'thumbnail_url', 'published_time', 'duration_formatted']:
        if col in final_df.columns: 
            final_df[col] = final_df[col].replace(0, "")

    worksheet.clear()
    worksheet.update([final_df.columns.values.tolist()] + final_df.values.tolist(), value_input_option='RAW')
    print("Sheet updated successfully!")
    
    return final_df


if __name__ == "__main__":
    new_videos = fetch_videos()
    if not new_videos.empty:
        updated_df = update_google_sheet(new_videos)
        print(f"✅ YouTube collection complete! {len(new_videos)} videos processed.")
    else:
        print("No videos found.")
