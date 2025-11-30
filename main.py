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
    if h > 0:
        return f"{int(h)}h {int(m)}m {int(s)}s"
    elif m > 0:
        return f"{int(m)}m {int(s)}s"
    else:
        return f"{int(s)}s"

# --- פונקציה חדשה: מציאת ה-ID האמיתי של ההעלאות ---
def get_uploads_playlist_id(youtube):
    print(f"Getting uploads ID for channel: {CHANNEL_ID}")
    request = youtube.channels().list(
        part="contentDetails",
        id=CHANNEL_ID
    )
    response = request.execute()
    try:
        uploads_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        print(f"Found Uploads Playlist ID: {uploads_id}")
        return uploads_id
    except Exception as e:
        print(f"Error finding uploads ID: {e}")
        return None

def fetch_videos():
    youtube = get_youtube_service()
    
    # שלב 1: משיכת ה-ID הנכון
    uploads_playlist_id = get_uploads_playlist_id(youtube)
    if not uploads_playlist_id:
        print("CRITICAL ERROR: Could not find playlist ID. Exiting.")
        return pd.DataFrame()

    il_timezone = pytz.timezone('Asia/Jerusalem')
    current_time_il = datetime.now(il_timezone).strftime('%Y-%m-%d %H:%M')
    
    # הגדרת זמן חיתוך (30 יום אחורה)
    cutoff_date = datetime.now(pytz.utc) - timedelta(days=30)
    
    videos = []
    next_page_token = None
    video_count = 0
    
    print("Starting to fetch videos...")
    
    while True:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        
        video_ids_to_fetch = []
        
        # סינון ראשוני לפי תאריך
        for item in response['items']:
            pub_date_str = item['contentDetails']['videoPublishedAt']
            pub_date = datetime.strptime(pub_date_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
            
            if pub_date < cutoff_date:
                print(f"Reached old videos ({pub_date_str}). Stopping fetch.")
                next_page_token = None 
                break
            
            video_ids_to_fetch.append(item['contentDetails']['videoId'])
        
        if not video_ids_to_fetch:
            break
            
        print(f"Fetching stats for batch of {len(video_ids_to_fetch)} videos...")

        stats_request = youtube.videos().list(
            part="snippet,contentDetails,statistics,topicDetails",
            id=','.join(video_ids_to_fetch)
        )
        stats_response = stats_request.execute()
        
        for item in stats_response['items']:
            duration_iso = item['contentDetails']['duration']
            try:
                duration_seconds = isodate.parse_duration(duration_iso).total_seconds()
            except:
                duration_seconds = 0
            
            is_short = False
            if duration_seconds <= 60 and duration_seconds > 0:
                is_short = True
            
            video_url = f"https://www.youtube.com/watch?v={item['id']}"
            
            thumbnails = item['snippet']['thumbnails']
            thumbnail_url = thumbnails.get('maxres', thumbnails.get('high', thumbnails.get('medium')))['url']
            tags = ",".join(item['snippet'].get('tags', []))

            views = int(item['statistics'].get('viewCount', 0))
            likes = int(item['statistics'].get('likeCount', 0))
            comments = int(item['statistics'].get('commentCount', 0))

            like_rate = (likes / views * 100) if views > 0 else 0
            comment_rate = (comments / views * 100) if views > 0 else 0
            
            published_full = item['snippet']['publishedAt']
            published_date = published_full[:10]
            published_time = published_full[11:16]

            videos.append({
                'video_id': item['id'],
                'published_at': published_date,
                'published_time': published_time,
                'title': item['snippet']['title'],
                'description': item['snippet']['description'],
                'thumbnail_url': thumbnail_url,
                'tags': tags,
                'video_type': 'Shorts' if is_short else 'רגיל',
                'views': views,
                'likes': likes,
                'comments': comments,
                'duration_seconds': duration_seconds,
                'duration_formatted': format_duration(duration_seconds),
                'like_rate': round(like_rate, 2),
                'comment_rate': round(comment_rate, 4),
                'video_url': video_url,
                'last_updated': current_time_il
            })
            video_count += 1
            
        if not next_page_token and 'nextPageToken' in response:
             next_page_token = response['nextPageToken']
        elif not next_page_token:
             break
    
    print(f"Total videos fetched: {len(videos)}")
    return pd.DataFrame(videos)

def update_google_sheet(new_data_df):
    print("Connecting to Google Sheets...")
    gc = get_sheet_client()
    sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c/edit")
    
    try:
        worksheet = sh.worksheet(SHEET_NAME)
    except:
        print(f"Could not find sheet named '{SHEET_NAME}', trying first sheet.")
        worksheet = sh.get_worksheet(0)
    
    existing_data = worksheet.get_all_records()
    existing_df = pd.DataFrame(existing_data)
    
    # אם הטבלה ריקה (כמו המצב שלך עכשיו)
    if existing_df.empty:
        print("Sheet is empty. Writing fresh data...")
        final_df = new_data_df
    else:
        print(f"Found {len(existing_df)} existing videos. Merging...")
        new_data_df['video_id'] = new_data_df['video_id'].astype(str)
        existing_df['video_id'] = existing_df['video_id'].astype(str)
        
        for col in new_data_df.columns:
            if col not in existing_df.columns:
                existing_df[col] = ""

        combined = pd.concat([new_data_df, existing_df])
        final_df = combined.drop_duplicates(subset=['video_id'], keep='first')
    
    final_df = final_df.sort_values(by='published_at', ascending=False)
    final_df = final_df.fillna(0)
    final_df = final_df.replace([np.inf, -np.inf], 0)
    
    text_cols = ['description', 'tags', 'thumbnail_url', 'published_time', 'duration_formatted']
    for col in text_cols:
        if col in final_df.columns:
            final_df[col] = final_df[col].replace(0, "")

    print(f"Writing {len(final_df)} rows to sheet...")
    
    worksheet.clear()
    data_to_write = [final_df.columns.values.tolist()] + final_df.values.tolist()
    worksheet.update(data_to_write, value_input_option='RAW')
    
    print("Success! Data updated.")

if __name__ == "__main__":
    new_videos = fetch_videos()
    if not new_videos.empty:
        update_google_sheet(new_videos)
    else:
        print("No videos found in the last 30 days.")
