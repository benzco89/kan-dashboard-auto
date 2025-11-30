import os
import json
import pandas as pd
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import isodate
from datetime import datetime, timedelta
import pytz # חשוב לזמן ישראל

# --- הגדרות ---
CHANNEL_ID = 'UC_HwfTAcjBESKZRJq6BTCpg'
SHEET_NAME = 'נתוני יוטיוב' # השם החדש שקבעת
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

def get_youtube_service():
    api_key = os.environ['YOUTUBE_API_KEY']
    return build('youtube', 'v3', developerKey=api_key)

def get_sheet_client():
    creds_json = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    return gspread.authorize(creds)

def fetch_videos():
    youtube = get_youtube_service()
    
    # זמן ישראל לעדכון האחרון
    il_timezone = pytz.timezone('Asia/Jerusalem')
    current_time_il = datetime.now(il_timezone).strftime('%Y-%m-%d %H:%M')
    
    # משיכת 30 יום אחורה
    published_after = (datetime.now() - timedelta(days=30)).isoformat() + "Z"
    
    videos = []
    next_page_token = None
    
    print("Fetching videos from YouTube...")
    while True:
        request = youtube.search().list(
            part="snippet",
            channelId=CHANNEL_ID,
            maxResults=50,
            order="date",
            publishedAfter=published_after,
            type="video",
            pageToken=next_page_token
        )
        response = request.execute()
        
        video_ids = [item['id']['videoId'] for item in response['items']]
        
        if not video_ids:
            break

        stats_request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids)
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

            videos.append({
                'video_id': item['id'],
                'published_at': item['snippet']['publishedAt'][:10],
                'title': item['snippet']['title'],
                'video_type': 'Shorts' if is_short else 'רגיל', # כאן נכנס התיקון לעברית
                'views': int(item['statistics'].get('viewCount', 0)),
                'likes': int(item['statistics'].get('likeCount', 0)),
                'comments': int(item['statistics'].get('commentCount', 0)),
                'duration_seconds': duration_seconds,
                'video_url': video_url,
                'last_updated': current_time_il # זמן ישראל
            })
            
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
            
    return pd.DataFrame(videos)

def update_google_sheet(new_data_df):
    print("Connecting to Google Sheets...")
    gc = get_sheet_client()
    
    # פתיחה לפי ה-URL שלך (הכי בטוח)
    sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c/edit")
    
    # ניסיון לפתוח את הגיליון בשם החדש
    try:
        worksheet = sh.worksheet(SHEET_NAME)
    except:
        print(f"Could not find sheet named '{SHEET_NAME}', trying first sheet.")
        worksheet = sh.get_worksheet(0)
    
    # קריאת נתונים קיימים
    existing_data = worksheet.get_all_records()
    existing_df = pd.DataFrame(existing_data)
    
    print(f"Found {len(existing_df)} existing videos.")
    
    if existing_df.empty:
        final_df = new_data_df
    else:
        # מיזוג חכם: חדש דורס ישן לפי ID
        # ממירים את video_id למחרוזת כדי למנוע בעיות
        new_data_df['video_id'] = new_data_df['video_id'].astype(str)
        existing_df['video_id'] = existing_df['video_id'].astype(str)
        
        # שמים את החדשים למעלה
        combined = pd.concat([new_data_df, existing_df])
        
        # מסירים כפילויות (משאירים את הראשון = החדש ביותר שהגיע מה-API)
        final_df = combined.drop_duplicates(subset=['video_id'], keep='first')
    
    # מיון סופי לפי תאריך
    final_df = final_df.sort_values(by='published_at', ascending=False)
    
    # החלפת ערכי NaN (ריקים) במחרוזת ריקה כדי לא לשבור את ה-JSON
    final_df = final_df.fillna('')

    print(f"Writing {len(final_df)} videos to sheet...")
    
    # ניקוי וכתיבה מחדש
    worksheet.clear()
    worksheet.update([final_df.columns.values.tolist()] + final_df.values.tolist())
    
    print("Success! Data updated.")

if __name__ == "__main__":
    new_videos = fetch_videos()
    if not new_videos.empty:
        update_google_sheet(new_videos)
    else:
        print("No videos found.")
