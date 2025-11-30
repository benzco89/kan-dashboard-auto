import os
import json
import pandas as pd
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import isodate
from datetime import datetime, timedelta

# --- הגדרות ---
CHANNEL_ID = 'UC_HwfTAcjBESKZRJq6BTCpg' # כאן חדשות
SHEET_NAME = 'נתוני יוטיוב' # ודא שזה השם של הטאב למטה בשיטס!
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

def get_youtube_service():
    api_key = os.environ['YOUTUBE_API_KEY']
    return build('youtube', 'v3', developerKey=api_key)

def get_sheet_client():
    # טעינת המפתח מהסודות
    creds_json = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, SCOPES)
    return gspread.authorize(creds)

def fetch_videos():
    youtube = get_youtube_service()
    
    # משיכת סרטונים מהחודש האחרון (כדי לעדכן גם ישנים)
    published_after = (datetime.now() - timedelta(days=30)).isoformat() + "Z"
    
    videos = []
    next_page_token = None
    
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
        
        # משיכת סטטיסטיקות מלאות ל-IDs שמצאנו
        stats_request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids)
        )
        stats_response = stats_request.execute()
        
        for item in stats_response['items']:
            # חישוב שניות
            duration_iso = item['contentDetails']['duration']
            try:
                duration_seconds = isodate.parse_duration(duration_iso).total_seconds()
            except:
                duration_seconds = 0
            
            # זיהוי שורטס
            is_short = False
            if duration_seconds <= 60 and duration_seconds > 0:
                is_short = True
            
            # יצירת הלינק
            video_url = f"https://www.youtube.com/watch?v={item['id']}"

            videos.append({
                'video_id': item['id'],
                'published_at': item['snippet']['publishedAt'][:10], # רק תאריך
                'title': item['snippet']['title'],
                'video_type': 'Shorts' if is_short else 'רגיל', # זיהוי בעברית לדשבורד
                'views': int(item['statistics'].get('viewCount', 0)),
                'likes': int(item['statistics'].get('likeCount', 0)),
                'comments': int(item['statistics'].get('commentCount', 0)),
                'duration_seconds': duration_seconds,
                'video_url': video_url,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M')
            })
            
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
            
    return pd.DataFrame(videos)

def update_google_sheet(new_data_df):
    gc = get_sheet_client()
    # פתיחת הגיליון הראשון בקובץ
    # שים לב: כאן צריך לשים את ה-ID של השיטס שלך מה-URL
    # או להשתמש ב-open_by_url
    # הדרך הכי בטוחה: פתח לפי שם הקובץ או URL
    sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c/edit") 
    worksheet = sh.worksheet(SHEET_NAME)
    
    # קריאת הנתונים הקיימים
    existing_data = worksheet.get_all_records()
    existing_df = pd.DataFrame(existing_data)
    
    if existing_df.empty:
        final_df = new_data_df
    else:
        # --- הלוגיקה החכמה (Upsert) ---
        # 1. שמים את המידע החדש למעלה
        combined_df = pd.concat([new_data_df, existing_df])
        
        # 2. מסירים כפילויות לפי video_id, משאירים את העליון (החדש ביותר)
        # זה מעדכן את הצפיות לסרטונים קיימים ומוסיף חדשים
        final_df = combined_df.drop_duplicates(subset=['video_id'], keep='first')
    
    # מיון לפי תאריך (מהחדש לישן)
    final_df = final_df.sort_values(by='published_at', ascending=False)
    
    # כתיבה חזרה לשיטס (מוחקים הכל וכותבים מחדש את המעודכן)
    worksheet.clear()
    # הוספת כותרות
    worksheet.append_row(final_df.columns.tolist())
    # הוספת המידע
    worksheet.append_rows(final_df.values.tolist())
    
    print("Update Complete!")

if __name__ == "__main__":
    print("Starting job...")
    new_videos = fetch_videos()
    print(f"Fetched {len(new_videos)} videos from API.")
    update_google_sheet(new_videos)
