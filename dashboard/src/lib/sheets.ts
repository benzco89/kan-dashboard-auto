import { google } from 'googleapis';
import * as path from 'path';

const SPREADSHEET_ID = '1WB0cFc2RgR1Z-crjhtkSqLKp1mMdFoby8NwV7h3UN6c';

// Sheet names (Hebrew)
export const SHEETS = {
  YOUTUBE: 'נתוני יוטיוב',
  FACEBOOK: 'נתוני פייסבוק',
  INSTAGRAM: 'נתוני אינסטגרם',
  FOLLOWERS: 'מעקב עוקבים',
  INSIGHTS: 'תובנות יומיות',
} as const;

// Type definitions based on the data structure
export interface YouTubeVideo {
  video_id: string;
  published_at: string;
  title: string;
  views: number;
  views_delta: number;
  likes: number;
  comments: number;
  like_rate: number;
  video_type: 'Shorts' | 'Regular';
}

export interface FacebookPost {
  post_id: string;
  date: string;
  title: string;
  type: 'Reels' | 'Images' | 'Videos' | 'Links';
  views: number;
  reach: number;
  likes: number;
  shares: number;
  engagement_rate: number;
}

export interface InstagramPost {
  media_id: string;
  date: string;
  caption: string;
  type: 'Reel' | 'Photo' | 'Carousel';
  views: number;
  reach: number;
  saved: number;
  shares: number;
  engagement_rate: number;
}

export interface FollowersData {
  date: string;
  yt_subscribers: number;
  yt_subscribers_change: number;
  fb_followers: number;
  fb_followers_change: number;
  ig_followers: number;
  ig_followers_change: number;
}

export interface DailyInsight {
  date: string;
  insights: string;
  timestamp: string;
}

// Initialize Google Sheets API
async function getGoogleSheetsClient() {
  const serviceAccountPath = path.join(process.cwd(), '..', 'service-account.json');

  const auth = new google.auth.GoogleAuth({
    keyFile: serviceAccountPath,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  return sheets;
}

// Generic function to fetch sheet data
async function fetchSheetData(sheetName: string): Promise<string[][]> {
  const sheets = await getGoogleSheetsClient();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: sheetName,
  });

  return response.data.values || [];
}

// Helper to find column index (case-insensitive)
function findColumnIndex(headers: string[], ...possibleNames: string[]): number {
  const lowerHeaders = headers.map(h => h?.toLowerCase()?.trim() || '');
  for (const name of possibleNames) {
    const idx = lowerHeaders.indexOf(name.toLowerCase());
    if (idx !== -1) return idx;
  }
  return -1;
}

// Parse YouTube data
function parseYouTubeData(rows: string[][]): YouTubeVideo[] {
  if (rows.length < 2) return [];

  const headers = rows[0];
  const colIdx = {
    video_id: findColumnIndex(headers, 'video_id'),
    published_at: findColumnIndex(headers, 'published_at'),
    title: findColumnIndex(headers, 'title'),
    views: findColumnIndex(headers, 'views'),
    views_delta: findColumnIndex(headers, 'views_delta'),
    likes: findColumnIndex(headers, 'likes'),
    comments: findColumnIndex(headers, 'comments'),
    like_rate: findColumnIndex(headers, 'like_rate'),
    video_type: findColumnIndex(headers, 'video_type', 'type'),
  };

  return rows.slice(1).map(row => {
    const rawType = colIdx.video_type >= 0 ? row[colIdx.video_type]?.trim() : '';
    // Determine video type - check for Shorts keywords
    let videoType: 'Shorts' | 'Regular' = 'Regular';
    if (rawType) {
      const lowerType = rawType.toLowerCase();
      if (lowerType === 'shorts' || lowerType.includes('short')) {
        videoType = 'Shorts';
      } else if (lowerType === 'regular' || lowerType.includes('regular')) {
        videoType = 'Regular';
      }
    }

    return {
      video_id: colIdx.video_id >= 0 ? row[colIdx.video_id] || '' : '',
      published_at: colIdx.published_at >= 0 ? row[colIdx.published_at] || '' : '',
      title: colIdx.title >= 0 ? row[colIdx.title] || '' : '',
      views: colIdx.views >= 0 ? parseInt(row[colIdx.views] || '0', 10) : 0,
      views_delta: colIdx.views_delta >= 0 ? parseInt(row[colIdx.views_delta] || '0', 10) : 0,
      likes: colIdx.likes >= 0 ? parseInt(row[colIdx.likes] || '0', 10) : 0,
      comments: colIdx.comments >= 0 ? parseInt(row[colIdx.comments] || '0', 10) : 0,
      like_rate: colIdx.like_rate >= 0 ? parseFloat(row[colIdx.like_rate] || '0') : 0,
      video_type: videoType,
    };
  });
}

// Parse Facebook data
function parseFacebookData(rows: string[][]): FacebookPost[] {
  if (rows.length < 2) return [];

  const headers = rows[0];
  const colIdx = {
    post_id: findColumnIndex(headers, 'post_id'),
    date: findColumnIndex(headers, 'date'),
    title: findColumnIndex(headers, 'title'),
    type: findColumnIndex(headers, 'type'),
    views: findColumnIndex(headers, 'views'),
    reach: findColumnIndex(headers, 'reach'),
    likes: findColumnIndex(headers, 'likes'),
    shares: findColumnIndex(headers, 'shares'),
    engagement_rate: findColumnIndex(headers, 'engagement_rate'),
  };

  return rows.slice(1).map(row => {
    const rawType = colIdx.type >= 0 ? row[colIdx.type]?.trim() : '';
    // Determine post type
    let postType: 'Reels' | 'Images' | 'Videos' | 'Links' = 'Videos';
    if (rawType) {
      const lowerType = rawType.toLowerCase();
      if (lowerType === 'reels' || lowerType === 'reel') {
        postType = 'Reels';
      } else if (lowerType === 'images' || lowerType === 'image' || lowerType === 'photo') {
        postType = 'Images';
      } else if (lowerType === 'videos' || lowerType === 'video') {
        postType = 'Videos';
      } else if (lowerType === 'links' || lowerType === 'link') {
        postType = 'Links';
      }
    }

    return {
      post_id: colIdx.post_id >= 0 ? row[colIdx.post_id] || '' : '',
      date: colIdx.date >= 0 ? row[colIdx.date] || '' : '',
      title: colIdx.title >= 0 ? row[colIdx.title] || '' : '',
      type: postType,
      views: colIdx.views >= 0 ? parseInt(row[colIdx.views] || '0', 10) : 0,
      reach: colIdx.reach >= 0 ? parseInt(row[colIdx.reach] || '0', 10) : 0,
      likes: colIdx.likes >= 0 ? parseInt(row[colIdx.likes] || '0', 10) : 0,
      shares: colIdx.shares >= 0 ? parseInt(row[colIdx.shares] || '0', 10) : 0,
      engagement_rate: colIdx.engagement_rate >= 0 ? parseFloat(row[colIdx.engagement_rate] || '0') : 0,
    };
  });
}

// Parse Instagram data
function parseInstagramData(rows: string[][]): InstagramPost[] {
  if (rows.length < 2) return [];

  const headers = rows[0];
  return rows.slice(1).map(row => ({
    media_id: row[headers.indexOf('media_id')] || '',
    date: row[headers.indexOf('date')] || '',
    caption: row[headers.indexOf('caption')] || '',
    type: (row[headers.indexOf('type')] as 'Reel' | 'Photo' | 'Carousel') || 'Reel',
    views: parseInt(row[headers.indexOf('views')] || '0', 10),
    reach: parseInt(row[headers.indexOf('reach')] || '0', 10),
    saved: parseInt(row[headers.indexOf('saved')] || '0', 10),
    shares: parseInt(row[headers.indexOf('shares')] || '0', 10),
    engagement_rate: parseFloat(row[headers.indexOf('engagement_rate')] || '0'),
  }));
}

// Parse Followers data
function parseFollowersData(rows: string[][]): FollowersData[] {
  if (rows.length < 2) return [];

  const headers = rows[0];
  return rows.slice(1).map(row => ({
    date: row[headers.indexOf('date')] || '',
    yt_subscribers: parseInt(row[headers.indexOf('yt_subscribers')] || '0', 10),
    yt_subscribers_change: parseInt(row[headers.indexOf('yt_subscribers_change')] || '0', 10),
    fb_followers: parseInt(row[headers.indexOf('fb_followers')] || '0', 10),
    fb_followers_change: parseInt(row[headers.indexOf('fb_followers_change')] || '0', 10),
    ig_followers: parseInt(row[headers.indexOf('ig_followers')] || '0', 10),
    ig_followers_change: parseInt(row[headers.indexOf('ig_followers_change')] || '0', 10),
  }));
}

// Parse Daily Insights data
function parseInsightsData(rows: string[][]): DailyInsight[] {
  if (rows.length < 2) return [];

  const headers = rows[0];
  return rows.slice(1).map(row => ({
    date: row[headers.indexOf('date')] || '',
    insights: row[headers.indexOf('insights')] || '',
    timestamp: row[headers.indexOf('timestamp')] || '',
  })).filter(item => item.insights); // Filter out empty insights
}

// Public API functions
export async function getYouTubeData(): Promise<YouTubeVideo[]> {
  const rows = await fetchSheetData(SHEETS.YOUTUBE);
  return parseYouTubeData(rows);
}

export async function getFacebookData(): Promise<FacebookPost[]> {
  const rows = await fetchSheetData(SHEETS.FACEBOOK);
  return parseFacebookData(rows);
}

export async function getInstagramData(): Promise<InstagramPost[]> {
  const rows = await fetchSheetData(SHEETS.INSTAGRAM);
  return parseInstagramData(rows);
}

export async function getFollowersData(): Promise<FollowersData[]> {
  const rows = await fetchSheetData(SHEETS.FOLLOWERS);
  return parseFollowersData(rows);
}

export async function getInsightsData(): Promise<DailyInsight[]> {
  const rows = await fetchSheetData(SHEETS.INSIGHTS);
  return parseInsightsData(rows);
}

// Get all data at once
export async function getAllDashboardData() {
  const [youtube, facebook, instagram, followers, insights] = await Promise.all([
    getYouTubeData(),
    getFacebookData(),
    getInstagramData(),
    getFollowersData(),
    getInsightsData(),
  ]);

  return {
    youtube,
    facebook,
    instagram,
    followers,
    insights,
  };
}

// Helper to filter data by date range
// Note: Data is pulled at 08:30 daily, so today's data doesn't exist yet
// We shift by 1 day: "7 days" means from (today-8) to (today-1), not (today-7) to today
export function filterByDateRange<T extends { date?: string; published_at?: string }>(
  data: T[],
  days: number
): T[] {
  const endDate = new Date();
  endDate.setDate(endDate.getDate() - 1); // Yesterday is the latest data
  endDate.setHours(23, 59, 59, 999);

  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days); // days ago from today (which means days-1 from yesterday)
  startDate.setHours(0, 0, 0, 0);

  return data.filter(item => {
    const dateStr = item.date || item.published_at;
    if (!dateStr) return false;
    const itemDate = new Date(dateStr);
    return itemDate >= startDate && itemDate <= endDate;
  });
}

// Helper to get the actual date range being displayed
export function getDateRangeDisplay(days: number): { start: Date; end: Date } {
  const end = new Date();
  end.setDate(end.getDate() - 1); // Yesterday

  const start = new Date();
  start.setDate(start.getDate() - days);

  return { start, end };
}
