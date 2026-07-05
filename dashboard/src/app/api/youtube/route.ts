import { NextResponse } from 'next/server';
import {
  getAllDashboardData,
  filterByDateRange,
  type YouTubeVideo,
  type FollowersData
} from '@/lib/sheets';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const days = parseInt(searchParams.get('days') || '30', 10);

    const data = await getAllDashboardData();

    // Filter by date range - current period
    const filteredYouTube = filterByDateRange(data.youtube, days);

    // Filter by date range - previous period (for comparison)
    const prevYouTube = filterByDateRange(data.youtube, days * 2).filter(v => {
      const date = new Date(v.published_at);
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - days);
      return date < cutoff;
    });

    // Get latest followers data
    const latestFollowers = data.followers.length > 0
      ? data.followers[data.followers.length - 1]
      : null;

    // Calculate YouTube-specific stats for current and previous periods
    const stats = calculateYouTubeStats(filteredYouTube, latestFollowers);
    const prevStats = calculateYouTubeStats(prevYouTube, null);

    // Get performance over time
    const performanceData = getYouTubePerformanceData(filteredYouTube);
    const prevPerformanceData = getYouTubePerformanceData(prevYouTube);

    // Get video type breakdown (Shorts vs Regular)
    const videoTypeBreakdown = getVideoTypeBreakdown(filteredYouTube);

    // Get like rate over time
    const likeRateData = getLikeRateData(filteredYouTube);

    // Get all videos sorted by views
    const videos = filteredYouTube
      .map(v => ({
        id: v.video_id,
        title: v.title,
        publishedAt: v.published_at,
        views: v.views,
        viewsDelta: v.views_delta,
        likes: v.likes,
        comments: v.comments,
        likeRate: v.like_rate,
        type: v.video_type,
        url: v.video_type === 'Shorts'
          ? `https://www.youtube.com/shorts/${v.video_id}`
          : `https://www.youtube.com/watch?v=${v.video_id}`,
      }))
      .sort((a, b) => b.views - a.views);

    return NextResponse.json({
      success: true,
      data: {
        stats,
        prevStats,
        performanceData,
        prevPerformanceData,
        videoTypeBreakdown,
        likeRateData,
        videos,
        lastUpdated: new Date().toISOString(),
      },
    });
  } catch (error) {
    console.error('Error fetching YouTube data:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to fetch YouTube data' },
      { status: 500 }
    );
  }
}

function calculateYouTubeStats(youtube: YouTubeVideo[], followers: FollowersData | null) {
  const totalViews = youtube.reduce((sum, v) => sum + v.views, 0);
  const totalLikes = youtube.reduce((sum, v) => sum + v.likes, 0);
  const totalComments = youtube.reduce((sum, v) => sum + v.comments, 0);
  const totalViewsDelta = youtube.reduce((sum, v) => sum + v.views_delta, 0);

  // Average like rate
  const avgLikeRate = youtube.length > 0
    ? youtube.reduce((sum, v) => sum + v.like_rate, 0) / youtube.length
    : 0;

  // Count by type
  const shortsCount = youtube.filter(v => v.video_type === 'Shorts').length;
  const regularCount = youtube.filter(v => v.video_type === 'Regular').length;

  return {
    subscribers: followers?.yt_subscribers || 0,
    subscribersChange: followers?.yt_subscribers_change || 0,
    totalViews,
    totalViewsDelta,
    totalLikes,
    totalComments,
    avgLikeRate,
    videoCount: youtube.length,
    shortsCount,
    regularCount,
  };
}

function getYouTubePerformanceData(youtube: YouTubeVideo[]) {
  // Group by date
  const dateMap = new Map<string, { views: number; likes: number; comments: number }>();

  youtube.forEach(v => {
    const date = v.published_at.split('T')[0];
    const existing = dateMap.get(date) || { views: 0, likes: 0, comments: 0 };
    existing.views += v.views;
    existing.likes += v.likes;
    existing.comments += v.comments;
    dateMap.set(date, existing);
  });

  return Array.from(dateMap.entries())
    .map(([date, data]) => ({
      date,
      views: data.views,
      likes: data.likes,
      comments: data.comments,
    }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

function getVideoTypeBreakdown(youtube: YouTubeVideo[]) {
  const shortsViews = youtube
    .filter(v => v.video_type === 'Shorts')
    .reduce((sum, v) => sum + v.views, 0);

  const regularViews = youtube
    .filter(v => v.video_type === 'Regular')
    .reduce((sum, v) => sum + v.views, 0);

  const total = shortsViews + regularViews;

  return [
    {
      name: 'Shorts',
      value: shortsViews,
      percentage: total > 0 ? Math.round((shortsViews / total) * 100) : 0,
      color: '#FF0000'  // YouTube Red
    },
    {
      name: 'סרטונים רגילים',
      value: regularViews,
      percentage: total > 0 ? Math.round((regularViews / total) * 100) : 0,
      color: '#282828'  // YouTube Dark
    },
  ];
}

function getLikeRateData(youtube: YouTubeVideo[]) {
  // Group by date and calculate average like rate
  const dateMap = new Map<string, { sum: number; count: number }>();

  youtube.forEach(v => {
    const date = v.published_at.split('T')[0];
    const existing = dateMap.get(date) || { sum: 0, count: 0 };
    existing.sum += v.like_rate;
    existing.count += 1;
    dateMap.set(date, existing);
  });

  return Array.from(dateMap.entries())
    .map(([date, data]) => ({
      date,
      likeRate: data.count > 0 ? data.sum / data.count : 0,
    }))
    .sort((a, b) => a.date.localeCompare(b.date));
}
