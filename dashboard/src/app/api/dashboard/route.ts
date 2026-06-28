import { NextResponse } from 'next/server';
import {
  getAllDashboardData,
  filterByDateRange,
  type YouTubeVideo,
  type FacebookPost,
  type InstagramPost,
  type FollowersData,
  type DailyInsight
} from '@/lib/sheets';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const days = parseInt(searchParams.get('days') || '7', 10);

    const data = await getAllDashboardData();

    // Filter by date range - current period
    const filteredYouTube = filterByDateRange(data.youtube, days);
    const filteredFacebook = filterByDateRange(data.facebook, days);
    const filteredInstagram = filterByDateRange(data.instagram, days);

    // Filter by date range - previous period (for comparison)
    const prevYouTube = filterByDateRange(data.youtube, days * 2).filter(v => {
      const date = new Date(v.published_at);
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - days);
      return date < cutoff;
    });
    const prevFacebook = filterByDateRange(data.facebook, days * 2).filter(p => {
      const date = new Date(p.date);
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - days);
      return date < cutoff;
    });
    const prevInstagram = filterByDateRange(data.instagram, days * 2).filter(p => {
      const date = new Date(p.date);
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - days);
      return date < cutoff;
    });

    // Get latest followers data (last entry is most recent)
    const latestFollowers = data.followers.length > 0
      ? data.followers[data.followers.length - 1]
      : null;

    // Calculate aggregate stats for current and previous periods
    const stats = calculateStats(filteredYouTube, filteredFacebook, filteredInstagram, latestFollowers);
    const prevStats = calculateStats(prevYouTube, prevFacebook, prevInstagram, null);

    // Get top performing content
    const topContent = getTopContent(filteredYouTube, filteredFacebook, filteredInstagram);

    // Get performance over time data for charts
    const performanceData = getPerformanceData(filteredYouTube, filteredFacebook, filteredInstagram);

    // Calculate engagement breakdown
    const engagementBreakdown = calculateEngagementBreakdown(filteredYouTube, filteredFacebook, filteredInstagram);

    // Get the actual last data date from followers sheet
    const lastDataDate = latestFollowers?.date || null;

    // Get latest insight (most recent entry)
    const latestInsight = data.insights.length > 0
      ? data.insights[data.insights.length - 1]
      : null;

    return NextResponse.json({
      success: true,
      data: {
        youtube: filteredYouTube,
        facebook: filteredFacebook,
        instagram: filteredInstagram,
        followers: latestFollowers,
        stats,
        prevStats,
        topContent,
        performanceData,
        engagementBreakdown,
        lastDataDate,
        latestInsight,
      },
    });
  } catch (error) {
    console.error('Error fetching dashboard data:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to fetch dashboard data' },
      { status: 500 }
    );
  }
}

function calculateStats(
  youtube: YouTubeVideo[],
  facebook: FacebookPost[],
  instagram: InstagramPost[],
  followers: FollowersData | null
) {
  // Total views across all platforms
  const totalViews =
    youtube.reduce((sum, v) => sum + v.views, 0) +
    facebook.reduce((sum, p) => sum + p.views, 0) +
    instagram.reduce((sum, p) => sum + p.views, 0);

  // Total reach (Facebook + Instagram only)
  const totalReach =
    facebook.reduce((sum, p) => sum + p.reach, 0) +
    instagram.reduce((sum, p) => sum + p.reach, 0);

  // Total engagement (likes + comments + shares + saved)
  const totalEngagement =
    youtube.reduce((sum, v) => sum + v.likes + v.comments, 0) +
    facebook.reduce((sum, p) => sum + p.likes + p.shares, 0) +
    instagram.reduce((sum, p) => sum + p.saved + p.shares, 0);

  // Average engagement rate
  const engagementRates = [
    ...facebook.map(p => p.engagement_rate),
    ...instagram.map(p => p.engagement_rate),
  ];
  const avgEngagementRate = engagementRates.length > 0
    ? engagementRates.reduce((a, b) => a + b, 0) / engagementRates.length
    : 0;

  // Content count by platform
  const contentCount = {
    youtube: youtube.length,
    facebook: facebook.length,
    instagram: instagram.length,
    total: youtube.length + facebook.length + instagram.length,
  };

  // Followers data
  const followersStats = followers ? {
    youtube: followers.yt_subscribers,
    youtubeChange: followers.yt_subscribers_change,
    facebook: followers.fb_followers,
    facebookChange: followers.fb_followers_change,
    instagram: followers.ig_followers,
    instagramChange: followers.ig_followers_change,
    total: followers.yt_subscribers + followers.fb_followers + followers.ig_followers,
  } : null;

  // Platform breakdown
  const platformViews = {
    youtube: youtube.reduce((sum, v) => sum + v.views, 0),
    facebook: facebook.reduce((sum, p) => sum + p.views, 0),
    instagram: instagram.reduce((sum, p) => sum + p.views, 0),
  };

  return {
    totalViews,
    totalReach,
    totalEngagement,
    avgEngagementRate,
    contentCount,
    followers: followersStats,
    platformViews,
  };
}

function getTopContent(
  youtube: YouTubeVideo[],
  facebook: FacebookPost[],
  instagram: InstagramPost[]
) {
  // Combine and sort by views
  const allContent = [
    ...youtube.map(v => ({
      id: v.video_id,
      title: v.title,
      platform: 'youtube' as const,
      views: v.views,
      engagement: v.likes + v.comments,
      date: v.published_at,
      type: v.video_type,
      url: v.video_type === 'Shorts'
        ? `https://www.youtube.com/shorts/${v.video_id}`
        : `https://www.youtube.com/watch?v=${v.video_id}`,
    })),
    ...facebook.map(p => ({
      id: p.post_id,
      title: p.title,
      platform: 'facebook' as const,
      views: p.views,
      engagement: p.likes + p.shares,
      date: p.date,
      type: p.type,
      url: `https://www.facebook.com/220634478361516/posts/${p.post_id}`,
    })),
    ...instagram.map(p => ({
      id: p.media_id,
      title: p.caption.substring(0, 50) + (p.caption.length > 50 ? '...' : ''),
      platform: 'instagram' as const,
      views: p.views,
      engagement: p.saved + p.shares,
      date: p.date,
      type: p.type,
      url: p.type === 'Reel'
        ? `https://www.instagram.com/reel/${p.media_id}`
        : `https://www.instagram.com/p/${p.media_id}`,
    })),
  ];

  // Sort by views and return top 10
  return allContent
    .sort((a, b) => b.views - a.views)
    .slice(0, 10);
}

function getPerformanceData(
  youtube: YouTubeVideo[],
  facebook: FacebookPost[],
  instagram: InstagramPost[]
) {
  // Group by date and calculate daily totals
  const dateMap = new Map<string, { youtube: number; facebook: number; instagram: number }>();

  youtube.forEach(v => {
    const date = v.published_at.split('T')[0];
    const existing = dateMap.get(date) || { youtube: 0, facebook: 0, instagram: 0 };
    existing.youtube += v.views;
    dateMap.set(date, existing);
  });

  facebook.forEach(p => {
    const date = p.date.split('T')[0];
    const existing = dateMap.get(date) || { youtube: 0, facebook: 0, instagram: 0 };
    existing.facebook += p.views;
    dateMap.set(date, existing);
  });

  instagram.forEach(p => {
    const date = p.date.split('T')[0];
    const existing = dateMap.get(date) || { youtube: 0, facebook: 0, instagram: 0 };
    existing.instagram += p.views;
    dateMap.set(date, existing);
  });

  // Convert to array and sort by date
  return Array.from(dateMap.entries())
    .map(([date, data]) => ({
      date,
      youtube: data.youtube,
      facebook: data.facebook,
      instagram: data.instagram,
      total: data.youtube + data.facebook + data.instagram,
    }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

function calculateEngagementBreakdown(
  youtube: YouTubeVideo[],
  facebook: FacebookPost[],
  instagram: InstagramPost[]
) {
  // Calculate total engagement by type
  const likes =
    youtube.reduce((sum, v) => sum + v.likes, 0) +
    facebook.reduce((sum, p) => sum + p.likes, 0);

  const comments = youtube.reduce((sum, v) => sum + v.comments, 0);

  const shares =
    facebook.reduce((sum, p) => sum + p.shares, 0) +
    instagram.reduce((sum, p) => sum + p.shares, 0);

  const saves = instagram.reduce((sum, p) => sum + p.saved, 0);

  const total = likes + comments + shares + saves;

  if (total === 0) {
    return [
      { name: "לייקים", value: 25, color: "rose" },
      { name: "תגובות", value: 25, color: "cyan" },
      { name: "שיתופים", value: 25, color: "amber" },
      { name: "שמירות", value: 25, color: "violet" },
    ];
  }

  return [
    { name: "לייקים", value: Math.round((likes / total) * 100), color: "rose" },
    { name: "תגובות", value: Math.round((comments / total) * 100), color: "cyan" },
    { name: "שיתופים", value: Math.round((shares / total) * 100), color: "amber" },
    { name: "שמירות", value: Math.round((saves / total) * 100), color: "violet" },
  ];
}
