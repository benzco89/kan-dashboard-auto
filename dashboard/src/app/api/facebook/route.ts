import { NextResponse } from 'next/server';
import {
  getAllDashboardData,
  filterByDateRange,
  type FacebookPost,
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
    const filteredFacebook = filterByDateRange(data.facebook, days);

    // Filter by date range - previous period (for comparison)
    const prevFacebook = filterByDateRange(data.facebook, days * 2).filter(p => {
      const date = new Date(p.date);
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - days);
      return date < cutoff;
    });

    // Get latest followers data
    const latestFollowers = data.followers.length > 0
      ? data.followers[data.followers.length - 1]
      : null;

    // Calculate Facebook-specific stats for current and previous periods
    const stats = calculateFacebookStats(filteredFacebook, latestFollowers);
    const prevStats = calculateFacebookStats(prevFacebook, null);

    // Get performance over time (views + reach)
    const performanceData = getFacebookPerformanceData(filteredFacebook);

    // Get post type breakdown
    const postTypeBreakdown = getPostTypeBreakdown(filteredFacebook);

    // Get all posts sorted by views
    const posts = filteredFacebook
      .map(p => ({
        id: p.post_id,
        title: p.title,
        date: p.date,
        type: p.type,
        views: p.views,
        reach: p.reach,
        likes: p.likes,
        shares: p.shares,
        engagementRate: p.engagement_rate,
        url: `https://www.facebook.com/220634478361516/posts/${p.post_id}`,
      }))
      .sort((a, b) => b.views - a.views);

    return NextResponse.json({
      success: true,
      data: {
        stats,
        prevStats,
        performanceData,
        postTypeBreakdown,
        posts,
        lastUpdated: new Date().toISOString(),
      },
    });
  } catch (error) {
    console.error('Error fetching Facebook data:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to fetch Facebook data' },
      { status: 500 }
    );
  }
}

function calculateFacebookStats(facebook: FacebookPost[], followers: FollowersData | null) {
  const totalViews = facebook.reduce((sum, p) => sum + p.views, 0);
  const totalReach = facebook.reduce((sum, p) => sum + p.reach, 0);
  const totalLikes = facebook.reduce((sum, p) => sum + p.likes, 0);
  const totalShares = facebook.reduce((sum, p) => sum + p.shares, 0);

  // Average engagement rate
  const avgEngagementRate = facebook.length > 0
    ? facebook.reduce((sum, p) => sum + p.engagement_rate, 0) / facebook.length
    : 0;

  // Count by type
  const reelsCount = facebook.filter(p => p.type === 'Reels').length;
  const imagesCount = facebook.filter(p => p.type === 'Images').length;
  const videosCount = facebook.filter(p => p.type === 'Videos').length;
  const linksCount = facebook.filter(p => p.type === 'Links').length;

  return {
    followers: followers?.fb_followers || 0,
    followersChange: followers?.fb_followers_change || 0,
    totalViews,
    totalReach,
    totalLikes,
    totalShares,
    avgEngagementRate,
    postCount: facebook.length,
    reelsCount,
    imagesCount,
    videosCount,
    linksCount,
  };
}

function getFacebookPerformanceData(facebook: FacebookPost[]) {
  // Group by date
  const dateMap = new Map<string, { views: number; reach: number; engagement: number }>();

  facebook.forEach(p => {
    const date = p.date.split('T')[0];
    const existing = dateMap.get(date) || { views: 0, reach: 0, engagement: 0 };
    existing.views += p.views;
    existing.reach += p.reach;
    existing.engagement += p.likes + p.shares;
    dateMap.set(date, existing);
  });

  return Array.from(dateMap.entries())
    .map(([date, data]) => ({
      date,
      views: data.views,
      reach: data.reach,
      engagement: data.engagement,
    }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

function getPostTypeBreakdown(facebook: FacebookPost[]) {
  const types = ['Reels', 'Videos', 'Images', 'Links'];
  const colors = ['#1877F2', '#0866FF', '#4A90D9', '#8BB8E8'];

  const breakdown = types.map((type, index) => {
    const posts = facebook.filter(p => p.type === type);
    const reach = posts.reduce((sum, p) => sum + p.reach, 0);
    const views = posts.reduce((sum, p) => sum + p.views, 0);
    return {
      name: type,
      value: reach, // Use reach instead of views for better cross-type comparison
      views: views,
      count: posts.length,
      color: colors[index],
    };
  });

  const total = breakdown.reduce((sum, b) => sum + b.value, 0);

  return breakdown.map(b => ({
    ...b,
    percentage: total > 0 ? Math.round((b.value / total) * 100) : 0,
  }));
}
