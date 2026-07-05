import { NextResponse } from 'next/server';
import {
  getAllDashboardData,
  filterByDateRange,
  type InstagramPost,
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
    const filteredInstagram = filterByDateRange(data.instagram, days);

    // Filter by date range - previous period (for comparison)
    const prevInstagram = filterByDateRange(data.instagram, days * 2).filter(p => {
      const date = new Date(p.date);
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - days);
      return date < cutoff;
    });

    // Get latest followers data
    const latestFollowers = data.followers.length > 0
      ? data.followers[data.followers.length - 1]
      : null;

    // Calculate Instagram-specific stats for current and previous periods
    const stats = calculateInstagramStats(filteredInstagram, latestFollowers);
    const prevStats = calculateInstagramStats(prevInstagram, null);

    // Get performance over time (views + reach)
    const performanceData = getInstagramPerformanceData(filteredInstagram);
    const prevPerformanceData = getInstagramPerformanceData(prevInstagram);

    // Get post type breakdown
    const postTypeBreakdown = getPostTypeBreakdown(filteredInstagram);

    // Get saves vs shares comparison
    const engagementComparison = getEngagementComparison(filteredInstagram);

    // Get all posts sorted by views
    const posts = filteredInstagram
      .map(p => ({
        id: p.media_id,
        caption: p.caption.substring(0, 80) + (p.caption.length > 80 ? '...' : ''),
        date: p.date,
        type: p.type,
        views: p.views,
        reach: p.reach,
        saved: p.saved,
        shares: p.shares,
        engagementRate: p.engagement_rate,
        url: p.type === 'Reel'
          ? `https://www.instagram.com/reel/${p.media_id}`
          : `https://www.instagram.com/p/${p.media_id}`,
      }))
      .sort((a, b) => b.views - a.views);

    return NextResponse.json({
      success: true,
      data: {
        stats,
        prevStats,
        performanceData,
        prevPerformanceData,
        postTypeBreakdown,
        engagementComparison,
        posts,
        lastUpdated: new Date().toISOString(),
      },
    });
  } catch (error) {
    console.error('Error fetching Instagram data:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to fetch Instagram data' },
      { status: 500 }
    );
  }
}

function calculateInstagramStats(instagram: InstagramPost[], followers: FollowersData | null) {
  const totalViews = instagram.reduce((sum, p) => sum + p.views, 0);
  const totalReach = instagram.reduce((sum, p) => sum + p.reach, 0);
  const totalSaved = instagram.reduce((sum, p) => sum + p.saved, 0);
  const totalShares = instagram.reduce((sum, p) => sum + p.shares, 0);

  // Average engagement rate
  const avgEngagementRate = instagram.length > 0
    ? instagram.reduce((sum, p) => sum + p.engagement_rate, 0) / instagram.length
    : 0;

  // Count by type
  const reelCount = instagram.filter(p => p.type === 'Reel').length;
  const photoCount = instagram.filter(p => p.type === 'Photo').length;
  const carouselCount = instagram.filter(p => p.type === 'Carousel').length;

  return {
    followers: followers?.ig_followers || 0,
    followersChange: followers?.ig_followers_change || 0,
    totalViews,
    totalReach,
    totalSaved,
    totalShares,
    avgEngagementRate,
    postCount: instagram.length,
    reelCount,
    photoCount,
    carouselCount,
  };
}

function getInstagramPerformanceData(instagram: InstagramPost[]) {
  // Group by date
  const dateMap = new Map<string, { views: number; reach: number; engagement: number }>();

  instagram.forEach(p => {
    const date = p.date.split('T')[0];
    const existing = dateMap.get(date) || { views: 0, reach: 0, engagement: 0 };
    existing.views += p.views;
    existing.reach += p.reach;
    existing.engagement += p.saved + p.shares;
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

function getPostTypeBreakdown(instagram: InstagramPost[]) {
  const types = ['Reel', 'Photo', 'Carousel'];
  const colors = ['#E4405F', '#F77737', '#833AB4'];

  const breakdown = types.map((type, index) => {
    const posts = instagram.filter(p => p.type === type);
    const views = posts.reduce((sum, p) => sum + p.views, 0);
    return {
      name: type,
      value: views,
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

function getEngagementComparison(instagram: InstagramPost[]) {
  // Group by date and compare saves vs shares
  const dateMap = new Map<string, { saved: number; shares: number }>();

  instagram.forEach(p => {
    const date = p.date.split('T')[0];
    const existing = dateMap.get(date) || { saved: 0, shares: 0 };
    existing.saved += p.saved;
    existing.shares += p.shares;
    dateMap.set(date, existing);
  });

  return Array.from(dateMap.entries())
    .map(([date, data]) => ({
      date,
      saved: data.saved,
      shares: data.shares,
    }))
    .sort((a, b) => a.date.localeCompare(b.date));
}
