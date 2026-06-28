import { NextResponse } from 'next/server';
import {
  getAllDashboardData,
  filterByDateRange,
  type TwitterPost,
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
    const filteredTwitter = filterByDateRange(data.twitter, days);

    // Filter by date range - previous period (for comparison)
    const prevTwitter = filterByDateRange(data.twitter, days * 2).filter(p => {
      const date = new Date(p.date);
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - days);
      return date < cutoff;
    });

    // Get latest followers data
    const latestFollowers = data.followers.length > 0
      ? data.followers[data.followers.length - 1]
      : null;

    const stats = calculateTwitterStats(filteredTwitter, latestFollowers);
    const prevStats = calculateTwitterStats(prevTwitter, null);

    const performanceData = getTwitterPerformanceData(filteredTwitter);
    const prevPerformanceData = getTwitterPerformanceData(prevTwitter);

    const postTypeBreakdown = getTweetTypeBreakdown(filteredTwitter);

    const posts = filteredTwitter
      .map(p => ({
        id: p.tweet_id,
        text: p.text.substring(0, 80) + (p.text.length > 80 ? '...' : ''),
        date: p.date,
        type: p.type,
        views: p.views,
        likes: p.likes,
        retweets: p.retweets,
        replies: p.replies,
        engagementRate: p.engagement_rate,
        url: p.permalink,
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
        posts,
        lastUpdated: new Date().toISOString(),
      },
    });
  } catch (error) {
    console.error('Error fetching Twitter data:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to fetch Twitter data' },
      { status: 500 }
    );
  }
}

function calculateTwitterStats(twitter: TwitterPost[], followers: FollowersData | null) {
  const totalViews = twitter.reduce((sum, p) => sum + p.views, 0);
  const totalLikes = twitter.reduce((sum, p) => sum + p.likes, 0);
  const totalRetweets = twitter.reduce((sum, p) => sum + p.retweets, 0);
  const totalReplies = twitter.reduce((sum, p) => sum + p.replies, 0);

  const avgEngagementRate = twitter.length > 0
    ? twitter.reduce((sum, p) => sum + p.engagement_rate, 0) / twitter.length
    : 0;

  const videoCount = twitter.filter(p => p.type === 'Video').length;
  const photoCount = twitter.filter(p => p.type === 'Photo').length;
  const textCount = twitter.filter(p => p.type === 'Text').length;

  return {
    followers: followers?.tw_followers || 0,
    followersChange: followers?.tw_followers_change || 0,
    totalViews,
    totalLikes,
    totalRetweets,
    totalReplies,
    avgEngagementRate,
    postCount: twitter.length,
    videoCount,
    photoCount,
    textCount,
  };
}

function getTwitterPerformanceData(twitter: TwitterPost[]) {
  const dateMap = new Map<string, { views: number; engagement: number }>();

  twitter.forEach(p => {
    const date = p.date.split('T')[0];
    const existing = dateMap.get(date) || { views: 0, engagement: 0 };
    existing.views += p.views;
    existing.engagement += p.likes + p.retweets + p.replies + p.quotes;
    dateMap.set(date, existing);
  });

  return Array.from(dateMap.entries())
    .map(([date, d]) => ({ date, views: d.views, engagement: d.engagement }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

function getTweetTypeBreakdown(twitter: TwitterPost[]) {
  const types: Array<TwitterPost['type']> = ['Video', 'Photo', 'Text'];
  const colors = ['#1D9BF0', '#794BC4', '#536471'];

  const breakdown = types.map((type, index) => {
    const posts = twitter.filter(p => p.type === type);
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
