"use client";

import { useEffect, useMemo, useState, Suspense } from "react";
import { Users, Eye, Heart, Video, Loader2, TrendingUp, TrendingDown, ExternalLink, Sparkles } from "lucide-react";
import { useDateRange } from "@/hooks/use-date-range";
import { Header } from "@/components/layout/header";
import { PlatformCard } from "@/components/dashboard/platform-card";
import { PerformanceChart } from "@/components/charts/performance-chart";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatCompactNumber } from "@/lib/utils";

// Platform Icons
const YouTubeIcon = ({ className = "w-5 h-5" }: { className?: string }) => (
  <svg className={`fill-current ${className}`} viewBox="0 0 24 24">
    <path d="M19.615 3.184c-3.604-.246-11.631-.245-15.23 0-3.897.266-4.356 2.62-4.385 8.816.029 6.185.484 8.549 4.385 8.816 3.6.245 11.626.246 15.23 0 3.897-.266 4.356-2.62 4.385-8.816-.029-6.185-.484-8.549-4.385-8.816zm-10.615 12.816v-8l8 3.993-8 4.007z"/>
  </svg>
);

const FacebookIcon = ({ className = "w-5 h-5" }: { className?: string }) => (
  <svg className={`fill-current ${className}`} viewBox="0 0 24 24">
    <path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z"/>
  </svg>
);

const InstagramIcon = ({ className = "w-5 h-5" }: { className?: string }) => (
  <svg className={`fill-current ${className}`} viewBox="0 0 24 24">
    <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zm0-2.163c-3.259 0-3.667.014-4.947.072-4.358.2-6.78 2.618-6.98 6.98-.059 1.281-.073 1.689-.073 4.948 0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98 1.281.058 1.689.072 4.948.072 3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98-1.281-.059-1.69-.073-4.949-.073zm0 5.838c-3.403 0-6.162 2.759-6.162 6.162s2.759 6.163 6.162 6.163 6.162-2.759 6.162-6.163c0-3.403-2.759-6.162-6.162-6.162zm0 10.162c-2.209 0-4-1.79-4-4 0-2.209 1.791-4 4-4s4 1.791 4 4c0 2.21-1.791 4-4 4zm6.406-11.845c-.796 0-1.441.645-1.441 1.44s.645 1.44 1.441 1.44c.795 0 1.439-.645 1.439-1.44s-.644-1.44-1.439-1.44z"/>
  </svg>
);

const platformIcons: Record<string, React.FC<{ className?: string }>> = {
  youtube: YouTubeIcon,
  facebook: FacebookIcon,
  instagram: InstagramIcon,
};

const platformColors: Record<string, string> = {
  youtube: "bg-red-500",
  facebook: "bg-blue-600",
  instagram: "bg-gradient-to-tr from-yellow-400 via-red-500 to-purple-600",
};

interface DashboardData {
  stats: {
    totalViews: number;
    totalReach: number;
    totalEngagement: number;
    avgEngagementRate: number;
    contentCount: {
      youtube: number;
      facebook: number;
      instagram: number;
      total: number;
    };
    followers: {
      youtube: number;
      youtubeChange: number;
      facebook: number;
      facebookChange: number;
      instagram: number;
      instagramChange: number;
      total: number;
    } | null;
    platformViews: {
      youtube: number;
      facebook: number;
      instagram: number;
    };
  };
  prevStats: {
    totalViews: number;
    totalReach: number;
    totalEngagement: number;
    contentCount: {
      total: number;
    };
  } | null;
  topContent: Array<{
    id: string;
    title: string;
    platform: "youtube" | "facebook" | "instagram";
    views: number;
    engagement: number;
    date: string;
    type: string;
    url: string;
  }>;
  performanceData: Array<{
    date: string;
    youtube: number;
    facebook: number;
    instagram: number;
    total: number;
  }>;
  lastDataDate: string | null;
  latestInsight: {
    date: string;
    insights: string;
    timestamp: string;
  } | null;
}

// Follower Card Component (for platform-specific follower counts)
function FollowerCard({
  platform,
  label,
  value,
  change,
  icon: Icon,
  iconBgColor,
  iconTextColor,
}: {
  platform?: string;
  label: string;
  value: number;
  change?: number;
  icon: React.ElementType;
  iconBgColor: string;
  iconTextColor: string;
}) {
  const isPositive = change !== undefined ? change >= 0 : true;

  return (
    <Card className="relative overflow-hidden">
      <CardContent className="p-3">
        <div className="flex items-center gap-3">
          <div className={`p-2 rounded-lg ${iconBgColor} ${iconTextColor}`}>
            <Icon className="w-4 h-4" />
          </div>
          <div className="flex-1 min-w-0">
            <p className="text-xs text-muted-foreground">{label}</p>
            <p className="text-lg font-bold">{formatCompactNumber(value)}</p>
          </div>
          {change !== undefined && (
            <div className={`flex items-center gap-0.5 text-xs ${isPositive ? 'text-green-600' : 'text-red-500'}`}>
              {isPositive ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
              <span>{isPositive ? '+' : ''}{formatCompactNumber(change)}</span>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

// Performance KPI Card Component
function KPICard({
  title,
  value,
  prevValue,
  icon: Icon,
  iconColor,
  suffix,
}: {
  title: string;
  value: number;
  prevValue?: number;
  icon: React.ElementType;
  iconColor: string;
  suffix?: string;
}) {
  const changePercent = prevValue && prevValue > 0 ? ((value - prevValue) / prevValue) * 100 : 0;
  const isPositive = changePercent >= 0;

  return (
    <Card className="relative overflow-hidden border-r-4 border-r-kan">
      <CardContent className="p-4">
        <div className="flex items-start justify-between">
          <div>
            <p className="text-xs text-muted-foreground mb-1">{title}</p>
            <p className="text-2xl font-bold">
              {formatCompactNumber(value)}{suffix}
            </p>
            {prevValue !== undefined && prevValue > 0 && (
              <div className={`flex items-center gap-1 mt-1 text-xs ${isPositive ? 'text-green-600' : 'text-red-500'}`}>
                {isPositive ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
                <span>{isPositive ? '+' : ''}{changePercent.toFixed(1)}%</span>
                <span className="text-muted-foreground">מהתקופה הקודמת</span>
              </div>
            )}
          </div>
          <div className={`p-2 rounded-lg ${iconColor}`}>
            <Icon className="w-4 h-4" />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function OverviewContent() {
  const { dateRange, setDateRange } = useDateRange("7");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<DashboardData | null>(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`/api/dashboard?days=${dateRange}`);
      const result = await response.json();
      if (result.success) {
        setData(result.data);
      } else {
        setError(result.error || "Failed to fetch data");
      }
    } catch (err) {
      setError("Failed to connect to the server");
      console.error("Error fetching dashboard data:", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [dateRange]);

  // Prepare sparkline data for each platform
  const youtubeSparkline = useMemo(() => {
    return (data?.performanceData || []).map((d) => ({
      date: d.date,
      value: d.youtube,
    }));
  }, [data?.performanceData]);

  const facebookSparkline = useMemo(() => {
    return (data?.performanceData || []).map((d) => ({
      date: d.date,
      value: d.facebook,
    }));
  }, [data?.performanceData]);

  const instagramSparkline = useMemo(() => {
    return (data?.performanceData || []).map((d) => ({
      date: d.date,
      value: d.instagram,
    }));
  }, [data?.performanceData]);

  // Format the last data date - subtract 1 day since data is pulled at 08:30
  // and represents data collected up to the previous day
  const formatLastDataDate = (dateStr: string | null) => {
    if (!dateStr) return null;
    const date = new Date(dateStr);
    date.setDate(date.getDate() - 1); // Show yesterday since data is pulled at 08:30
    return date.toLocaleDateString("he-IL", {
      day: "numeric",
      month: "long",
      year: "numeric",
    });
  };

  return (
    <div className="min-h-screen bg-background">
      <Header
        dateRange={dateRange}
        onDateRangeChange={setDateRange}
        onRefresh={fetchData}
        isLoading={loading}
      />

      <main className="max-w-screen-2xl mx-auto p-4 lg:p-6 space-y-6">
        {/* Page Title & Last Updated */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-xl font-bold">סקירת ביצועים</h1>
            <p className="text-sm text-muted-foreground">
              סיכום ביצועים מכל הפלטפורמות
            </p>
          </div>
          {data?.lastDataDate && (
            <div className="text-left">
              <p className="text-xs text-muted-foreground">נתונים עד</p>
              <p className="text-sm font-medium">{formatLastDataDate(data.lastDataDate)}</p>
            </div>
          )}
        </div>

        {/* Loading State */}
        {loading && !data && (
          <div className="flex items-center justify-center p-12">
            <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
            <span className="mr-3 text-muted-foreground">טוען נתונים...</span>
          </div>
        )}

        {/* Error State */}
        {error && !loading && (
          <Card className="border-destructive bg-destructive/10">
            <CardContent className="p-6">
              <p className="font-bold text-destructive">שגיאה בטעינת הנתונים</p>
              <p className="text-sm mt-1 text-destructive/80">{error}</p>
              <button
                onClick={fetchData}
                className="mt-3 text-sm font-medium underline text-destructive"
              >
                נסה שוב
              </button>
            </CardContent>
          </Card>
        )}

        {/* Main Content */}
        {data && (
          <>
            {/* Row 1: Followers by Platform */}
            <div className="space-y-2">
              <h2 className="text-sm font-bold text-kan">עוקבים</h2>
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
                <FollowerCard
                  label="YouTube"
                  value={data.stats.followers?.youtube || 0}
                  change={data.stats.followers?.youtubeChange}
                  icon={YouTubeIcon}
                  iconBgColor="bg-red-500/10"
                  iconTextColor="text-red-500"
                />
                <FollowerCard
                  label="Facebook"
                  value={data.stats.followers?.facebook || 0}
                  change={data.stats.followers?.facebookChange}
                  icon={FacebookIcon}
                  iconBgColor="bg-blue-600/10"
                  iconTextColor="text-blue-600"
                />
                <FollowerCard
                  label="Instagram"
                  value={data.stats.followers?.instagram || 0}
                  change={data.stats.followers?.instagramChange}
                  icon={InstagramIcon}
                  iconBgColor="bg-pink-500/10"
                  iconTextColor="text-pink-500"
                />
                <FollowerCard
                  label="סה״כ עוקבים"
                  value={data.stats.followers?.total || 0}
                  icon={Users}
                  iconBgColor="bg-primary/10"
                  iconTextColor="text-primary"
                />
              </div>
            </div>

            {/* Row 2: Performance KPIs */}
            <div className="space-y-2">
              <h2 className="text-sm font-bold text-kan">ביצועים בתקופה</h2>
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              <KPICard
                title="צפיות"
                value={data.stats.totalViews}
                prevValue={data.prevStats?.totalViews}
                icon={Eye}
                iconColor="bg-purple-500/10 text-purple-600"
              />
              <KPICard
                title="חשיפה"
                value={data.stats.totalReach}
                prevValue={data.prevStats?.totalReach}
                icon={Users}
                iconColor="bg-blue-500/10 text-blue-600"
              />
              <KPICard
                title="אינטראקציות"
                value={data.stats.totalEngagement}
                prevValue={data.prevStats?.totalEngagement}
                icon={Heart}
                iconColor="bg-orange-500/10 text-orange-600"
              />
              <KPICard
                title="תכנים"
                value={data.stats.contentCount.total}
                prevValue={data.prevStats?.contentCount?.total}
                icon={Video}
                iconColor="bg-teal-500/10 text-teal-600"
              />
              </div>
            </div>

            {/* Performance Chart */}
            <PerformanceChart data={data.performanceData} />

            {/* Platform Cards */}
            <div>
              <h2 className="text-sm font-bold text-kan mb-3">פירוט לפי פלטפורמה</h2>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                <PlatformCard
                  platform="youtube"
                  followers={data.stats.followers?.youtube || 0}
                  views={data.stats.platformViews.youtube}
                  sparklineData={youtubeSparkline}
                />
                <PlatformCard
                  platform="facebook"
                  followers={data.stats.followers?.facebook || 0}
                  views={data.stats.platformViews.facebook}
                  sparklineData={facebookSparkline}
                />
                <PlatformCard
                  platform="instagram"
                  followers={data.stats.followers?.instagram || 0}
                  views={data.stats.platformViews.instagram}
                  sparklineData={instagramSparkline}
                />
              </div>
            </div>

            {/* Top Content */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">תכנים מובילים</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  {data.topContent.slice(0, 8).map((item) => {
                    const PlatformIcon = platformIcons[item.platform];
                    return (
                      <a
                        key={item.id}
                        href={item.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center gap-3 p-2 rounded-lg hover:bg-muted transition-colors group"
                      >
                        <div
                          className={`w-8 h-8 rounded-lg flex items-center justify-center text-white shrink-0 ${platformColors[item.platform]}`}
                        >
                          <PlatformIcon className="w-4 h-4" />
                        </div>
                        <div className="flex-1 min-w-0">
                          <p className="font-medium text-sm truncate">
                            {item.title}
                          </p>
                          <p className="text-xs text-muted-foreground">
                            {formatCompactNumber(item.views)} צפיות • {item.type}
                          </p>
                        </div>
                        <ExternalLink className="w-4 h-4 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity" />
                      </a>
                    );
                  })}
                  {data.topContent.length === 0 && (
                    <p className="text-center py-4 text-sm text-muted-foreground">
                      אין תכנים לתצוגה
                    </p>
                  )}
                </div>
              </CardContent>
            </Card>

            {/* AI Insights */}
            {data.latestInsight && (
              <Card className="border-r-4 border-r-kan">
                <CardHeader className="pb-2">
                  <div className="flex items-center gap-2">
                    <div className="p-1.5 rounded-lg bg-primary/10">
                      <Sparkles className="w-4 h-4 text-primary" />
                    </div>
                    <CardTitle className="text-base">תובנות AI יומיות</CardTitle>
                    <span className="text-xs text-muted-foreground mr-auto">
                      {new Date(data.latestInsight.date).toLocaleDateString("he-IL", {
                        day: "numeric",
                        month: "long",
                      })}
                    </span>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="prose prose-sm dark:prose-invert max-w-none">
                    <div className="whitespace-pre-wrap text-sm leading-relaxed text-foreground">
                      {data.latestInsight.insights}
                    </div>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Quick Stats */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">נתונים נוספים</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-center">
                  <div>
                    <div className="text-xl font-bold">
                      {data.stats.avgEngagementRate.toFixed(2)}%
                    </div>
                    <div className="text-xs text-muted-foreground">
                      ממוצע מעורבות
                    </div>
                  </div>
                  <div>
                    <div className="text-xl font-bold">
                      {formatCompactNumber(data.stats.totalReach)}
                    </div>
                    <div className="text-xs text-muted-foreground">
                      חשיפה כוללת
                    </div>
                  </div>
                  <div>
                    <div className="text-xl font-bold">
                      {data.stats.contentCount.youtube}
                    </div>
                    <div className="text-xs text-muted-foreground">
                      סרטוני YouTube
                    </div>
                  </div>
                  <div>
                    <div className="text-xl font-bold">
                      {data.stats.contentCount.facebook +
                        data.stats.contentCount.instagram}
                    </div>
                    <div className="text-xs text-muted-foreground">
                      פוסטים FB+IG
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </>
        )}
      </main>
    </div>
  );
}

export default function OverviewPage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-background flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
      </div>
    }>
      <OverviewContent />
    </Suspense>
  );
}
