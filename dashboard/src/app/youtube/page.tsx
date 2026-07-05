"use client";

import { useState, useEffect, Suspense, useMemo } from "react";
import { Users, Eye, ThumbsUp, MessageCircle, Video, Loader2, TrendingUp, TrendingDown } from "lucide-react";
import { useDateRange } from "@/hooks/use-date-range";
import { Header } from "@/components/layout/header";
import { KPICard } from "@/components/dashboard/kpi-card";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { DataTable } from "@/components/ui/data-table";
import { formatCompactNumber, formatRelativeTime } from "@/lib/utils";
import {
  AreaChart,
  Area,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Legend,
} from "recharts";

interface YouTubeStats {
  subscribers: number;
  subscribersChange: number;
  totalViews: number;
  totalViewsDelta: number;
  totalLikes: number;
  totalComments: number;
  avgLikeRate: number;
  videoCount: number;
  shortsCount: number;
  regularCount: number;
}

interface YouTubeData {
  stats: YouTubeStats;
  prevStats: YouTubeStats | null;
  performanceData: Array<{
    date: string;
    views: number;
    likes: number;
    comments: number;
  }>;
  prevPerformanceData: Array<{
    date: string;
    views: number;
    likes: number;
    comments: number;
  }>;
  videoTypeBreakdown: Array<{
    name: string;
    value: number;
    percentage: number;
    color: string;
  }>;
  likeRateData: Array<{
    date: string;
    likeRate: number;
  }>;
  videos: Array<{
    id: string;
    title: string;
    publishedAt: string;
    views: number;
    viewsDelta: number;
    likes: number;
    comments: number;
    likeRate: number;
    type: string;
    url: string;
  }>;
  lastUpdated: string;
}

function YouTubeContent() {
  const { dateRange, setDateRange } = useDateRange("7");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<YouTubeData | null>(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`/api/youtube?days=${dateRange}`);
      const result = await response.json();
      if (result.success) {
        setData(result.data);
      } else {
        setError(result.error || "Failed to fetch data");
      }
    } catch (err) {
      setError("Failed to connect to the server");
      console.error("Error fetching YouTube data:", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [dateRange]);

  // Per-video engagement rate: (likes + comments) / views
  const videosWithEngagement = useMemo(
    () =>
      (data?.videos || []).map((v) => ({
        ...v,
        engagementRate: v.views > 0 ? ((v.likes + v.comments) / v.views) * 100 : 0,
      })),
    [data]
  );

  const formattedPerformanceData = (data?.performanceData || []).map((d, index) => {
    const prevData = data?.prevPerformanceData?.[index];
    return {
      ...d,
      date: new Date(d.date).toLocaleDateString("he-IL", {
        day: "2-digit",
        month: "2-digit",
      }),
      prevViews: prevData?.views ?? null,
    };
  });

  return (
    <div className="min-h-screen bg-background">
      <Header
        dateRange={dateRange}
        onDateRangeChange={setDateRange}
        onRefresh={fetchData}
        isLoading={loading}
      />

      <main className="max-w-screen-2xl mx-auto p-4 lg:p-6 space-y-6">
        {/* Page Title */}
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-lg bg-red-500 flex items-center justify-center">
            <svg className="w-6 h-6 fill-white" viewBox="0 0 24 24">
              <path d="M19.615 3.184c-3.604-.246-11.631-.245-15.23 0-3.897.266-4.356 2.62-4.385 8.816.029 6.185.484 8.549 4.385 8.816 3.6.245 11.626.246 15.23 0 3.897-.266 4.356-2.62 4.385-8.816-.029-6.185-.484-8.549-4.385-8.816zm-10.615 12.816v-8l8 3.993-8 4.007z"/>
            </svg>
          </div>
          <div>
            <h1 className="text-xl font-bold">YouTube Analytics</h1>
            <p className="text-sm text-muted-foreground">
              ניתוח ביצועי ערוץ YouTube של כאן חדשות
            </p>
          </div>
        </div>

        {/* Last Updated */}
        {data?.lastUpdated && (
          <p className="text-xs text-muted-foreground">
            עודכן {formatRelativeTime(data.lastUpdated)}
          </p>
        )}

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
            {/* KPI Cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <KPICard
                title="מנויים"
                value={data.stats.subscribers}
                change={data.stats.subscribersChange}
                icon={Users}
                iconColor="bg-red-500/10 text-red-600"
              />
              <KPICard
                title="צפיות"
                value={data.stats.totalViews}
                prevValue={data.prevStats?.totalViews}
                icon={Eye}
                iconColor="bg-red-500/10 text-red-600"
              />
              <KPICard
                title="לייקים"
                value={data.stats.totalLikes}
                prevValue={data.prevStats?.totalLikes}
                icon={ThumbsUp}
                iconColor="bg-red-500/10 text-red-600"
              />
              <KPICard
                title="תגובות"
                value={data.stats.totalComments}
                prevValue={data.prevStats?.totalComments}
                icon={MessageCircle}
                iconColor="bg-red-500/10 text-red-600"
              />
            </div>

            {/* Charts Row */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              {/* Views Over Time - Takes 2 columns */}
              <Card className="lg:col-span-2">
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">צפיות לאורך זמן</CardTitle>
                  <CardDescription className="text-xs">צפיות יומיות בסרטונים</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={formattedPerformanceData}>
                        <defs>
                          <linearGradient id="colorViews" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#FF0000" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="#FF0000" stopOpacity={0} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                        <XAxis
                          dataKey="date"
                          tick={{ fontSize: 11 }}
                          tickLine={false}
                          axisLine={false}
                        />
                        <YAxis
                          tick={{ fontSize: 11 }}
                          tickLine={false}
                          axisLine={false}
                          tickFormatter={(value) => formatCompactNumber(value)}
                        />
                        <Tooltip
                          content={({ active, payload, label }) => {
                            if (!active || !payload) return null;
                            const viewsEntry = payload.find(p => p.dataKey === 'views');
                            const prevViewsEntry = payload.find(p => p.dataKey === 'prevViews');
                            return (
                              <div className="rounded-lg border bg-white dark:bg-gray-800 p-3 shadow-lg">
                                <p className="font-bold text-sm mb-2">{label}</p>
                                <p className="text-sm">
                                  צפיות: <span className="font-bold">{formatCompactNumber(viewsEntry?.value as number)}</span>
                                </p>
                                {prevViewsEntry?.value != null && (
                                  <p className="text-sm text-gray-400">
                                    תקופה קודמת: {formatCompactNumber(prevViewsEntry.value as number)}
                                  </p>
                                )}
                              </div>
                            );
                          }}
                        />
                        <Legend
                          verticalAlign="top"
                          height={36}
                          content={() => (
                            <div className="flex justify-center gap-4 text-sm mb-2">
                              <span className="flex items-center gap-1">
                                <span className="w-3 h-0.5 bg-[#FF0000]"></span>
                                צפיות
                              </span>
                              <span className="flex items-center gap-1">
                                <span className="w-3 h-0.5 bg-gray-400" style={{ backgroundImage: 'repeating-linear-gradient(90deg, #9CA3AF 0, #9CA3AF 3px, transparent 3px, transparent 6px)' }}></span>
                                תקופה קודמת
                              </span>
                            </div>
                          )}
                        />
                        <Area
                          type="monotone"
                          dataKey="views"
                          name="צפיות"
                          stroke="#FF0000"
                          strokeWidth={2}
                          fill="url(#colorViews)"
                        />
                        <Line
                          type="monotone"
                          dataKey="prevViews"
                          name="תקופה קודמת"
                          stroke="#9CA3AF"
                          strokeWidth={1.5}
                          strokeDasharray="5 5"
                          dot={false}
                          connectNulls
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              {/* Video Type Breakdown - Pie Chart */}
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">סוג תוכן</CardTitle>
                  <CardDescription className="text-xs">Shorts vs Regular</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Pie
                          data={data.videoTypeBreakdown}
                          cx="50%"
                          cy="50%"
                          innerRadius={50}
                          outerRadius={80}
                          paddingAngle={5}
                          dataKey="value"
                        >
                          {data.videoTypeBreakdown.map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={entry.color} />
                          ))}
                        </Pie>
                        <Tooltip
                          content={({ active, payload }) => {
                            if (!active || !payload || !payload[0]) return null;
                            const entry = payload[0].payload;
                            return (
                              <div className="rounded-lg border bg-white dark:bg-gray-800 p-3 shadow-lg">
                                <p className="font-bold text-sm">{entry.name}</p>
                                <p className="text-sm">{formatCompactNumber(entry.value)} צפיות</p>
                                <p className="text-sm text-muted-foreground">{entry.percentage}%</p>
                              </div>
                            );
                          }}
                        />
                        <Legend
                          verticalAlign="bottom"
                          formatter={(value: string) => (
                            <span className="text-sm">{value}</span>
                          )}
                        />
                      </PieChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Video Stats Summary */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.videoCount}</div>
                  <div className="text-xs text-muted-foreground">סה"כ סרטונים</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.shortsCount}</div>
                  <div className="text-xs text-muted-foreground">Shorts</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.regularCount}</div>
                  <div className="text-xs text-muted-foreground">סרטונים רגילים</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.avgLikeRate.toFixed(2)}%</div>
                  <div className="text-xs text-muted-foreground">ממוצע לייקים</div>
                </CardContent>
              </Card>
            </div>

            {/* Videos Table */}
            <Card>
              <CardHeader>
                <CardTitle className="text-base">סרטונים מובילים</CardTitle>
                <CardDescription className="text-xs">
                  {data.stats.videoCount} סרטונים ב-{dateRange} הימים האחרונים
                  <span className="mr-4 inline-flex items-center gap-3">
                    <span className="inline-flex items-center gap-1">
                      <span className="w-3 h-3 rounded bg-green-500"></span>
                      <span>צפיות גבוהות</span>
                    </span>
                    <span className="inline-flex items-center gap-1">
                      <span className="w-3 h-3 rounded bg-blue-500"></span>
                      <span>מעורבות גבוהה</span>
                    </span>
                  </span>
                </CardDescription>
              </CardHeader>
              <CardContent>
                <DataTable
                  data={videosWithEngagement}
                  searchable={true}
                  searchKeys={["title"]}
                  searchPlaceholder="חיפוש לפי כותרת..."
                  exportable={true}
                  exportFileName="youtube_videos"
                  showRank={true}
                  rankToggle={true}
                  rankKey="engagementRate"
                  defaultSortKey="engagementRate"
                  defaultSortDirection="desc"
                  columns={[
                    {
                      key: "title",
                      header: "כותרת",
                      sortable: true,
                      render: (video) => (
                        <div className="flex-1 min-w-0">
                          <div className="max-w-xs truncate font-medium">{video.title}</div>
                          <div className="text-xs text-muted-foreground">
                            {new Date(video.publishedAt).toLocaleDateString("he-IL")}
                          </div>
                        </div>
                      ),
                    },
                    {
                      key: "type",
                      header: "סוג",
                      sortable: true,
                      align: "center",
                      render: (video) => (
                        <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${
                          video.type === 'Shorts'
                            ? 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300'
                            : 'bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300'
                        }`}>
                          {video.type}
                        </span>
                      ),
                    },
                    {
                      key: "views",
                      header: "צפיות",
                      sortable: true,
                      align: "center",
                      render: (video) => (
                        <span className="font-medium">
                          {formatCompactNumber(video.views)}
                          {video.viewsDelta > 0 && (
                            <span className="text-xs text-green-600 mr-1">
                              +{formatCompactNumber(video.viewsDelta)}
                            </span>
                          )}
                        </span>
                      ),
                    },
                    {
                      key: "likes",
                      header: "לייקים",
                      sortable: true,
                      align: "center",
                      render: (video) => formatCompactNumber(video.likes),
                    },
                    {
                      key: "comments",
                      header: "תגובות",
                      sortable: true,
                      align: "center",
                      render: (video) => formatCompactNumber(video.comments),
                    },
                    {
                      key: "likeRate",
                      header: "% לייקים",
                      sortable: true,
                      align: "center",
                      render: (video) => `${video.likeRate.toFixed(2)}%`,
                    },
                    {
                      key: "engagementRate",
                      header: "% מעורבות",
                      sortable: true,
                      align: "center",
                      render: (video) => (
                        <span className="font-medium">{video.engagementRate.toFixed(2)}%</span>
                      ),
                    },
                  ]}
                  getRowUrl={(video) => video.url}
                  getRowHighlight={(video) => {
                    const avgViews = data.stats.totalViews / data.stats.videoCount;
                    const avgEngagement = (data.stats.totalLikes + data.stats.totalComments) / data.stats.videoCount;
                    const videoEngagement = video.likes + video.comments;
                    if (video.views > avgViews * 2) return "views";
                    if (videoEngagement > avgEngagement * 2) return "engagement";
                    return null;
                  }}
                />
              </CardContent>
            </Card>
          </>
        )}
      </main>
    </div>
  );
}

export default function YouTubePage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-background flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
      </div>
    }>
      <YouTubeContent />
    </Suspense>
  );
}
