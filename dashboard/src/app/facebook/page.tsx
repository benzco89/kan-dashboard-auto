"use client";

import { useState, useEffect, Suspense, useMemo } from "react";
import { Users, Eye, Share2, ThumbsUp, Loader2, Target } from "lucide-react";
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
  BarChart,
  Bar,
  Legend,
  Cell,
} from "recharts";

interface FacebookStats {
  followers: number;
  followersChange: number;
  totalViews: number;
  totalReach: number;
  totalLikes: number;
  totalShares: number;
  avgEngagementRate: number;
  postCount: number;
  reelsCount: number;
  imagesCount: number;
  videosCount: number;
  linksCount: number;
}

interface FacebookData {
  stats: FacebookStats;
  prevStats: FacebookStats | null;
  performanceData: Array<{
    date: string;
    views: number;
    reach: number;
    engagement: number;
  }>;
  prevPerformanceData: Array<{
    date: string;
    views: number;
    reach: number;
    engagement: number;
  }>;
  postTypeBreakdown: Array<{
    name: string;
    value: number;
    views: number;
    count: number;
    percentage: number;
    color: string;
  }>;
  posts: Array<{
    id: string;
    title: string;
    date: string;
    type: string;
    views: number;
    reach: number;
    likes: number;
    shares: number;
    engagementRate: number;
    url: string;
  }>;
  lastUpdated: string;
}

function FacebookContent() {
  const { dateRange, setDateRange } = useDateRange("7");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<FacebookData | null>(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`/api/facebook?days=${dateRange}`);
      const result = await response.json();
      if (result.success) {
        setData(result.data);
      } else {
        setError(result.error || "Failed to fetch data");
      }
    } catch (err) {
      setError("Failed to connect to the server");
      console.error("Error fetching Facebook data:", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [dateRange]);

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
          <div className="w-10 h-10 rounded-lg bg-blue-600 flex items-center justify-center">
            <svg className="w-6 h-6 fill-white" viewBox="0 0 24 24">
              <path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z"/>
            </svg>
          </div>
          <div>
            <h1 className="text-xl font-bold">Facebook Analytics</h1>
            <p className="text-sm text-muted-foreground">
              ניתוח ביצועי עמוד Facebook של כאן חדשות
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
                title="עוקבים"
                value={data.stats.followers}
                change={data.stats.followersChange}
                icon={Users}
                iconColor="bg-blue-500/10 text-blue-600"
              />
              <KPICard
                title="צפיות"
                value={data.stats.totalViews}
                prevValue={data.prevStats?.totalViews}
                icon={Eye}
                iconColor="bg-blue-500/10 text-blue-600"
              />
              <KPICard
                title="חשיפה"
                value={data.stats.totalReach}
                prevValue={data.prevStats?.totalReach}
                icon={Target}
                iconColor="bg-blue-500/10 text-blue-600"
              />
              <KPICard
                title="שיתופים"
                value={data.stats.totalShares}
                prevValue={data.prevStats?.totalShares}
                icon={Share2}
                iconColor="bg-blue-500/10 text-blue-600"
              />
            </div>

            {/* Charts Row */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              {/* Views & Reach Over Time - Takes 2 columns */}
              <Card className="lg:col-span-2">
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">צפיות וחשיפה לאורך זמן</CardTitle>
                  <CardDescription className="text-xs">מדדים יומיים</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={formattedPerformanceData}>
                        <defs>
                          <linearGradient id="colorFbViews" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#1877F2" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="#1877F2" stopOpacity={0} />
                          </linearGradient>
                          <linearGradient id="colorFbReach" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#42B72A" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="#42B72A" stopOpacity={0} />
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
                            const reachEntry = payload.find(p => p.dataKey === 'reach');
                            const prevViewsEntry = payload.find(p => p.dataKey === 'prevViews');
                            return (
                              <div className="rounded-lg border bg-white dark:bg-gray-800 p-3 shadow-lg">
                                <p className="font-bold text-sm mb-2">{label}</p>
                                {viewsEntry && (
                                  <p className="text-sm">
                                    <span style={{ color: viewsEntry.color }}>צפיות:</span>{" "}
                                    <span className="font-bold">{formatCompactNumber(viewsEntry.value as number)}</span>
                                  </p>
                                )}
                                {reachEntry && (
                                  <p className="text-sm">
                                    <span style={{ color: reachEntry.color }}>חשיפה:</span>{" "}
                                    <span className="font-bold">{formatCompactNumber(reachEntry.value as number)}</span>
                                  </p>
                                )}
                                {prevViewsEntry?.value != null && (
                                  <p className="text-sm text-gray-400">
                                    תקופה קודמת (צפיות): {formatCompactNumber(prevViewsEntry.value as number)}
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
                                <span className="w-3 h-0.5 bg-[#1877F2]"></span>
                                צפיות
                              </span>
                              <span className="flex items-center gap-1">
                                <span className="w-3 h-0.5 bg-[#42B72A]"></span>
                                חשיפה
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
                          stroke="#1877F2"
                          strokeWidth={2}
                          fill="url(#colorFbViews)"
                        />
                        <Area
                          type="monotone"
                          dataKey="reach"
                          name="חשיפה"
                          stroke="#42B72A"
                          strokeWidth={2}
                          fill="url(#colorFbReach)"
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

              {/* Post Type Breakdown - Bar Chart */}
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">חשיפה לפי סוג</CardTitle>
                  <CardDescription className="text-xs">חשיפה לפי סוג תוכן</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={data.postTypeBreakdown} layout="vertical">
                        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" horizontal={false} />
                        <XAxis
                          type="number"
                          tick={{ fontSize: 11 }}
                          tickLine={false}
                          axisLine={false}
                          tickFormatter={(value) => formatCompactNumber(value)}
                        />
                        <YAxis
                          type="category"
                          dataKey="name"
                          tick={{ fontSize: 11 }}
                          tickLine={false}
                          axisLine={false}
                          width={60}
                        />
                        <Tooltip
                          content={({ active, payload }) => {
                            if (!active || !payload || !payload[0]) return null;
                            const entry = payload[0].payload;
                            return (
                              <div className="rounded-lg border bg-white dark:bg-gray-800 p-3 shadow-lg">
                                <p className="font-bold text-sm">{entry.name}</p>
                                <p className="text-sm">{formatCompactNumber(entry.value)} חשיפה</p>
                                <p className="text-sm text-muted-foreground">{formatCompactNumber(entry.views || 0)} צפיות</p>
                                <p className="text-sm text-muted-foreground">{entry.count} פוסטים</p>
                              </div>
                            );
                          }}
                        />
                        <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                          {data.postTypeBreakdown.map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={entry.color} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Post Stats Summary */}
            <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.postCount}</div>
                  <div className="text-xs text-muted-foreground">סה"כ פוסטים</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.reelsCount}</div>
                  <div className="text-xs text-muted-foreground">Reels</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.videosCount}</div>
                  <div className="text-xs text-muted-foreground">Videos</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.imagesCount}</div>
                  <div className="text-xs text-muted-foreground">Images</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.avgEngagementRate.toFixed(2)}%</div>
                  <div className="text-xs text-muted-foreground">ממוצע מעורבות</div>
                </CardContent>
              </Card>
            </div>

            {/* Posts Table */}
            <Card>
              <CardHeader>
                <CardTitle className="text-base">פוסטים מובילים</CardTitle>
                <CardDescription className="text-xs">
                  {data.stats.postCount} פוסטים ב-{dateRange} הימים האחרונים
                  <span className="mr-4 inline-flex items-center gap-3">
                    <span className="inline-flex items-center gap-1">
                      <span className="w-3 h-3 rounded bg-green-500"></span>
                      <span>צפיות גבוהות</span>
                    </span>
                    <span className="inline-flex items-center gap-1">
                      <span className="w-3 h-3 rounded bg-purple-500"></span>
                      <span>חשיפה גבוהה</span>
                    </span>
                  </span>
                </CardDescription>
              </CardHeader>
              <CardContent>
                <DataTable
                  data={data.posts}
                  searchable={true}
                  searchKeys={["title"]}
                  searchPlaceholder="חיפוש לפי כותרת..."
                  exportable={true}
                  exportFileName="facebook_posts"
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
                      render: (post) => (
                        <div className="flex-1 min-w-0">
                          <div className="max-w-xs truncate font-medium">{post.title}</div>
                          <div className="text-xs text-muted-foreground">
                            {new Date(post.date).toLocaleDateString("he-IL")}
                          </div>
                        </div>
                      ),
                    },
                    {
                      key: "type",
                      header: "סוג",
                      sortable: true,
                      align: "center",
                      render: (post) => (
                        <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${
                          post.type === 'Reels'
                            ? 'bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300'
                            : post.type === 'Videos'
                            ? 'bg-indigo-100 text-indigo-700 dark:bg-indigo-900 dark:text-indigo-300'
                            : post.type === 'Images'
                            ? 'bg-sky-100 text-sky-700 dark:bg-sky-900 dark:text-sky-300'
                            : 'bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300'
                        }`}>
                          {post.type}
                        </span>
                      ),
                    },
                    {
                      key: "views",
                      header: "צפיות",
                      sortable: true,
                      align: "center",
                      render: (post) => <span className="font-medium">{formatCompactNumber(post.views)}</span>,
                    },
                    {
                      key: "reach",
                      header: "חשיפה",
                      sortable: true,
                      align: "center",
                      render: (post) => formatCompactNumber(post.reach),
                    },
                    {
                      key: "likes",
                      header: "לייקים",
                      sortable: true,
                      align: "center",
                      render: (post) => formatCompactNumber(post.likes),
                    },
                    {
                      key: "shares",
                      header: "שיתופים",
                      sortable: true,
                      align: "center",
                      render: (post) => formatCompactNumber(post.shares),
                    },
                    {
                      key: "engagementRate",
                      header: "% מעורבות",
                      sortable: true,
                      align: "center",
                      render: (post) => `${post.engagementRate.toFixed(2)}%`,
                    },
                  ]}
                  getRowUrl={(post) => post.url}
                  getRowHighlight={(post) => {
                    const avgViews = data.stats.totalViews / data.stats.postCount;
                    const avgReach = data.stats.totalReach / data.stats.postCount;
                    if (post.views > avgViews * 2) return "views";
                    if (post.reach > avgReach * 2) return "reach";
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

export default function FacebookPage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-background flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
      </div>
    }>
      <FacebookContent />
    </Suspense>
  );
}
