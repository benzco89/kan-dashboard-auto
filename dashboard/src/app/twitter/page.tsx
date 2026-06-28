"use client";

import { useState, useEffect, Suspense } from "react";
import { Users, Eye, Heart, Repeat2, Loader2 } from "lucide-react";
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

interface TwitterStats {
  followers: number;
  followersChange: number;
  totalViews: number;
  totalLikes: number;
  totalRetweets: number;
  totalReplies: number;
  avgEngagementRate: number;
  postCount: number;
  videoCount: number;
  photoCount: number;
  textCount: number;
}

interface TwitterData {
  stats: TwitterStats;
  prevStats: TwitterStats | null;
  performanceData: Array<{ date: string; views: number; engagement: number }>;
  prevPerformanceData: Array<{ date: string; views: number; engagement: number }>;
  postTypeBreakdown: Array<{
    name: string;
    value: number;
    count: number;
    percentage: number;
    color: string;
  }>;
  posts: Array<{
    id: string;
    text: string;
    date: string;
    type: string;
    views: number;
    likes: number;
    retweets: number;
    replies: number;
    engagementRate: number;
    url: string;
  }>;
  lastUpdated: string;
}

function TwitterContent() {
  const { dateRange, setDateRange } = useDateRange("7");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<TwitterData | null>(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`/api/twitter?days=${dateRange}`);
      const result = await response.json();
      if (result.success) {
        setData(result.data);
      } else {
        setError(result.error || "Failed to fetch data");
      }
    } catch (err) {
      setError("Failed to connect to the server");
      console.error("Error fetching Twitter data:", err);
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
          <div className="w-10 h-10 rounded-lg bg-black flex items-center justify-center">
            <svg className="w-5 h-5 fill-white" viewBox="0 0 24 24">
              <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z" />
            </svg>
          </div>
          <div>
            <h1 className="text-xl font-bold">X (Twitter) Analytics</h1>
            <p className="text-sm text-muted-foreground">
              ניתוח ביצועי חשבון X של כאן חדשות
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

        {/* Empty State - sheet not populated yet */}
        {data && data.stats.postCount === 0 && !loading && (
          <Card>
            <CardContent className="p-6">
              <p className="font-bold">אין עדיין נתוני טוויטר</p>
              <p className="text-sm mt-1 text-muted-foreground">
                הגיליון &quot;נתוני טוויטר&quot; יתמלא אחרי ההרצה הראשונה של twitter_collector.py.
              </p>
            </CardContent>
          </Card>
        )}

        {/* Main Content */}
        {data && data.stats.postCount > 0 && (
          <>
            {/* KPI Cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <KPICard
                title="עוקבים"
                value={data.stats.followers}
                change={data.stats.followersChange}
                icon={Users}
                iconColor="bg-sky-500/10 text-sky-600"
              />
              <KPICard
                title="צפיות"
                value={data.stats.totalViews}
                prevValue={data.prevStats?.totalViews}
                icon={Eye}
                iconColor="bg-sky-500/10 text-sky-600"
              />
              <KPICard
                title="לייקים"
                value={data.stats.totalLikes}
                prevValue={data.prevStats?.totalLikes}
                icon={Heart}
                iconColor="bg-sky-500/10 text-sky-600"
              />
              <KPICard
                title="ריטוויטים"
                value={data.stats.totalRetweets}
                prevValue={data.prevStats?.totalRetweets}
                icon={Repeat2}
                iconColor="bg-sky-500/10 text-sky-600"
              />
            </div>

            {/* Charts Row */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              {/* Views Over Time - Takes 2 columns */}
              <Card className="lg:col-span-2">
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">צפיות לאורך זמן</CardTitle>
                  <CardDescription className="text-xs">מדדים יומיים</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={formattedPerformanceData}>
                        <defs>
                          <linearGradient id="colorTwViews" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#1D9BF0" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="#1D9BF0" stopOpacity={0} />
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
                            const engEntry = payload.find(p => p.dataKey === 'engagement');
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
                                {engEntry && (
                                  <p className="text-sm">
                                    <span style={{ color: engEntry.color }}>מעורבות:</span>{" "}
                                    <span className="font-bold">{formatCompactNumber(engEntry.value as number)}</span>
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
                                <span className="w-3 h-0.5 bg-[#1D9BF0]"></span>
                                צפיות
                              </span>
                              <span className="flex items-center gap-1">
                                <span className="w-3 h-0.5 bg-[#794BC4]"></span>
                                מעורבות
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
                          stroke="#1D9BF0"
                          strokeWidth={2}
                          fill="url(#colorTwViews)"
                        />
                        <Area
                          type="monotone"
                          dataKey="engagement"
                          name="מעורבות"
                          stroke="#794BC4"
                          strokeWidth={2}
                          fillOpacity={0}
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

              {/* Tweet Type Breakdown - Pie Chart */}
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">סוג תוכן</CardTitle>
                  <CardDescription className="text-xs">וידאו / תמונה / טקסט</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Pie
                          data={data.postTypeBreakdown}
                          cx="50%"
                          cy="50%"
                          innerRadius={50}
                          outerRadius={80}
                          paddingAngle={5}
                          dataKey="value"
                        >
                          {data.postTypeBreakdown.map((entry, index) => (
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
                                <p className="text-sm text-muted-foreground">{entry.count} ציוצים</p>
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

            {/* Tweet Stats Summary */}
            <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.postCount}</div>
                  <div className="text-xs text-muted-foreground">סה"כ ציוצים</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.videoCount}</div>
                  <div className="text-xs text-muted-foreground">וידאו</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.photoCount}</div>
                  <div className="text-xs text-muted-foreground">תמונה</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.textCount}</div>
                  <div className="text-xs text-muted-foreground">טקסט</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.avgEngagementRate.toFixed(2)}%</div>
                  <div className="text-xs text-muted-foreground">ממוצע מעורבות</div>
                </CardContent>
              </Card>
            </div>

            {/* Tweets Table */}
            <Card>
              <CardHeader>
                <CardTitle className="text-base">ציוצים מובילים</CardTitle>
                <CardDescription className="text-xs">
                  {data.stats.postCount} ציוצים ב-{dateRange} הימים האחרונים
                </CardDescription>
              </CardHeader>
              <CardContent>
                <DataTable
                  data={data.posts}
                  searchable={true}
                  searchKeys={["text"]}
                  searchPlaceholder="חיפוש לפי טקסט..."
                  exportable={true}
                  exportFileName="twitter_posts"
                  showRank={true}
                  rankToggle={true}
                  rankKey="views"
                  defaultSortKey="views"
                  defaultSortDirection="desc"
                  columns={[
                    {
                      key: "text",
                      header: "תוכן",
                      sortable: true,
                      render: (post) => (
                        <div className="flex-1 min-w-0">
                          <div className="max-w-xs truncate font-medium">{post.text}</div>
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
                          post.type === 'Video'
                            ? 'bg-sky-100 text-sky-700 dark:bg-sky-900 dark:text-sky-300'
                            : post.type === 'Photo'
                            ? 'bg-purple-100 text-purple-700 dark:bg-purple-900 dark:text-purple-300'
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
                      render: (post) => (
                        <span className="font-medium">{formatCompactNumber(post.views)}</span>
                      ),
                    },
                    {
                      key: "likes",
                      header: "לייקים",
                      sortable: true,
                      align: "center",
                      render: (post) => formatCompactNumber(post.likes),
                    },
                    {
                      key: "retweets",
                      header: "ריטוויטים",
                      sortable: true,
                      align: "center",
                      render: (post) => formatCompactNumber(post.retweets),
                    },
                    {
                      key: "replies",
                      header: "תגובות",
                      sortable: true,
                      align: "center",
                      render: (post) => formatCompactNumber(post.replies),
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
                    if (post.views > avgViews * 2) return "views";
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

export default function TwitterPage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-background flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
      </div>
    }>
      <TwitterContent />
    </Suspense>
  );
}
