"use client";

import { useState, useEffect, Suspense } from "react";
import { Users, Eye, Bookmark, Share2, Loader2, Target } from "lucide-react";
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
  BarChart,
  Bar,
} from "recharts";

interface InstagramStats {
  followers: number;
  followersChange: number;
  totalViews: number;
  totalReach: number;
  totalSaved: number;
  totalShares: number;
  avgEngagementRate: number;
  postCount: number;
  reelCount: number;
  photoCount: number;
  carouselCount: number;
}

interface InstagramData {
  stats: InstagramStats;
  prevStats: InstagramStats | null;
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
    count: number;
    percentage: number;
    color: string;
  }>;
  engagementComparison: Array<{
    date: string;
    saved: number;
    shares: number;
  }>;
  posts: Array<{
    id: string;
    caption: string;
    date: string;
    type: string;
    views: number;
    reach: number;
    saved: number;
    shares: number;
    engagementRate: number;
    url: string;
  }>;
  lastUpdated: string;
}

function InstagramContent() {
  const { dateRange, setDateRange } = useDateRange("7");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<InstagramData | null>(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`/api/instagram?days=${dateRange}`);
      const result = await response.json();
      if (result.success) {
        setData(result.data);
      } else {
        setError(result.error || "Failed to fetch data");
      }
    } catch (err) {
      setError("Failed to connect to the server");
      console.error("Error fetching Instagram data:", err);
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

  const formattedEngagementData = (data?.engagementComparison || []).map((d) => ({
    ...d,
    date: new Date(d.date).toLocaleDateString("he-IL", {
      day: "2-digit",
      month: "2-digit",
    }),
  }));

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
          <div className="w-10 h-10 rounded-lg bg-gradient-to-tr from-yellow-400 via-red-500 to-purple-600 flex items-center justify-center">
            <svg className="w-6 h-6 fill-white" viewBox="0 0 24 24">
              <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zm0-2.163c-3.259 0-3.667.014-4.947.072-4.358.2-6.78 2.618-6.98 6.98-.059 1.281-.073 1.689-.073 4.948 0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98 1.281.058 1.689.072 4.948.072 3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98-1.281-.059-1.69-.073-4.949-.073zm0 5.838c-3.403 0-6.162 2.759-6.162 6.162s2.759 6.163 6.162 6.163 6.162-2.759 6.162-6.163c0-3.403-2.759-6.162-6.162-6.162zm0 10.162c-2.209 0-4-1.79-4-4 0-2.209 1.791-4 4-4s4 1.791 4 4c0 2.21-1.791 4-4 4zm6.406-11.845c-.796 0-1.441.645-1.441 1.44s.645 1.44 1.441 1.44c.795 0 1.439-.645 1.439-1.44s-.644-1.44-1.439-1.44z"/>
            </svg>
          </div>
          <div>
            <h1 className="text-xl font-bold">Instagram Analytics</h1>
            <p className="text-sm text-muted-foreground">
              ניתוח ביצועי עמוד Instagram של כאן חדשות
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
                iconColor="bg-pink-500/10 text-pink-600"
              />
              <KPICard
                title="צפיות"
                value={data.stats.totalViews}
                prevValue={data.prevStats?.totalViews}
                icon={Eye}
                iconColor="bg-pink-500/10 text-pink-600"
              />
              <KPICard
                title="שמירות"
                value={data.stats.totalSaved}
                prevValue={data.prevStats?.totalSaved}
                icon={Bookmark}
                iconColor="bg-pink-500/10 text-pink-600"
              />
              <KPICard
                title="שיתופים"
                value={data.stats.totalShares}
                prevValue={data.prevStats?.totalShares}
                icon={Share2}
                iconColor="bg-pink-500/10 text-pink-600"
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
                          <linearGradient id="colorIgViews" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#E4405F" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="#E4405F" stopOpacity={0} />
                          </linearGradient>
                          <linearGradient id="colorIgReach" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#833AB4" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="#833AB4" stopOpacity={0} />
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
                                <span className="w-3 h-0.5 bg-[#E4405F]"></span>
                                צפיות
                              </span>
                              <span className="flex items-center gap-1">
                                <span className="w-3 h-0.5 bg-[#833AB4]"></span>
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
                          stroke="#E4405F"
                          strokeWidth={2}
                          fill="url(#colorIgViews)"
                        />
                        <Area
                          type="monotone"
                          dataKey="reach"
                          name="חשיפה"
                          stroke="#833AB4"
                          strokeWidth={2}
                          fill="url(#colorIgReach)"
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

              {/* Post Type Breakdown - Pie Chart */}
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">סוג תוכן</CardTitle>
                  <CardDescription className="text-xs">Reel / Photo / Carousel</CardDescription>
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
                                <p className="text-sm text-muted-foreground">{entry.count} פוסטים</p>
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

            {/* Saves vs Shares Chart */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">שמירות לעומת שיתופים</CardTitle>
                <CardDescription className="text-xs">השוואה יומית</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-48">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={formattedEngagementData}>
                      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" vertical={false} />
                      <XAxis
                        dataKey="date"
                        tick={{ fontSize: 10 }}
                        tickLine={false}
                        axisLine={false}
                      />
                      <YAxis
                        tick={{ fontSize: 10 }}
                        tickLine={false}
                        axisLine={false}
                        tickFormatter={(value) => formatCompactNumber(value)}
                      />
                      <Tooltip
                        content={({ active, payload, label }) => {
                          if (!active || !payload) return null;
                          return (
                            <div className="rounded-lg border bg-white dark:bg-gray-800 p-3 shadow-lg">
                              <p className="font-bold text-sm mb-2">{label}</p>
                              {payload.map((entry: any, index: number) => (
                                <p key={index} className="text-sm">
                                  <span style={{ color: entry.color }}>{entry.name}:</span>{" "}
                                  <span className="font-bold">{formatCompactNumber(entry.value)}</span>
                                </p>
                              ))}
                            </div>
                          );
                        }}
                      />
                      <Legend
                        verticalAlign="top"
                        height={36}
                        formatter={(value: string) => (
                          <span className="text-sm">{value}</span>
                        )}
                      />
                      <Bar dataKey="saved" name="שמירות" fill="#E4405F" radius={[4, 4, 0, 0]} />
                      <Bar dataKey="shares" name="שיתופים" fill="#833AB4" radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>

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
                  <div className="text-2xl font-bold">{data.stats.reelCount}</div>
                  <div className="text-xs text-muted-foreground">Reels</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.photoCount}</div>
                  <div className="text-xs text-muted-foreground">Photos</div>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4 text-center">
                  <div className="text-2xl font-bold">{data.stats.carouselCount}</div>
                  <div className="text-xs text-muted-foreground">Carousels</div>
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
                  searchKeys={["caption"]}
                  searchPlaceholder="חיפוש לפי תיאור..."
                  exportable={true}
                  exportFileName="instagram_posts"
                  showRank={true}
                  rankToggle={true}
                  rankKey="engagementRate"
                  defaultSortKey="engagementRate"
                  defaultSortDirection="desc"
                  columns={[
                    {
                      key: "caption",
                      header: "תיאור",
                      sortable: true,
                      render: (post) => (
                        <div className="flex-1 min-w-0">
                          <div className="max-w-xs truncate font-medium">{post.caption}</div>
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
                          post.type === 'Reel'
                            ? 'bg-pink-100 text-pink-700 dark:bg-pink-900 dark:text-pink-300'
                            : post.type === 'Photo'
                            ? 'bg-orange-100 text-orange-700 dark:bg-orange-900 dark:text-orange-300'
                            : 'bg-purple-100 text-purple-700 dark:bg-purple-900 dark:text-purple-300'
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
                      key: "reach",
                      header: "חשיפה",
                      sortable: true,
                      align: "center",
                      render: (post) => formatCompactNumber(post.reach),
                    },
                    {
                      key: "saved",
                      header: "שמירות",
                      sortable: true,
                      align: "center",
                      render: (post) => formatCompactNumber(post.saved),
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

export default function InstagramPage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-background flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
      </div>
    }>
      <InstagramContent />
    </Suspense>
  );
}
