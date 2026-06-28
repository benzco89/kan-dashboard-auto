"use client";

import { useState } from "react";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { formatCompactNumber } from "@/lib/utils";

interface PerformanceData {
  date: string;
  youtube: number;
  facebook: number;
  instagram: number;
  total?: number;
}

interface PerformanceChartProps {
  data: PerformanceData[];
  title?: string;
  description?: string;
}

const PLATFORMS = [
  { key: "youtube", name: "YouTube", color: "#FF0000" },
  { key: "facebook", name: "Facebook", color: "#1877F2" },
  { key: "instagram", name: "Instagram", color: "#E4405F" },
] as const;

export function PerformanceChart({
  data,
  title = "ביצועים לאורך זמן",
  description = "צפיות יומיות לפי פלטפורמה",
}: PerformanceChartProps) {
  const [visiblePlatforms, setVisiblePlatforms] = useState<Set<string>>(
    new Set(["youtube", "facebook", "instagram"])
  );
  const [showTotal, setShowTotal] = useState(false);

  const togglePlatform = (platform: string) => {
    const newVisible = new Set(visiblePlatforms);
    if (newVisible.has(platform)) {
      // Don't allow hiding all platforms
      if (newVisible.size > 1) {
        newVisible.delete(platform);
      }
    } else {
      newVisible.add(platform);
    }
    setVisiblePlatforms(newVisible);
  };

  const formattedData = data.map((d) => ({
    ...d,
    date: new Date(d.date).toLocaleDateString("he-IL", {
      day: "2-digit",
      month: "2-digit",
    }),
    total: d.youtube + d.facebook + d.instagram,
  }));

  // Calculate totals for summary
  const totals = data.reduce(
    (acc, d) => ({
      youtube: acc.youtube + d.youtube,
      facebook: acc.facebook + d.facebook,
      instagram: acc.instagram + d.instagram,
    }),
    { youtube: 0, facebook: 0, instagram: 0 }
  );
  const grandTotal = totals.youtube + totals.facebook + totals.instagram;

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
          <div>
            <CardTitle className="text-base lg:text-lg">{title}</CardTitle>
            <CardDescription className="text-xs">{description}</CardDescription>
          </div>

          {/* Platform Toggles */}
          <div className="flex items-center gap-1 flex-wrap">
            {PLATFORMS.map((platform) => (
              <button
                key={platform.key}
                onClick={() => togglePlatform(platform.key)}
                className={`px-2 py-1 rounded-md text-xs font-medium transition-all border ${
                  visiblePlatforms.has(platform.key)
                    ? "border-transparent"
                    : "border-gray-300 dark:border-gray-600 opacity-50"
                }`}
                style={{
                  backgroundColor: visiblePlatforms.has(platform.key)
                    ? `${platform.color}20`
                    : "transparent",
                  color: visiblePlatforms.has(platform.key)
                    ? platform.color
                    : undefined,
                }}
              >
                {platform.name}
              </button>
            ))}
            <button
              onClick={() => setShowTotal(!showTotal)}
              className={`px-2 py-1 rounded-md text-xs font-medium transition-all border ${
                showTotal
                  ? "bg-primary/10 text-primary border-transparent"
                  : "border-gray-300 dark:border-gray-600 opacity-50"
              }`}
            >
              סה״כ
            </button>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {/* Summary Stats */}
        <div className="grid grid-cols-4 gap-2 mb-4">
          {PLATFORMS.map((platform) => (
            <div
              key={platform.key}
              className={`text-center p-2 rounded-lg transition-opacity ${
                visiblePlatforms.has(platform.key) ? "opacity-100" : "opacity-40"
              }`}
              style={{ backgroundColor: `${platform.color}10` }}
            >
              <div
                className="text-lg font-bold"
                style={{ color: platform.color }}
              >
                {formatCompactNumber(totals[platform.key as keyof typeof totals])}
              </div>
              <div className="text-xs text-muted-foreground">{platform.name}</div>
            </div>
          ))}
          <div className="text-center p-2 rounded-lg bg-muted/50">
            <div className="text-lg font-bold text-primary">
              {formatCompactNumber(grandTotal)}
            </div>
            <div className="text-xs text-muted-foreground">סה״כ</div>
          </div>
        </div>

        {/* Chart */}
        <div className="h-64 lg:h-72">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={formattedData}>
              <defs>
                {PLATFORMS.map((platform) => (
                  <linearGradient
                    key={platform.key}
                    id={`color${platform.key}`}
                    x1="0"
                    y1="0"
                    x2="0"
                    y2="1"
                  >
                    <stop offset="5%" stopColor={platform.color} stopOpacity={0.3} />
                    <stop offset="95%" stopColor={platform.color} stopOpacity={0} />
                  </linearGradient>
                ))}
                <linearGradient id="colorTotal" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#F7381B" stopOpacity={0.2} />
                  <stop offset="95%" stopColor="#F7381B" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 11 }}
                tickLine={false}
                axisLine={false}
                className="text-muted-foreground"
              />
              <YAxis
                tick={{ fontSize: 11 }}
                tickLine={false}
                axisLine={false}
                tickFormatter={(value) => formatCompactNumber(value)}
                className="text-muted-foreground"
              />
              <Tooltip
                content={({ active, payload, label }) => {
                  if (!active || !payload) return null;
                  const totalValue = payload.reduce(
                    (sum, entry) => sum + (entry.dataKey !== "total" ? (entry.value as number) : 0),
                    0
                  );
                  return (
                    <div className="rounded-lg border bg-white dark:bg-gray-800 p-3 shadow-lg">
                      <p className="font-bold text-sm mb-2 border-b pb-2">{label}</p>
                      {payload
                        .filter((entry) => entry.dataKey !== "total")
                        .map((entry: any, index: number) => (
                          <div
                            key={index}
                            className="flex items-center justify-between gap-4 py-1"
                          >
                            <div className="flex items-center gap-2">
                              <div
                                className="w-3 h-3 rounded-full"
                                style={{ backgroundColor: entry.color }}
                              />
                              <span className="text-sm text-muted-foreground">
                                {entry.name}
                              </span>
                            </div>
                            <span className="font-bold text-sm">
                              {formatCompactNumber(entry.value)}
                            </span>
                          </div>
                        ))}
                      <div className="flex items-center justify-between gap-4 pt-2 mt-2 border-t">
                        <span className="text-sm font-medium">סה״כ</span>
                        <span className="font-bold text-sm text-primary">
                          {formatCompactNumber(totalValue)}
                        </span>
                      </div>
                    </div>
                  );
                }}
              />
              {/* Total Area (shown first so it's behind) */}
              {showTotal && (
                <Area
                  type="monotone"
                  dataKey="total"
                  name="סה״כ"
                  stroke="#F7381B"
                  strokeWidth={2}
                  strokeDasharray="5 5"
                  fill="url(#colorTotal)"
                />
              )}
              {/* Platform Areas */}
              {PLATFORMS.map(
                (platform) =>
                  visiblePlatforms.has(platform.key) && (
                    <Area
                      key={platform.key}
                      type="monotone"
                      dataKey={platform.key}
                      name={platform.name}
                      stroke={platform.color}
                      strokeWidth={2}
                      fill={`url(#color${platform.key})`}
                    />
                  )
              )}
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
