"use client";

import Link from "next/link";
import { Card, CardContent } from "@/components/ui/card";
import { cn, formatCompactNumber } from "@/lib/utils";
import {
  AreaChart,
  Area,
  ResponsiveContainer,
} from "recharts";

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

const TwitterIcon = ({ className = "w-5 h-5" }: { className?: string }) => (
  <svg className={`fill-current ${className}`} viewBox="0 0 24 24">
    <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/>
  </svg>
);

const platformConfig = {
  youtube: {
    name: "YouTube",
    icon: YouTubeIcon,
    color: "#FF0000",
    href: "/youtube",
    bgClass: "bg-red-500",
    metric1Label: "מנויים",
    metric2Label: "צפיות",
  },
  facebook: {
    name: "Facebook",
    icon: FacebookIcon,
    color: "#1877F2",
    href: "/facebook",
    bgClass: "bg-blue-600",
    metric1Label: "עוקבים",
    metric2Label: "צפיות",
  },
  instagram: {
    name: "Instagram",
    icon: InstagramIcon,
    color: "#E4405F",
    href: "/instagram",
    bgClass: "bg-gradient-to-tr from-yellow-400 via-red-500 to-purple-600",
    metric1Label: "עוקבים",
    metric2Label: "צפיות",
  },
  twitter: {
    name: "X",
    icon: TwitterIcon,
    color: "#1D9BF0",
    href: "/twitter",
    bgClass: "bg-black",
    metric1Label: "עוקבים",
    metric2Label: "צפיות",
  },
};

interface PlatformCardProps {
  platform: "youtube" | "facebook" | "instagram" | "twitter";
  followers: number;
  views: number;
  sparklineData: Array<{ date: string; value: number }>;
}

export function PlatformCard({
  platform,
  followers,
  views,
  sparklineData,
}: PlatformCardProps) {
  const config = platformConfig[platform];
  const Icon = config.icon;

  return (
    <Link href={config.href}>
      <Card
        className="hover:shadow-lg transition-all cursor-pointer border-t-4"
        style={{ borderTopColor: config.color }}
      >
        <CardContent className="p-4">
          <div className="flex items-center gap-2 mb-3">
            <div className={cn("text-white p-1.5 rounded-lg", config.bgClass)}>
              <Icon className="w-4 h-4" />
            </div>
            <span className="font-bold text-sm">{config.name}</span>
          </div>

          {/* Sparkline */}
          <div className="h-10 w-full mb-3">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={sparklineData}>
                <defs>
                  <linearGradient id={`gradient-${platform}`} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={config.color} stopOpacity={0.3} />
                    <stop offset="95%" stopColor={config.color} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <Area
                  type="monotone"
                  dataKey="value"
                  stroke={config.color}
                  strokeWidth={2}
                  fill={`url(#gradient-${platform})`}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          {/* Metrics */}
          <div className="grid grid-cols-2 gap-3 text-center">
            <div>
              <div className="text-lg font-black">
                {formatCompactNumber(followers)}
              </div>
              <div className="text-xs text-muted-foreground">
                {config.metric1Label}
              </div>
            </div>
            <div>
              <div className="text-lg font-black">
                {formatCompactNumber(views)}
              </div>
              <div className="text-xs text-muted-foreground">
                {config.metric2Label}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </Link>
  );
}
