import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

// Format number with Hebrew locale
export function formatNumber(num: number | undefined | null): string {
  if (num === undefined || num === null) return "0";
  return num.toLocaleString("he-IL");
}

// Format large numbers (K, M)
export function formatCompactNumber(value: number): string {
  if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
  if (value >= 1000) return `${(value / 1000).toFixed(0)}K`;
  return value.toString();
}

// Format relative time in Hebrew
export function formatRelativeTime(dateString: string): string {
  const date = new Date(dateString);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);

  if (diffMins < 1) return "עכשיו";
  if (diffMins < 60) return `לפני ${diffMins} דקות`;
  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `לפני ${diffHours} שעות`;
  return date.toLocaleDateString("he-IL");
}

// Platform colors
export const PLATFORM_COLORS = {
  youtube: "#FF0000",
  facebook: "#1877F2",
  instagram: "#E4405F",
  primary: "#F7381B",
} as const;

// Chart colors for Recharts
export const CHART_COLORS = {
  youtube: "hsl(0, 100%, 50%)",
  facebook: "hsl(214, 89%, 52%)",
  instagram: "hsl(340, 75%, 54%)",
  primary: "hsl(9, 94%, 54%)",
} as const;
