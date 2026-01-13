"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import { Moon, Sun, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { cn } from "@/lib/utils";

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

const navItems = [
  { href: "/", label: "סקירה כללית", icon: null },
  { href: "/youtube", label: "YouTube", icon: YouTubeIcon, color: "text-youtube" },
  { href: "/facebook", label: "Facebook", icon: FacebookIcon, color: "text-facebook" },
  { href: "/instagram", label: "Instagram", icon: InstagramIcon, color: "text-instagram" },
];

const dateRanges = [
  { value: "7", label: "7 ימים", display: "7 ימים" },
  { value: "14", label: "14 ימים", display: "14 ימים" },
  { value: "30", label: "30 ימים", display: "30 ימים" },
  { value: "90", label: "90 ימים", display: "90 ימים" },
];

interface HeaderProps {
  dateRange: string;
  onDateRangeChange: (value: string) => void;
  onRefresh?: () => void;
  isLoading?: boolean;
}

export function Header({
  dateRange,
  onDateRangeChange,
  onRefresh,
  isLoading = false
}: HeaderProps) {
  const pathname = usePathname();
  const { theme, setTheme } = useTheme();

  return (
    <header className="sticky top-0 z-50 border-b border-t-[6px] border-t-kan header-bg shadow-sm">
      <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8 h-14 flex items-center justify-between gap-4">
        {/* Logo */}
        <Link href="/" className="flex items-center gap-1.5 shrink-0">
          <span className="text-xl font-black tracking-tighter">כאן</span>
          <div className="bg-primary w-7 h-7 rounded-md flex items-center justify-center text-white font-bold text-base shadow transform -skew-x-6">
            <span>|&lt;</span>
          </div>
          <span className="text-xl font-black tracking-tighter">חדשות</span>
        </Link>

        {/* Navigation */}
        <nav className="hidden md:flex items-center gap-1 nav-container rounded-lg p-1">
          {navItems.map((item) => {
            const isActive = pathname === item.href;
            return (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  "px-3 py-1.5 rounded-md text-sm font-medium transition-all flex items-center gap-1.5 border-b-2",
                  isActive
                    ? "nav-item-active shadow-sm text-kan border-b-kan"
                    : "text-muted-foreground hover:text-foreground border-transparent"
                )}
              >
                {item.icon && (
                  <item.icon className={cn("w-4 h-4", isActive && item.color)} />
                )}
                {item.label}
              </Link>
            );
          })}
        </nav>

        {/* Actions */}
        <div className="flex items-center gap-2 shrink-0">
          {/* Theme Toggle */}
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
          >
            <Sun className="h-4 w-4 rotate-0 scale-100 transition-all dark:-rotate-90 dark:scale-0" />
            <Moon className="absolute h-4 w-4 rotate-90 scale-0 transition-all dark:rotate-0 dark:scale-100" />
            <span className="sr-only">Toggle theme</span>
          </Button>

          {/* Refresh */}
          {onRefresh && (
            <Button
              variant="ghost"
              size="icon"
              onClick={onRefresh}
              disabled={isLoading}
              title="רענן נתונים"
            >
              <RefreshCw className={cn("h-4 w-4", isLoading && "animate-spin")} />
            </Button>
          )}

          {/* Date Range */}
          <Select value={dateRange} onValueChange={onDateRangeChange} dir="rtl">
            <SelectTrigger className="w-[110px] text-sm select-trigger-bg">
              <SelectValue placeholder="בחר טווח">
                {dateRanges.find(r => r.value === dateRange)?.label}
              </SelectValue>
            </SelectTrigger>
            <SelectContent className="select-content-bg">
              {dateRanges.map((range) => (
                <SelectItem key={range.value} value={range.value}>
                  <span dir="rtl">{range.label}</span>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
    </header>
  );
}
