import { Card, CardContent } from "@/components/ui/card";
import { cn, formatCompactNumber } from "@/lib/utils";
import { TrendingUp, TrendingDown, LucideIcon } from "lucide-react";

interface KPICardProps {
  title: string;
  value: number;
  change?: number; // Absolute change (e.g., +500 followers)
  prevValue?: number; // Previous period value for percentage comparison
  changeLabel?: string;
  icon: LucideIcon;
  iconColor?: string;
}

export function KPICard({
  title,
  value,
  change,
  prevValue,
  changeLabel,
  icon: Icon,
  iconColor = "bg-primary/10 text-primary",
}: KPICardProps) {
  // Calculate percentage change if prevValue is provided
  const percentChange = prevValue && prevValue > 0
    ? ((value - prevValue) / prevValue) * 100
    : null;

  // Determine if positive based on either change or percentChange
  const hasChange = change !== undefined || percentChange !== null;
  const isPositive = change !== undefined
    ? change >= 0
    : (percentChange !== null ? percentChange >= 0 : true);

  return (
    <Card className="hover:shadow-md transition-shadow">
      <CardContent className="p-4">
        <div className="flex items-center gap-2 mb-2">
          <div className={cn("p-1.5 rounded-lg", iconColor)}>
            <Icon className="w-4 h-4" />
          </div>
          <span className="text-xs font-medium text-muted-foreground">
            {title}
          </span>
        </div>
        <div className="text-2xl font-black">
          {formatCompactNumber(value)}
        </div>
        {hasChange && (
          <div
            className={cn(
              "flex items-center gap-1 mt-1 text-xs font-medium",
              isPositive ? "text-green-600" : "text-red-600"
            )}
          >
            {isPositive ? (
              <TrendingUp className="w-3 h-3" />
            ) : (
              <TrendingDown className="w-3 h-3" />
            )}
            <span>
              {change !== undefined ? (
                // Show absolute change
                <>
                  {isPositive ? "+" : ""}
                  {formatCompactNumber(Math.abs(change))}
                  {changeLabel && ` ${changeLabel}`}
                </>
              ) : percentChange !== null ? (
                // Show percentage change
                <>
                  {isPositive ? "+" : ""}
                  {percentChange.toFixed(1)}%
                  <span className="text-muted-foreground mr-1">מהתקופה הקודמת</span>
                </>
              ) : null}
            </span>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
