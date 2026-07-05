"use client";

import { useState, useMemo } from "react";
import { ChevronUp, ChevronDown, ChevronLeft, ChevronRight, ExternalLink, Search, X, Download } from "lucide-react";
import { Button } from "./button";
import { formatCompactNumber } from "@/lib/utils";

type SortDirection = "asc" | "desc" | null;

interface Column<T> {
  key: keyof T | string;
  header: string;
  sortable?: boolean;
  align?: "left" | "center" | "right";
  render?: (item: T) => React.ReactNode;
  getValue?: (item: T) => number | string;
}

interface DataTableProps<T> {
  data: T[];
  columns: Column<T>[];
  pageSize?: number;
  getRowHighlight?: (item: T) => "views" | "engagement" | "reach" | null;
  getRowUrl?: (item: T) => string | undefined;
  searchable?: boolean;
  searchKeys?: (keyof T)[];
  searchPlaceholder?: string;
  exportable?: boolean;
  exportFileName?: string;
  /** Show a leading "#" rank column reflecting the current sort order */
  showRank?: boolean;
  /** Column key the table is sorted by on first render */
  defaultSortKey?: string;
  /** Direction the table is sorted by on first render (defaults to "desc") */
  defaultSortDirection?: SortDirection;
  /** Show a best/worst toggle that ranks by `rankKey` */
  rankToggle?: boolean;
  /** Column key the best/worst toggle sorts by */
  rankKey?: string;
  /** Label for the "best first" toggle option */
  rankDescLabel?: string;
  /** Label for the "worst first" toggle option */
  rankAscLabel?: string;
}

const highlightColors = {
  views: "highlight-views",
  engagement: "highlight-engagement",
  reach: "highlight-reach",
};

export function DataTable<T extends Record<string, any>>({
  data,
  columns,
  pageSize = 15,
  getRowHighlight,
  getRowUrl,
  searchable = false,
  searchKeys = [],
  searchPlaceholder = "חיפוש...",
  exportable = false,
  exportFileName = "data",
  showRank = false,
  defaultSortKey,
  defaultSortDirection,
  rankToggle = false,
  rankKey,
  rankDescLabel = "מצליחים ביותר",
  rankAscLabel = "פחות מצליחים",
}: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<string | null>(defaultSortKey ?? null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(
    defaultSortKey ? defaultSortDirection ?? "desc" : null
  );
  const [currentPage, setCurrentPage] = useState(1);
  const [searchQuery, setSearchQuery] = useState("");

  // Filter data based on search
  const filteredData = useMemo(() => {
    if (!searchQuery.trim() || searchKeys.length === 0) return data;

    const query = searchQuery.toLowerCase().trim();
    return data.filter(item => {
      return searchKeys.some(key => {
        const value = item[key];
        if (value === null || value === undefined) return false;
        return String(value).toLowerCase().includes(query);
      });
    });
  }, [data, searchQuery, searchKeys]);

  const sortedData = useMemo(() => {
    if (!sortKey || !sortDirection) return filteredData;

    const column = columns.find((c) => c.key === sortKey);

    return [...filteredData].sort((a, b) => {
      let aVal: any;
      let bVal: any;

      if (column?.getValue) {
        aVal = column.getValue(a);
        bVal = column.getValue(b);
      } else {
        aVal = a[sortKey];
        bVal = b[sortKey];
      }

      // Handle numbers
      if (typeof aVal === "number" && typeof bVal === "number") {
        return sortDirection === "asc" ? aVal - bVal : bVal - aVal;
      }

      // Handle strings
      const aStr = String(aVal || "");
      const bStr = String(bVal || "");
      return sortDirection === "asc"
        ? aStr.localeCompare(bStr)
        : bStr.localeCompare(aStr);
    });
  }, [filteredData, sortKey, sortDirection, columns]);

  const totalPages = Math.ceil(sortedData.length / pageSize);
  const startIndex = (currentPage - 1) * pageSize;
  const paginatedData = sortedData.slice(startIndex, startIndex + pageSize);

  // Reset to first page when search changes
  const handleSearch = (value: string) => {
    setSearchQuery(value);
    setCurrentPage(1);
  };

  // Export to CSV
  const handleExport = () => {
    // Create headers
    const headers = columns.map(col => col.header);

    // Create data rows
    const rows = sortedData.map(item => {
      return columns.map(col => {
        let value: string | number;
        if (col.getValue) {
          value = col.getValue(item);
        } else {
          const rawValue = item[col.key as keyof T];
          value = rawValue !== null && rawValue !== undefined ? String(rawValue) : '';
        }
        // Handle special characters and commas in CSV
        if (typeof value === 'string') {
          // Escape quotes and wrap in quotes if contains comma, quote, or newline
          if (value.includes(',') || value.includes('"') || value.includes('\n')) {
            value = `"${value.replace(/"/g, '""')}"`;
          }
        }
        return value ?? '';
      });
    });

    // Create CSV content with BOM for Hebrew support
    const BOM = '\uFEFF';
    const csvContent = BOM + [headers.join(','), ...rows.map(row => row.join(','))].join('\n');

    // Create and trigger download
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${exportFileName}_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  // Best/worst ranking toggle: sort by the success metric in a given direction
  const setRanking = (dir: "desc" | "asc") => {
    if (!rankKey) return;
    setSortKey(rankKey);
    setSortDirection(dir);
    setCurrentPage(1);
  };
  const rankActive = rankKey && sortKey === rankKey ? sortDirection : null;

  const handleSort = (key: string) => {
    if (sortKey === key) {
      if (sortDirection === "desc") {
        setSortDirection("asc");
      } else if (sortDirection === "asc") {
        setSortDirection(null);
        setSortKey(null);
      } else {
        setSortDirection("desc");
      }
    } else {
      setSortKey(key);
      setSortDirection("desc");
    }
    setCurrentPage(1);
  };

  const renderSortIcon = (key: string) => {
    if (sortKey !== key) {
      return <span className="w-4 h-4 opacity-30">⇅</span>;
    }
    if (sortDirection === "desc") {
      return <ChevronDown className="w-4 h-4" />;
    }
    if (sortDirection === "asc") {
      return <ChevronUp className="w-4 h-4" />;
    }
    return <span className="w-4 h-4 opacity-30">⇅</span>;
  };

  return (
    <div className="space-y-4">
      {/* Toolbar: Search + Ranking toggle + Export */}
      {(searchable || exportable || (rankToggle && rankKey)) && (
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <div className="flex items-center gap-3 flex-wrap">
            {/* Search Input */}
            {searchable && (
              <div className="relative">
                <Search className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => handleSearch(e.target.value)}
                  placeholder={searchPlaceholder}
                  className="w-full sm:w-72 h-9 pr-10 pl-8 text-sm border rounded-md bg-background focus:outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary"
                />
                {searchQuery && (
                  <button
                    onClick={() => handleSearch("")}
                    className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                  >
                    <X className="w-4 h-4" />
                  </button>
                )}
              </div>
            )}

            {/* Best / worst ranking toggle */}
            {rankToggle && rankKey && (
              <div className="inline-flex h-9 rounded-md border overflow-hidden text-sm shrink-0">
                <button
                  onClick={() => setRanking("desc")}
                  className={`px-3 transition-colors ${
                    rankActive === "desc"
                      ? "bg-primary text-white"
                      : "bg-background hover:bg-muted text-muted-foreground"
                  }`}
                >
                  {rankDescLabel}
                </button>
                <button
                  onClick={() => setRanking("asc")}
                  className={`px-3 border-r transition-colors ${
                    rankActive === "asc"
                      ? "bg-primary text-white"
                      : "bg-background hover:bg-muted text-muted-foreground"
                  }`}
                >
                  {rankAscLabel}
                </button>
              </div>
            )}
          </div>

          {/* Export Button */}
          {exportable && (
            <Button
              variant="outline"
              size="sm"
              onClick={handleExport}
              className="gap-2"
            >
              <Download className="w-4 h-4" />
              <span>ייצוא CSV</span>
            </Button>
          )}
        </div>
      )}

      <div className="overflow-x-auto">
        <table className="w-full text-sm text-foreground">
          <thead>
            <tr className="border-b">
              {showRank && (
                <th className="py-3 px-2 w-10 font-medium text-muted-foreground text-center select-none">
                  #
                </th>
              )}
              {columns.map((column) => (
                <th
                  key={String(column.key)}
                  className={`py-3 px-2 font-medium text-muted-foreground ${
                    column.align === "center"
                      ? "text-center"
                      : column.align === "left"
                      ? "text-left"
                      : "text-right"
                  } ${column.sortable ? "cursor-pointer hover:text-foreground select-none" : ""}`}
                  onClick={() => column.sortable && handleSort(String(column.key))}
                >
                  <div className={`flex items-center gap-1 ${
                    column.align === "center" ? "justify-center" :
                    column.align === "left" ? "justify-start" : "justify-end"
                  }`}>
                    <span>{column.header}</span>
                    {column.sortable && renderSortIcon(String(column.key))}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {paginatedData.map((item, index) => {
              const highlight = getRowHighlight?.(item);
              const url = getRowUrl?.(item);
              const rank = startIndex + index + 1;

              return (
                <tr
                  key={index}
                  className={`border-b transition-colors group bg-transparent ${
                    highlight ? highlightColors[highlight] : "hover:bg-muted/50"
                  }`}
                >
                  {showRank && (
                    <td className="py-3 px-2 text-center">
                      <span
                        className={`tabular-nums text-sm font-semibold ${
                          rank <= 3 ? "text-primary" : "text-muted-foreground"
                        }`}
                      >
                        {rank}
                      </span>
                    </td>
                  )}
                  {columns.map((column, colIndex) => (
                    <td
                      key={String(column.key)}
                      className={`py-3 px-2 ${
                        column.align === "center"
                          ? "text-center"
                          : column.align === "left"
                          ? "text-left"
                          : "text-right"
                      }`}
                    >
                      {colIndex === 0 && url ? (
                        <a
                          href={url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="flex items-center gap-2 hover:text-primary"
                        >
                          <div className="flex-1 min-w-0">
                            {column.render ? column.render(item) : item[column.key]}
                          </div>
                          <ExternalLink className="w-4 h-4 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity shrink-0" />
                        </a>
                      ) : column.render ? (
                        column.render(item)
                      ) : (
                        item[column.key]
                      )}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            מציג {startIndex + 1}-{Math.min(startIndex + pageSize, sortedData.length)} מתוך {sortedData.length}
          </p>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
              disabled={currentPage === 1}
            >
              <ChevronRight className="w-4 h-4" />
            </Button>
            <span className="text-sm">
              עמוד {currentPage} מתוך {totalPages}
            </span>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
              disabled={currentPage === totalPages}
            >
              <ChevronLeft className="w-4 h-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
