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
}: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);
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
      {/* Toolbar: Search + Export */}
      {(searchable || exportable) && (
        <div className="flex items-center justify-between gap-4 flex-wrap">
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

              return (
                <tr
                  key={index}
                  className={`border-b transition-colors group bg-transparent ${
                    highlight ? highlightColors[highlight] : "hover:bg-muted/50"
                  }`}
                >
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
