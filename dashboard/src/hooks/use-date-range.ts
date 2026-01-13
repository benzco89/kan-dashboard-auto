"use client";

import { useSearchParams, useRouter, usePathname } from "next/navigation";
import { useCallback } from "react";

export function useDateRange(defaultValue: string = "7") {
  const searchParams = useSearchParams();
  const router = useRouter();
  const pathname = usePathname();

  const dateRange = searchParams.get("days") || defaultValue;

  const setDateRange = useCallback((value: string) => {
    const params = new URLSearchParams(searchParams.toString());
    params.set("days", value);
    router.replace(`${pathname}?${params.toString()}`, { scroll: false });
  }, [searchParams, router, pathname]);

  return { dateRange, setDateRange };
}
