"use client";

import { Config } from "@/config";
import { trackAppRouter } from "@socialgouv/matomo-next";
import { usePathname, useSearchParams } from "next/navigation";
import { useEffect } from "react";

export function MatomoAnalytics() {
  const pathname = usePathname();
  const searchParams = useSearchParams();

  useEffect(() => {
    trackAppRouter({
      url: Config.get<string>("analytics.matomoUrl"),
      siteId: Config.get<string>("analytics.matomoSiteId"),
      pathname,
      searchParams,
      disableCookies: true,
    });
  }, [pathname, searchParams]);

  return null;
}
