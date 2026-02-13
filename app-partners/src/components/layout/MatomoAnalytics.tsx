"use client";

import { Config } from "@/config";
import { useAuth } from "@/providers/AuthProvider";
import { push, trackAppRouter } from "@socialgouv/matomo-next";
import { usePathname, useSearchParams } from "next/navigation";
import { useEffect, useRef } from "react";

export function MatomoAnalytics() {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { user, isAuth } = useAuth();
  const lastUserIdRef = useRef<string | null>(null);

  useEffect(() => {
    const analyticsId = isAuth && user?.analytics_id ? user.analytics_id : null;
    if (analyticsId === lastUserIdRef.current) return;
    lastUserIdRef.current = analyticsId;

    if (analyticsId) {
      push(["setUserId", analyticsId]);
      if (user?.organisation) {
        push(["setCustomDimension", Config.get<string>("analytics.matomoOrgDimensionId"), user.organisation]);
      }
    } else {
      push(["resetUserId"]);
    }
  }, [isAuth, user]);

  useEffect(() => {
    const url = Config.get<string>("analytics.matomoUrl");
    url && url.length && void trackAppRouter({
      url,
      siteId: Config.get<string>("analytics.matomoSiteId"),
      pathname,
      searchParams,
      disableCookies: true,
    });
  }, [pathname, searchParams]);

  return null;
}
