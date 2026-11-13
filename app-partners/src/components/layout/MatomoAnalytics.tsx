"use client";

import { analytics, matomoEnabled } from "@/config/analytics";
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
    if (!matomoEnabled(analytics)) return;
    const analyticsId = isAuth && user?.analytics_id ? user.analytics_id : null;
    if (analyticsId === lastUserIdRef.current) return;
    lastUserIdRef.current = analyticsId;

    if (analyticsId) {
      push(["setUserId", analyticsId]);
      if (user?.organisation) {
        push(["setCustomDimension", analytics.matomoOrgDimensionId, user.organisation]);
      }
    } else {
      push(["resetUserId"]);
    }
  }, [isAuth, user]);

  useEffect(() => {
    if (matomoEnabled(analytics)) {
      trackAppRouter({
        url: analytics.matomoUrl,
        siteId: analytics.matomoSiteId,
        pathname,
        searchParams,
        disableCookies: true,
      });
    }
  }, [pathname, searchParams]);

  return null;
}
