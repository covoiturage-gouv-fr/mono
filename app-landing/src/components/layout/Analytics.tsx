"use client";
import { useEffect } from "react";
import { init } from "@socialgouv/matomo-next";
import { Config } from "@/config";

const matomoUrl = Config.get<string>('analytics.matomoUrl');
const matomoSiteId = Config.get<string>('analytics.matomoSiteId');

const Analytics = () => {
  useEffect(() => {
    init({ url: matomoUrl, siteId: matomoSiteId });
  }, []);

  return null;
};

export default Analytics;
