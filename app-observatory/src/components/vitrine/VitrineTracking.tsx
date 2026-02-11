"use client";
import { sendEvent } from "@socialgouv/matomo-next";
import { useEffect } from "react";

export function VitrineTracking() {
  useEffect(() => {
    void sendEvent({ category: "vitrine", action: "landing", name: "startup-etat" });
  }, []);

  return null;
}
