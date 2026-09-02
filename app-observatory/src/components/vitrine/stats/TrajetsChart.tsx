"use client";

import {
  SeriePoint,
  TRAJETS_GOAL_2027,
  TRAJETS_PAR_AN,
  TRAJETS_PAR_MOIS,
  frMonthLabel,
} from "@/app/startup-etat/stats/data";
import { OBSERVATORY_API_URL } from "@/helpers/api";
import { useApi } from "@/hooks/useApi";
import StatChart from "./StatChart";

type EvolFluxRow = { year: number; month?: number; journeys: number | string };

// Série nationale des trajets « passager » validés, issue de l'API observatoire
// (endpoint evol-flux, périmètre France). Complétée / doublée par un repli figé
// (src/app/stats/data.ts) qui garde l'historique complet depuis 2019 et prend le
// relais si l'API est indisponible.
export default function TrajetsChart({
  granularity,
}: {
  granularity: "month" | "year";
}) {
  const params = ["code=XXXXX", "type=country", "indic=journeys", "past=7"];
  if (granularity === "month") {
    params.push("month=1");
  }
  const url = `${OBSERVATORY_API_URL}/evol-flux?${params.join("&")}`;
  const { data } = useApi<EvolFluxRow[]>(url);

  const fallback = granularity === "month" ? TRAJETS_PAR_MOIS : TRAJETS_PAR_AN;
  const merged = new Map<string, number>(
    fallback.map((p) => [p.x, p.y] as [string, number]),
  );
  for (const row of data ?? []) {
    const key =
      granularity === "month"
        ? `${row.year}-${String(row.month).padStart(2, "0")}`
        : String(row.year);
    merged.set(key, Number(row.journeys));
  }

  const points: SeriePoint[] = Array.from(merged, ([x, y]) => ({ x, y })).sort(
    (a, b) => a.x.localeCompare(b.x),
  );

  return (
    <StatChart
      title={
        granularity === "month"
          ? 'Trajets « passager » validés par mois'
          : 'Trajets « passager » validés par an'
      }
      labels={points.map((p) =>
        granularity === "month" ? frMonthLabel(p.x) : p.x,
      )}
      values={points.map((p) => p.y)}
      showValues={granularity === "year"}
      goal={granularity === "month" ? TRAJETS_GOAL_2027 : undefined}
      goalLabel="Objectif 2027 : 3 M/mois"
    />
  );
}
