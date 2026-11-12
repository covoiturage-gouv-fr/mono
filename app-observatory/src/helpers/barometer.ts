import type {
  BarometerChart,
  BarometerMeasure,
  BarometerPoint,
  BarometerProvider,
} from "@/interfaces/observatoire/barometer";
import type { FeatureCollection, Point } from "geojson";
import { OBSERVATORY_API_URL } from "./api";

// Tant que la route datalake n'existe pas, `NEXT_PUBLIC_BAROMETER_FIXTURES=true`
// sert les JSON de `public/barometre-fixtures/`.
const USE_FIXTURES = process.env.NEXT_PUBLIC_BAROMETER_FIXTURES === "true";

export const barometerPointsUrl = () =>
  USE_FIXTURES ? "/barometre-fixtures/points.json" : `${OBSERVATORY_API_URL}/barometer/points`;

export const barometerMeasuresUrl = (id: number) =>
  USE_FIXTURES
    ? `/barometre-fixtures/mesures/${id}.json`
    : `${OBSERVATORY_API_URL}/barometer/measures?id=${id}`;

const PALETTE = ["#000091", "#e1000f", "#b7a73f", "#6a6af4", "#3182bd", "#18753c"];

export function providersOf(points: BarometerPoint[]): BarometerProvider[] {
  const byId = new Map(points.map((p) => [p.fournisseur_id, p.fournisseur_nom]));
  return Array.from(byId, ([id, name]) => ({ id, name }))
    .sort((a, b) => a.name.localeCompare(b.name, "fr"));
}

export function filterPoints(points: BarometerPoint[], providerId: string | null): BarometerPoint[] {
  return providerId ? points.filter((p) => p.fournisseur_id === providerId) : points;
}

export function providerColors(providers: BarometerProvider[]): Record<string, string> {
  return Object.fromEntries(providers.map((p, i) => [p.id, PALETTE[i % PALETTE.length]]));
}

export function pointsToGeoJSON(points: BarometerPoint[]): FeatureCollection<Point, BarometerPoint> {
  return {
    type: "FeatureCollection",
    features: points.map((p) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [p.lon, p.lat] },
      properties: p,
    })),
  };
}

const JOURS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"];

// Dates ISO sans heure : lues et formatées en UTC pour ne pas glisser d'un jour.
const utc = (iso: string) => new Date(`${iso}T00:00:00Z`);
const format = (iso: string, opts: Intl.DateTimeFormatOptions) =>
  utc(iso).toLocaleDateString("fr-FR", { timeZone: "UTC", ...opts });

export function periodLabel(debut: string, fin: string): string {
  if (debut === fin) return format(debut, { day: "numeric", month: "long", year: "numeric" });
  const d = utc(debut);
  const lastDay = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0)).toISOString().slice(0, 10);
  if (d.getUTCDate() === 1 && fin === lastDay) return format(debut, { month: "long", year: "numeric" });
  return `du ${format(debut, {})} au ${format(fin, {})}`;
}

const sortedUnique = <T>(values: T[], compare: (a: T, b: T) => number) =>
  Array.from(new Set(values)).sort(compare);

// Une série par période : valeurs alignées sur `labels`, NaN là où la période n'a pas de mesure.
function seriesByPeriod(
  rows: BarometerMeasure[],
  labels: string[],
  labelOf: (m: BarometerMeasure) => string,
  seriesOf: (m: BarometerMeasure) => string,
) {
  const ordered = [...rows].sort((a, b) => a.debut.localeCompare(b.debut));
  const names = Array.from(new Set(ordered.map(seriesOf)));
  return names.map((name) => {
    const values = new Map(rows.filter((m) => seriesOf(m) === name).map((m) => [labelOf(m), m.tx_covoiturage]));
    return { label: name, data: labels.map((l) => values.get(l) ?? NaN) };
  });
}

export function chartsOf(measures: BarometerMeasure[]): BarometerChart[] {
  const charts: BarometerChart[] = [];
  const period = (m: BarometerMeasure) => periodLabel(m.debut, m.fin);

  const byHour = measures.filter((m) => m.heure !== null);
  if (byHour.length) {
    const hours = sortedUnique(byHour.map((m) => m.heure!), (a, b) => a - b);
    const labels = hours.map((h) => `${h}h`);
    charts.push({
      key: "heure",
      title: "Taux de covoiturage par heure",
      kind: "bar",
      labels,
      series: seriesByPeriod(
        byHour,
        labels,
        (m) => `${m.heure}h`,
        (m) => (m.jour ? `${period(m)} (${JOURS[m.jour - 1]})` : period(m)),
      ),
    });
  }

  const byDay = measures.filter((m) => m.heure === null && m.jour !== null);
  if (byDay.length) {
    const days = sortedUnique(byDay.map((m) => m.jour!), (a, b) => a - b);
    const labels = days.map((d) => JOURS[d - 1]);
    charts.push({
      key: "jour",
      title: "Taux de covoiturage moyen par jour de la semaine",
      kind: "bar",
      labels,
      series: seriesByPeriod(byDay, labels, (m) => JOURS[(m.jour!) - 1], period),
    });
  }

  const byPeriod = measures
    .filter((m) => m.heure === null && m.jour === null)
    .sort((a, b) => a.debut.localeCompare(b.debut));
  if (byPeriod.length) {
    charts.push({
      key: "periode",
      title: "Évolution du taux de covoiturage moyen",
      kind: byPeriod.length >= 3 ? "line" : "bar",
      labels: byPeriod.map(period),
      series: [{ label: "Taux moyen", data: byPeriod.map((m) => m.tx_covoiturage) }],
    });
  }

  return charts;
}
