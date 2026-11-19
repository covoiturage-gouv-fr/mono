import { Prng } from "./prng.ts";
import { CarpoolStatus, SyntheticProfile } from "./types.ts";

export interface Commune {
  arr: string;
  lon: number;
  lat: number;
  pop: number | null;
}

export type AnomalyKind = "too_fast" | "same_identity" | "duplicate";

export interface StatusColumns {
  acquisition_status: "processed" | "expired" | "canceled";
  fraud_status: "pending" | "passed" | "failed";
  anomaly_status: "pending" | "passed" | "failed";
}

interface Identity {
  identity_key: string;
  operator_user_id: string;
  phone: string;
  phone_trunc: string;
}

export interface GeneratedCarpool extends StatusColumns {
  operator: string;
  territory: string;
  status: CarpoolStatus;
  anomaly: AnomalyKind | null;
  uuid: string;
  operator_journey_id: string;
  operator_trip_id: string;
  operator_class: string;
  start_datetime: string;
  end_datetime: string;
  created_at: string;
  start_lon: number;
  start_lat: number;
  start_geo_code: string;
  end_lon: number;
  end_lat: number;
  end_geo_code: string;
  distance: number;
  driver: Identity;
  passenger: Identity;
  passenger_seats: number;
  passenger_contribution: number;
  driver_revenue: number;
}

const HOUR_WEIGHTS: Record<string, number> = {
  6: 2,
  7: 8,
  8: 10,
  9: 4,
  12: 2,
  13: 2,
  16: 4,
  17: 9,
  18: 9,
  19: 4,
  21: 1,
};
const CLASS_WEIGHTS = { C: 80, B: 15, A: 5 };
// à vol d'oiseau → par la route
const ROAD_FACTOR = 1.3;
const MIN_DISTANCE = 2_000;
const MAX_DISTANCE = 80_000;

/** Premier jour (UTC) des `months` mois civils qui précèdent le mois de la date de référence */
export function monthsBefore(referenceDate: Date, months: number): Date[] {
  const year = referenceDate.getUTCFullYear();
  const month = referenceDate.getUTCMonth();
  return Array.from({ length: months }, (_, i) => new Date(Date.UTC(year, month - months + i, 1)));
}

export function statusColumns(status: CarpoolStatus): StatusColumns {
  switch (status) {
    case "ok":
      return { acquisition_status: "processed", fraud_status: "passed", anomaly_status: "passed" };
    case "fraud_error":
      return { acquisition_status: "processed", fraud_status: "failed", anomaly_status: "passed" };
    case "anomaly_error":
      return { acquisition_status: "processed", fraud_status: "passed", anomaly_status: "failed" };
    case "expired":
      return { acquisition_status: "expired", fraud_status: "pending", anomaly_status: "pending" };
    case "canceled":
      return { acquisition_status: "canceled", fraud_status: "pending", anomaly_status: "pending" };
  }
}

/** Distance à vol d'oiseau en mètres */
export function haversine(a: { lon: number; lat: number }, b: { lon: number; lat: number }): number {
  const rad = (deg: number) => (deg * Math.PI) / 180;
  const dLat = rad(b.lat - a.lat);
  const dLon = rad(b.lon - a.lon);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(rad(a.lat)) * Math.cos(rad(b.lat)) * Math.sin(dLon / 2) ** 2;
  return 2 * 6_371_000 * Math.asin(Math.sqrt(h));
}

export function makeIdentity(seed: string, operator: string, index: number, operatorIndex: number): Identity {
  const prng = new Prng(`${seed}:identity:${operator}:${index}`);
  // 06 39 98 xx xx : tranche réservée par l'ARCEP aux numéros fictifs
  const phone = `+3363998${operatorIndex}${String(index % 1000).padStart(3, "0")}`;
  return {
    identity_key: prng.hex(64),
    operator_user_id: prng.uuid(),
    phone,
    phone_trunc: phone.slice(0, -2),
  };
}

function pickCommune(prng: Prng, communes: Commune[]): Commune {
  const total = communes.reduce((sum, c) => sum + (c.pop || 1), 0);
  let cursor = prng.next() * total;
  for (const commune of communes) {
    cursor -= commune.pop || 1;
    if (cursor < 0) return commune;
  }
  return communes[communes.length - 1];
}

// ± 200 m autour du centroïde : le point reste dans la commune dans la quasi-totalité des cas
function jitter(prng: Prng, value: number): number {
  return Number((value + (prng.next() - 0.5) * 0.004).toFixed(6));
}

function pickStart(prng: Prng, month: Date): Date {
  const daysInMonth = new Date(Date.UTC(month.getUTCFullYear(), month.getUTCMonth() + 1, 0)).getUTCDate();
  let day = prng.int(1, daysInMonth);
  const weekday = new Date(Date.UTC(month.getUTCFullYear(), month.getUTCMonth(), day)).getUTCDay();
  // un second tirage pour les week-ends : moins de trajets, sans les supprimer
  if (weekday === 0 || weekday === 6) day = prng.int(1, daysInMonth);
  const hour = Number(prng.weighted(HOUR_WEIGHTS));
  // heure locale approchée par UTC+1, sans heure d'été
  return new Date(Date.UTC(month.getUTCFullYear(), month.getUTCMonth(), day, hour - 1, prng.int(0, 59)));
}

function addSeconds(date: Date, seconds: number): Date {
  return new Date(date.getTime() + seconds * 1000);
}

/**
 * Trajets du profil, dans les communes fournies par territoire.
 * Chaque trajet a son propre tirage : changer le volume ne modifie pas les trajets déjà générés.
 */
export function generateCarpools(
  profile: SyntheticProfile,
  referenceDate: Date,
  communes: Record<string, Commune[]>,
): GeneratedCarpool[] {
  const { seed, carpools: params } = profile;
  const operatorIndex = new Map(profile.operators.map((o, i) => [o.key, i]));
  const result: GeneratedCarpool[] = [];

  for (const territory of profile.territories) {
    const pool = communes[territory.key] ?? [];
    if (!pool.length) throw new Error(`Aucune commune pour le territoire '${territory.key}'`);
    if (!territory.operators.length) throw new Error(`Aucun opérateur pour le territoire '${territory.key}'`);

    for (const month of monthsBefore(referenceDate, profile.months)) {
      const monthKey = month.toISOString().slice(0, 7).replace("-", "");
      let previous: GeneratedCarpool | undefined;

      for (let i = 0; i < params.perTerritoryPerMonth; i++) {
        const id = `${territory.key}-${monthKey}-${String(i).padStart(5, "0")}`;
        const prng = new Prng(`${seed}:carpool:${id}`);

        const operator = prng.pick(territory.operators);
        const identity = (index: number) => makeIdentity(seed, operator, index, operatorIndex.get(operator) ?? 0);
        const driverIndex = prng.int(0, params.identitiesPerOperator - 1);
        const passengerIndex = (driverIndex + prng.int(1, params.identitiesPerOperator - 1)) %
          params.identitiesPerOperator;

        const start = pickCommune(prng, pool);
        let end = pickCommune(prng, pool);
        for (let attempt = 0; attempt < 5; attempt++) {
          const d = haversine(start, end) * ROAD_FACTOR;
          if (d >= MIN_DISTANCE && d <= MAX_DISTANCE) break;
          end = pickCommune(prng, pool);
        }
        const startPoint = { lon: jitter(prng, start.lon), lat: jitter(prng, start.lat) };
        const endPoint = { lon: jitter(prng, end.lon), lat: jitter(prng, end.lat) };
        const distance = Math.max(Math.round(haversine(startPoint, endPoint) * ROAD_FACTOR), prng.int(800, 1_900));

        // doublon possible seulement à la suite d'un trajet du même opérateur
        const kinds: AnomalyKind[] = previous?.operator === operator
          ? ["too_fast", "same_identity", "duplicate"]
          : ["too_fast", "same_identity"];
        const anomaly = prng.next() < params.anomalyRate ? prng.pick(kinds) : null;
        const status: CarpoolStatus = anomaly === null
          ? prng.weighted(params.statusWeights)
          : anomaly === "duplicate"
          ? "anomaly_error"
          : "fraud_error";

        const speed = anomaly === "too_fast" ? prng.int(200, 300) : prng.int(25, 70);
        const startDate = pickStart(prng, month);
        const endDate = addSeconds(startDate, Math.max(Math.round(distance / (speed / 3.6)), 60));
        const seats = prng.next() < 0.9 ? 1 : 2;
        const contribution = Math.round((distance / 1000) * prng.int(6, 10)) * seats;

        const carpool: GeneratedCarpool = {
          operator,
          territory: territory.key,
          status,
          anomaly,
          ...statusColumns(status),
          uuid: prng.uuid(),
          operator_journey_id: `syn-${id}`,
          operator_trip_id: `syn-trip-${id}`,
          operator_class: prng.weighted(CLASS_WEIGHTS),
          start_datetime: startDate.toISOString(),
          end_datetime: endDate.toISOString(),
          created_at: addSeconds(endDate, prng.int(300, 86_400)).toISOString(),
          start_lon: startPoint.lon,
          start_lat: startPoint.lat,
          start_geo_code: start.arr,
          end_lon: endPoint.lon,
          end_lat: endPoint.lat,
          end_geo_code: end.arr,
          distance,
          driver: identity(driverIndex),
          passenger: identity(anomaly === "same_identity" ? driverIndex : passengerIndex),
          passenger_seats: seats,
          passenger_contribution: contribution,
          driver_revenue: contribution,
        };

        // doublon : même équipage, même trajet, même horaire que le trajet précédent
        if (anomaly === "duplicate" && previous) {
          const { start_datetime, end_datetime, created_at, driver, passenger, distance } = previous;
          const { start_lon, start_lat, start_geo_code, end_lon, end_lat, end_geo_code } = previous;
          Object.assign(carpool, { start_datetime, end_datetime, created_at, driver, passenger, distance });
          Object.assign(carpool, { start_lon, start_lat, start_geo_code, end_lon, end_lat, end_geo_code });
        }

        result.push(carpool);
        previous = carpool;
      }
    }
  }

  return result;
}
