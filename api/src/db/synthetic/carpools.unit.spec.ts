import { assert, assertEquals, assertNotEquals, assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { Commune, generateCarpools, haversine, makeIdentity, monthsBefore, statusColumns } from "./carpools.ts";
import { SyntheticProfile } from "./types.ts";

const profile: SyntheticProfile = {
  name: "test",
  seed: "unit",
  allowedEnvs: ["local"],
  referenceDate: "2026-03-15",
  months: 3,
  operators: [
    { key: "one", name: "One", legal_name: "One", siret: "00000001800010", uuid: "u1" },
    { key: "two", name: "Two", legal_name: "Two", siret: "00000002600013", uuid: "u2" },
  ],
  territories: [
    {
      key: "north",
      name: "North",
      legal_name: "North",
      siret: "",
      codes: [],
      operators: ["one", "two"],
      campaigns: [],
    },
    { key: "south", name: "South", legal_name: "South", siret: "", codes: [], operators: ["two"], campaigns: [] },
  ],
  users: [],
  carpools: {
    perTerritoryPerMonth: 200,
    identitiesPerOperator: 20,
    anomalyRate: 0.1,
    statusWeights: { ok: 80, fraud_error: 5, anomaly_error: 5, expired: 5, canceled: 5 },
  },
};
const reference = new Date("2026-03-15T00:00:00Z");
const communes: Record<string, Commune[]> = {
  north: [
    { arr: "35238", lon: -1.68, lat: 48.11, pop: 220000 },
    { arr: "35051", lon: -1.62, lat: 48.12, pop: 18000 },
    { arr: "35047", lon: -1.75, lat: 48.02, pop: 19000 },
  ],
  south: [
    { arr: "86066", lon: 0.55, lat: 46.82, pop: 31000 },
    { arr: "86245", lon: 0.45, lat: 46.77, pop: null },
  ],
};

describe("synthetic / carpools", () => {
  const carpools = generateCarpools(profile, reference, communes);

  it("lists the full months before the reference date", () => {
    assertEquals(
      monthsBefore(reference, 3).map((d) => d.toISOString().slice(0, 10)),
      ["2025-12-01", "2026-01-01", "2026-02-01"],
    );
    assertEquals(monthsBefore(new Date("2026-01-01T00:00:00Z"), 1)[0].toISOString().slice(0, 10), "2025-12-01");
  });

  it("maps statuses to the three status columns", () => {
    assertEquals(statusColumns("ok"), {
      acquisition_status: "processed",
      fraud_status: "passed",
      anomaly_status: "passed",
    });
    assertEquals(statusColumns("fraud_error").fraud_status, "failed");
    assertEquals(statusColumns("anomaly_error").anomaly_status, "failed");
    assertEquals(statusColumns("expired").acquisition_status, "expired");
    assertEquals(statusColumns("canceled").acquisition_status, "canceled");
  });

  it("measures distances", () => {
    // Paris - Lyon : environ 392 km à vol d'oiseau
    const d = haversine({ lon: 2.3522, lat: 48.8566 }, { lon: 4.8357, lat: 45.764 });
    assert(d > 385_000 && d < 400_000, String(d));
  });

  it("is reproducible", () => {
    assertEquals(carpools, generateCarpools(profile, reference, communes));
    assertNotEquals(carpools, generateCarpools({ ...profile, seed: "other" }, reference, communes));
  });

  it("keeps existing trips when the volume grows", () => {
    const small = generateCarpools(
      { ...profile, carpools: { ...profile.carpools, perTerritoryPerMonth: 50, anomalyRate: 0 } },
      reference,
      communes,
    );
    const large = generateCarpools(
      { ...profile, carpools: { ...profile.carpools, anomalyRate: 0 } },
      reference,
      communes,
    );
    const byId = new Map(large.map((c) => [c.operator_journey_id, c]));
    assertEquals(small.length, 2 * 3 * 50);
    assert(small.every((c) => JSON.stringify(byId.get(c.operator_journey_id)) === JSON.stringify(c)));
  });

  it("generates the expected volume with unique keys", () => {
    assertEquals(carpools.length, 2 * 3 * 200);
    assertEquals(new Set(carpools.map((c) => `${c.operator}/${c.operator_journey_id}`)).size, carpools.length);
    assertEquals(new Set(carpools.map((c) => c.uuid)).size, carpools.length);
  });

  it("stays inside the period, the territory and its operators", () => {
    for (const c of carpools) {
      assert(
        c.start_datetime >= "2025-11-30T23:00:00.000Z" && c.start_datetime < "2026-03-01T00:00:00.000Z",
        c.start_datetime,
      );
      assert(c.end_datetime > c.start_datetime);
      assert(c.created_at > c.end_datetime);
      const codes = communes[c.territory].map((x) => x.arr);
      assert(codes.includes(c.start_geo_code) && codes.includes(c.end_geo_code));
      assert(profile.territories.find((t) => t.key === c.territory)!.operators.includes(c.operator));
      const start = communes[c.territory].find((x) => x.arr === c.start_geo_code)!;
      assert(haversine(start, { lon: c.start_lon, lat: c.start_lat }) < 400);
    }
  });

  it("produces plausible distances, speeds and amounts for regular trips", () => {
    for (const c of carpools.filter((x) => x.anomaly === null)) {
      const seconds = (Date.parse(c.end_datetime) - Date.parse(c.start_datetime)) / 1000;
      const kmh = (c.distance / seconds) * 3.6;
      assert(c.distance >= 800 && c.distance < 100_000, String(c.distance));
      assert(kmh > 10 && kmh <= 71, String(kmh));
      assert(c.passenger_contribution >= 0 && c.driver_revenue === c.passenger_contribution);
      assertNotEquals(c.driver.identity_key, c.passenger.identity_key);
      assert(["A", "B", "C"].includes(c.operator_class));
    }
  });

  it("mixes statuses and flags every anomaly as an error", () => {
    const statuses = new Set(carpools.map((c) => c.status));
    assertEquals([...statuses].sort(), ["anomaly_error", "canceled", "expired", "fraud_error", "ok"]);
    assert(carpools.filter((c) => c.status === "ok").length > carpools.length * 0.6);

    const anomalies = carpools.filter((c) => c.anomaly !== null);
    assert(anomalies.length > 0 && anomalies.length < carpools.length * 0.2);
    assertEquals(new Set(anomalies.map((c) => c.anomaly)).size, 3);
    for (const c of anomalies) {
      assertEquals(c.status, c.anomaly === "duplicate" ? "anomaly_error" : "fraud_error");
    }
  });

  it("builds each kind of anomaly", () => {
    for (const [index, c] of carpools.entries()) {
      const seconds = (Date.parse(c.end_datetime) - Date.parse(c.start_datetime)) / 1000;
      if (c.anomaly === "too_fast") assert((c.distance / seconds) * 3.6 > 150 || seconds === 60);
      if (c.anomaly === "same_identity") assertEquals(c.driver, c.passenger);
      if (c.anomaly === "duplicate") {
        const previous = carpools[index - 1];
        assertEquals(
          [c.operator, c.start_datetime, c.driver, c.passenger, c.start_geo_code, c.end_geo_code],
          [
            previous.operator,
            previous.start_datetime,
            previous.driver,
            previous.passenger,
            previous.start_geo_code,
            previous.end_geo_code,
          ],
        );
        assertNotEquals(c.operator_journey_id, previous.operator_journey_id);
      }
    }
  });

  it("builds fictitious identities", () => {
    const identity = makeIdentity("unit", "one", 7, 1);
    assertEquals(identity, makeIdentity("unit", "one", 7, 1));
    assertEquals(identity.phone, "+33639981007");
    assertEquals(identity.phone_trunc, "+336399810");
    assertEquals(identity.identity_key.length, 64);
    assertNotEquals(identity.identity_key, makeIdentity("unit", "two", 7, 1).identity_key);
  });

  it("fails without communes", () => {
    assertThrows(() => generateCarpools(profile, reference, { north: communes.north }), Error, "Aucune commune");
  });
});
