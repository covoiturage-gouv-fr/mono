import { assert, assertEquals, assertNotEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { PerimeterRepositoryProvider } from "@/pdc/services/territory/providers/PerimeterRepositoryProvider.ts";
import { ApplyAction } from "../actions/ApplyAction.ts";
import { FinalizeAction } from "../actions/FinalizeAction.ts";
import { IncentiveRepositoryProvider } from "../providers/IncentiveRepositoryProvider.ts";
import { MetadataRepositoryProvider } from "../providers/MetadataRepositoryProvider.ts";
import { PolicyRepositoryProvider } from "../providers/PolicyRepositoryProvider.ts";
import { TerritoryRepositoryProvider } from "../providers/TerritoryRepositoryProvider.ts";
import { TripRepositoryProvider } from "../providers/TripRepositoryProvider.ts";
import { SimulationRepositoryProvider } from "./SimulationRepositoryProvider.ts";
import { SimulationService } from "./SimulationService.ts";

// la_rochelle_2026: BlaBlaCar Daily, 5-70 km, classes B/C, 6 trips per driver and per day, aom 241700434
const BLABLACAR_DAILY_UUID = "0b361f5b-4651-45f1-8f59-5952d5e745fd";
const LA_ROCHELLE_AOM = "241700434";
const OTHER_AOM = "243300316";
const [IN_1, IN_2, OUT] = ["17300", "17400", "33063"];

describe("SimulationService", () => {
  let db: DenoDbContext;
  let simulation: SimulationService;
  let policyId: number;
  let operatorId: number;
  let incentiveRepository: IncentiveRepositoryProvider;
  let policyRepository: PolicyRepositoryProvider;
  let tripRepository: TripRepositoryProvider;
  let territoryRepository: TerritoryRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  const from = new Date("2026-03-01T00:00:00Z");
  const to = new Date("2026-03-04T00:00:00Z");

  async function insertGeo(arr: string, aom: string) {
    await db.connection.query(sql`
      INSERT INTO geo.perimeters (id, year, centroid, geom, geom_simple, arr, com, epci, dep, reg, country, aom)
      VALUES (
        ${Number(arr)}, 2026,
        ST_GeomFromText('POINT(1 46)', 4326),
        ST_GeomFromText('MULTIPOLYGON(((1 46, 1 47, 2 47, 2 46, 1 46)))', 4326),
        ST_GeomFromText('MULTIPOLYGON(((1 46, 1 47, 2 47, 2 46, 1 46)))', 4326),
        ${arr}, ${arr}, ${"2" + arr}, ${arr.slice(0, 2)}, '75', 'XXXXX', ${aom}
      )
    `);
  }

  async function insertCarpool(
    journey: string,
    datetime: string,
    start: string,
    end: string,
    opts: { driver?: string; distance?: number; operator_class?: string; canceled?: boolean } = {},
  ) {
    const rows = await db.connection.query<{ _id: number }>(sql`
      INSERT INTO carpool_v2.carpools (
        operator_id, operator_journey_id, operator_class, start_datetime, start_position, end_datetime, end_position,
        distance, driver_identity_key, driver_revenue, passenger_identity_key, passenger_seats, passenger_contribution
      ) VALUES (
        ${operatorId}, ${journey}, ${opts.operator_class ?? "C"}, ${datetime}::timestamptz,
        ST_SetSRID(ST_MakePoint(1.1, 46.1), 4326)::geography,
        ${datetime}::timestamptz + interval '20 minutes',
        ST_SetSRID(ST_MakePoint(1.2, 46.2), 4326)::geography,
        ${opts.distance ?? 15000}, ${opts.driver ?? "driver-1"}, 200, ${"passenger-" + journey}, 1, 100
      ) RETURNING _id
    `);
    const carpool_id = rows[0]._id;
    await db.connection.query(sql`
      INSERT INTO carpool_v2.geo (carpool_id, start_geo_code, end_geo_code) VALUES (${carpool_id}, ${start}, ${end})
    `);
    await db.connection.query(sql`
      INSERT INTO carpool_v2.status (carpool_id, acquisition_status)
      VALUES (${carpool_id}, ${opts.canceled ? "canceled" : "processed"}::carpool_v2.carpool_acquisition_status_enum)
    `);
  }

  function snapshot() {
    return db.connection.query<{ t: string; h: string }>(sql`
      SELECT 'incentives' AS t, md5(coalesce(string_agg(x::text, '|' ORDER BY x::text), '')) AS h FROM policy.incentives x
      UNION ALL SELECT 'metas', md5(coalesce(string_agg(x::text, '|' ORDER BY x::text), '')) FROM policy.policy_metas x
      UNION ALL SELECT 'policies', md5(coalesce(string_agg(x::text, '|' ORDER BY x::text), '')) FROM policy.policies x
      UNION ALL SELECT 'perimeters', md5(coalesce(string_agg(x::text, '|' ORDER BY x::text), '')) FROM territory.territory_perimeters x
      ORDER BY 1
    `);
  }

  beforeAll(async () => {
    db = await before();

    for (const [arr, aom] of [[IN_1, LA_ROCHELLE_AOM], [IN_2, LA_ROCHELLE_AOM], [OUT, OTHER_AOM]]) {
      await insertGeo(arr, aom);
    }

    const operators = await db.connection.query<{ _id: number }>(sql`
      SELECT _id FROM operator.operators WHERE uuid = ${BLABLACAR_DAILY_UUID}::uuid
    `);
    if (operators.length) {
      operatorId = operators[0]._id;
    } else {
      const created = await db.connection.query<{ _id: number }>(sql`
        INSERT INTO operator.operators (_id, name, legal_name, siret, company, address, bank, contacts, uuid)
        VALUES (9001, 'BBC Daily', 'BBC Daily', '12345678900011', '{}', '{}', '{}', '{}', ${BLABLACAR_DAILY_UUID}::uuid)
        RETURNING _id
      `);
      operatorId = created[0]._id;
    }

    const territory = await db.connection.query<{ _id: number }>(sql`
      INSERT INTO territory.territory_group (_id, company_id, name) VALUES (9001, 1, 'Simulation test') RETURNING _id
    `);
    await db.connection.query(sql`
      INSERT INTO territory.territory_perimeters (territory_id, version, arr, valid_from)
      VALUES (${territory[0]._id}, 1, ${[IN_1, IN_2]}::varchar[], '2026-01-01T00:00:00Z')
    `);
    const policy = await db.connection.query<{ _id: number }>(sql`
      INSERT INTO policy.policies (_id, territory_id, start_date, end_date, name, status, handler, max_amount)
      VALUES (9001, ${territory[0]._id}, '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z', 'Simulation test', 'active',
        'la_rochelle_2026', 21000000)
      RETURNING _id
    `);
    policyId = policy[0]._id;

    // a driver above the daily limit of 6 trips: the last ones are only reduced by the stateful step
    for (let i = 0; i < 8; i++) {
      await insertCarpool(`d1-${i}`, `2026-03-02T02:0${i}:00Z`, IN_1, IN_2);
    }
    await insertCarpool("d2-0", "2026-03-02T03:00:00Z", IN_1, IN_2, { driver: "driver-2" });
    await insertCarpool("d2-1", "2026-03-02T03:30:00Z", IN_1, OUT, { driver: "driver-2" });
    await insertCarpool("too-short", "2026-03-02T04:00:00Z", IN_1, IN_2, { driver: "driver-3", distance: 3000 });
    await insertCarpool("canceled", "2026-03-02T05:00:00Z", IN_1, IN_2, { driver: "driver-4", canceled: true });
    // second day, after the first finalize windows
    await insertCarpool("d5-0", "2026-03-03T10:00:00Z", IN_2, IN_1, { driver: "driver-5" });
    // outside the perimeter: never selected
    await insertCarpool("outside", "2026-03-02T06:00:00Z", OUT, OUT, { driver: "driver-6" });

    incentiveRepository = new IncentiveRepositoryProvider(db.connection);
    policyRepository = new PolicyRepositoryProvider(db.connection);
    tripRepository = new TripRepositoryProvider(db.connection);
    territoryRepository = new TerritoryRepositoryProvider(db.connection);
    simulation = new SimulationService(
      policyRepository,
      tripRepository,
      territoryRepository,
      new PerimeterRepositoryProvider(db.connection),
      new SimulationRepositoryProvider(db.connection),
      {} as never,
      {} as never,
    );
  });

  afterAll(async () => {
    await after(db);
  });

  it("writes nothing and matches a real apply --override then finalize", async () => {
    const before = await snapshot();
    const rows = await simulation.simulate({ policy_id: policyId, from, to });
    assertEquals(await snapshot(), before, "dry-run must not write");

    assertEquals(rows.length, 13);
    const byJourney = new Map(rows.map((r) => [r.operator_journey_id, r]));
    assert(!byJourney.has("outside"));
    assertEquals(byJourney.get("d1-0")?.simulated_final_amount, byJourney.get("d1-0")?.simulated_stateless_amount);
    assertEquals(byJourney.get("d1-7")?.simulated_final_amount, 0, "7th trip of the day is cut by the stateful step");
    assertNotEquals(byJourney.get("d1-7")?.simulated_stateless_amount, 0);
    assertEquals(byJourney.get("too-short")?.simulated_stateless_amount, 0);
    assert(byJourney.get("too-short")?.rejection_reason);
    assertEquals(byJourney.get("canceled")?.simulated_final_status, "error");
    assertEquals(byJourney.get("d1-0")?.current_amount, undefined);

    const apply = new ApplyAction(policyRepository, incentiveRepository, tripRepository, territoryRepository);
    await apply.processPolicy(policyId, from, to, true);
    const finalize = new FinalizeAction(
      policyRepository,
      incentiveRepository,
      new MetadataRepositoryProvider(db.connection),
      {} as never,
      {} as never,
    );
    await finalize.finalizeRange(from, to);

    const stored = await db.connection.query<
      { operator_journey_id: string; result: number; amount: number; status: string }
    >(
      sql`SELECT operator_journey_id, result, amount, status FROM policy.incentives WHERE policy_id = ${policyId}`,
    );
    assertEquals(stored.length, rows.length);
    for (const s of stored) {
      const r = byJourney.get(s.operator_journey_id)!;
      assertEquals(
        [r.simulated_stateless_amount, r.simulated_final_amount, r.simulated_final_status],
        [s.result, s.amount, s.status],
        s.operator_journey_id,
      );
    }
  });

  it("reports the current amounts of an existing run and still writes nothing", async () => {
    const before = await snapshot();
    const rows = await simulation.simulate({ policy_id: policyId, from, to });
    assertEquals(await snapshot(), before);

    const row = rows.find((r) => r.operator_journey_id === "d1-0")!;
    assertEquals([row.current_amount, row.current_status], [row.simulated_final_amount, "validated"]);
  });

  it("restricts the trips to --perimeter without writing the territory perimeter", async () => {
    const before = await snapshot();
    const rows = await simulation.simulate({ policy_id: policyId, from, to, perimeter: `aom:${OTHER_AOM}` });
    assertEquals(await snapshot(), before);

    assertEquals(rows.map((r) => r.operator_journey_id), ["d2-1", "outside"]);
  });

  it("rejects unknown perimeter codes", async () => {
    let error: unknown;
    try {
      await simulation.simulate({ policy_id: policyId, from, to, perimeter: "aom:999999999" });
    } catch (e) {
      error = e;
    }
    assert(error instanceof Error && error.message.includes("aom:999999999"));
  });
});
