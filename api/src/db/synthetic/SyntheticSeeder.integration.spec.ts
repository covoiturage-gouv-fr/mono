import { assert, assertEquals, assertRejects } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { luhnKey } from "./siret.ts";
import { SeedReport, SyntheticSeeder } from "./SyntheticSeeder.ts";
import { SyntheticProfile } from "./types.ts";

function siret(base: string): string {
  const siren = base + luhnKey(base);
  return `${siren}0001${luhnKey(`${siren}0001`)}`;
}

// codes présents dans le jeu géographique de test (seeds/geo.csv, millésime 2021)
const profile: SyntheticProfile = {
  name: "test",
  seed: "integration",
  allowedEnvs: ["local"],
  referenceDate: "2026-03-01",
  months: 2,
  operators: [
    { key: "one", name: "Synth One", legal_name: "Synth One SAS", siret: siret("00000011"), uuid: crypto.randomUUID() },
    { key: "two", name: "Synth Two", legal_name: "Synth Two SAS", siret: siret("00000012"), uuid: crypto.randomUUID() },
  ],
  territories: [
    {
      key: "saclay",
      name: "Synth Saclay",
      legal_name: "SYNTH SACLAY",
      siret: siret("00000013"),
      codes: ["epci:200056232"],
      operators: ["one", "two"],
      campaigns: [
        {
          name: "Synth terminée",
          handler: "grand_chatellerault_2025",
          start_date: "2025-01-01T00:00:00+0100",
          end_date: "2025-12-31T00:00:00+0100",
          max_amount: 1_000_00,
        },
        {
          name: "Synth active",
          handler: "grand_chatellerault_2026",
          start_date: "2026-01-01T00:00:00+0100",
          end_date: "2027-01-01T00:00:00+0100",
          max_amount: 1_000_00,
        },
      ],
    },
    {
      key: "lyon",
      name: "Synth Lyon",
      legal_name: "SYNTH LYON",
      siret: siret("00000014"),
      codes: ["com:69123"],
      operators: ["one"],
      campaigns: [],
    },
  ],
  users: [
    { email: "synth.admin@example.com", firstname: "A", lastname: "Admin", role: "registry.admin" },
    {
      email: "synth.one@example.com",
      firstname: "O",
      lastname: "One",
      role: "operator.admin",
      operator: "one",
      login_siren: siret("00000011").slice(0, 9),
    },
    {
      email: "synth.saclay@example.com",
      firstname: "S",
      lastname: "Saclay",
      role: "territory.user",
      territory: "saclay",
      login_siren: siret("00000013").slice(0, 9),
    },
  ],
  carpools: {
    perTerritoryPerMonth: 40,
    identitiesPerOperator: 10,
    anomalyRate: 0.1,
    statusWeights: { ok: 70, fraud_error: 10, anomaly_error: 5, expired: 10, canceled: 5 },
  },
};
const referenceDate = new Date("2026-03-01T00:00:00Z");

describe("SyntheticSeeder", () => {
  let db: DenoDbContext;
  let report: SeedReport;
  const { before, after } = makeDenoDbBeforeAfter();

  async function snapshot() {
    const [counts] = await db.connection.query<Record<string, number>>(sql`
      SELECT
        (SELECT count(*)::int FROM company.companies) AS companies,
        (SELECT count(*)::int FROM operator.operators) AS operators,
        (SELECT count(*)::int FROM territory.territory_group) AS territories,
        (SELECT count(*)::int FROM territory.territory_perimeters) AS perimeters,
        (SELECT count(*)::int FROM auth.users) AS users,
        (SELECT count(*)::int FROM auth.user_scopes) AS scopes,
        (SELECT count(*)::int FROM policy.policies) AS policies,
        (SELECT count(*)::int FROM carpool_v2.carpools) AS carpools,
        (SELECT count(*)::int FROM carpool_v2.status) AS statuses,
        (SELECT count(*)::int FROM carpool_v2.geo) AS geos,
        (SELECT count(*)::int FROM carpool_v2.requests) AS requests
    `);
    const carpools = await db.connection.query(sql`
      SELECT cc.operator_journey_id, cc.uuid, cc.start_datetime, cc.created_at, cc.distance, cs.acquisition_status
      FROM carpool_v2.carpools cc
      JOIN carpool_v2.status cs ON cs.carpool_id = cc._id
      WHERE cc.operator_journey_id LIKE 'syn-%'
      ORDER BY cc.operator_journey_id
    `);
    return { counts, carpools };
  }

  beforeAll(async () => {
    db = await before();
  });

  afterAll(async () => {
    await after(db);
  });

  it("seeds the whole profile", async () => {
    const initial = (await snapshot()).counts;
    report = await new SyntheticSeeder(db.connection, profile, referenceDate).run();
    const { counts } = await snapshot();

    assertEquals(report.carpools, { generated: 160, inserted: 160 });
    assertEquals(counts.companies - initial.companies, 4);
    assertEquals(counts.operators - initial.operators, 2);
    assertEquals(counts.territories - initial.territories, 2);
    assertEquals(counts.perimeters - initial.perimeters, 2);
    assertEquals(counts.users - initial.users, 3);
    assertEquals(counts.scopes - initial.scopes, 2);
    assertEquals(counts.policies - initial.policies, 2);
    for (const table of ["carpools", "statuses", "geos", "requests"]) {
      assertEquals(counts[table] - initial[table], 160, table);
    }
  });

  it("creates territories with a versioned perimeter and their company", async () => {
    const rows = await db.connection.query<{ name: string; siret: string; version: number; arr: string[] }>(sql`
      SELECT tg.name, cc.siret, tp.version, tp.arr
      FROM territory.territory_group tg
      JOIN company.companies cc ON cc._id = tg.company_id
      JOIN territory.territory_perimeters tp ON tp.territory_id = tg._id
      WHERE tg._id = ${report.territories.saclay}
    `);
    assertEquals(rows, [
      { name: "Synth Saclay", siret: profile.territories[0].siret, version: 1, arr: ["91377", "91471", "91477"] },
    ]);
  });

  it("creates users with their scope and login_siren", async () => {
    const rows = await db.connection.query(sql`
      SELECT u.email, u.role, u.status::varchar, u.login_siren, s.operator_id, s.territory_id, s.is_default
      FROM auth.users u
      LEFT JOIN auth.user_scopes s ON s.user_id = u._id
      WHERE u.email LIKE 'synth.%'
      ORDER BY u.email
    `);
    assertEquals(rows, [
      {
        email: "synth.admin@example.com",
        role: "registry.admin",
        status: "active",
        login_siren: null,
        operator_id: null,
        territory_id: null,
        is_default: null,
      },
      {
        email: "synth.one@example.com",
        role: "operator.admin",
        status: "active",
        login_siren: profile.operators[0].siret.slice(0, 9),
        operator_id: report.operators.one,
        territory_id: null,
        is_default: true,
      },
      {
        email: "synth.saclay@example.com",
        role: "territory.user",
        status: "active",
        login_siren: profile.territories[0].siret.slice(0, 9),
        operator_id: null,
        territory_id: report.territories.saclay,
        is_default: true,
      },
    ]);
  });

  it("creates finished and active campaigns", async () => {
    assertEquals(
      report.policies.map(({ territory, handler, status }) => ({ territory, handler, status })),
      [
        { territory: "saclay", handler: "grand_chatellerault_2025", status: "finished" },
        { territory: "saclay", handler: "grand_chatellerault_2026", status: "active" },
      ],
    );
  });

  it("geocodes carpools inside the territories with varied statuses", async () => {
    const rows = await db.connection.query<{ outside: number; statuses: number; mismatched: number }>(sql`
      SELECT
        count(*) FILTER (
          WHERE NOT (cg.start_geo_code = ANY(tp.arr) OR cg.end_geo_code = ANY(tp.arr))
        )::int AS outside,
        count(DISTINCT (cs.acquisition_status, cs.fraud_status, cs.anomaly_status))::int AS statuses,
        count(*) FILTER (WHERE cc.operator_id <> ALL(${Object.values(report.operators)}::int[]))::int AS mismatched
      FROM carpool_v2.carpools cc
      JOIN carpool_v2.geo cg ON cg.carpool_id = cc._id
      JOIN carpool_v2.status cs ON cs.carpool_id = cc._id
      JOIN territory.territory_perimeters tp
        ON tp.territory_id = CASE WHEN cc.operator_journey_id LIKE 'syn-saclay-%'
          THEN ${report.territories.saclay}::int ELSE ${report.territories.lyon}::int END
      WHERE cc.operator_journey_id LIKE 'syn-%'
    `);
    assertEquals(rows[0].outside, 0);
    assertEquals(rows[0].mismatched, 0);
    assert(rows[0].statuses >= 3, String(rows[0].statuses));
  });

  it("is idempotent", async () => {
    const first = await snapshot();
    const second = await new SyntheticSeeder(db.connection, profile, referenceDate).run();

    assertEquals(second.carpools, { generated: 160, inserted: 0 });
    assertEquals({ ...second, carpools: null }, { ...report, carpools: null });
    assertEquals(await snapshot(), first);
  });

  it("refuses a base where a profile operator uuid belongs to another operator", async () => {
    const clash: SyntheticProfile = {
      ...profile,
      operators: profile.operators.map((o, i) => i === 0 ? { ...o, siret: siret("00000099") } : o),
    };
    const first = await snapshot();
    await assertRejects(
      () => new SyntheticSeeder(db.connection, clash, referenceDate).run(),
      Error,
      "déjà porté par un autre opérateur",
    );
    assertEquals(await snapshot(), first);
  });

  it("never moves a sequence backwards", async () => {
    await db.connection.query(sql`SELECT setval('operator.operators__id_seq', 100000)`);
    await new SyntheticSeeder(db.connection, profile, referenceDate).run();
    const [{ last_value }] = await db.connection.query<{ last_value: number }>(sql`
      SELECT last_value::int FROM operator.operators__id_seq
    `);
    assertEquals(last_value, 100000);
  });

  it("fails on perimeters missing from geo.perimeters", async () => {
    const unknown: SyntheticProfile = {
      ...profile,
      territories: [{ ...profile.territories[0], name: "Synth inconnu", codes: ["epci:999999999"] }],
    };
    await assertRejects(
      () => new SyntheticSeeder(db.connection, unknown, referenceDate).run(),
      Error,
      "Périmètres absents de geo.perimeters pour 'Synth inconnu' : epci:999999999",
    );
  });

  it("fails on an empty geo schema", async () => {
    await db.connection.query(sql`DELETE FROM geo.perimeters`);
    await assertRejects(
      () => new SyntheticSeeder(db.connection, profile, referenceDate).run(),
      Error,
      "geo.perimeters est vide",
    );
  });
});
