import { assertEquals, assertRejects } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { env_or_fail } from "@/lib/env/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";
import { DenoMigrator } from "./DenoMigrator.ts";

// Seeds : 17 périmètres, millésime 2021, dans la table geo.perimeters.
describe("geo.perimeters millésimes", () => {
  const mig = new DenoMigrator(env_or_fail("APP_POSTGRES_URL"));

  const stage = (year: number, limit = 1000) =>
    mig.testConn.query(sql`
      CREATE TABLE geo_export.${raw(`perimeters_${year}`)} AS
      SELECT ${year}::smallint AS year, centroid, geom, geom_simple, l_arr, arr, l_com, com, l_epci, epci,
        l_dep, dep, l_reg, reg, l_country, country, l_aom, aom, l_reseau, reseau, pop, surface,
        make_date(${year}, 1, 1) AS valid_from, make_date(${year} + 1, 1, 1) AS valid_until
      FROM geo.perimeters WHERE year = 2021 LIMIT ${limit}
    `);

  const attach = (source: string, year: number, replace = false) =>
    mig.testConn.query<{ rows: number }>(sql`
      SELECT geo.attach_millesime(${source}::regclass, ${year}::smallint, ${replace}) AS rows
    `);

  const switchToMillesimes = async () =>
    mig.testConn.query(
      raw(await Deno.readTextFile(new URL("../../../db/geo/switch-to-millesimes.sql", import.meta.url))),
    );

  const kind = async () =>
    (await mig.testConn.query<{ kind: string }>(sql`
      SELECT relkind::text AS kind FROM pg_class WHERE oid = 'geo.perimeters'::regclass
    `))[0].kind;

  // geo.perimeters_all avant la bascule, geo.perimeters après.
  let parent = "geo.perimeters_all";
  const partitions = async () =>
    (await mig.testConn.query<{ partition: string; count: number }>(sql`
      SELECT tableoid::regclass::text AS partition, count(*)::int AS count
      FROM ${raw(parent)} GROUP BY 1 ORDER BY 1
    `)).map((r) => `${r.partition}:${r.count}`);

  beforeAll(async () => {
    await mig.create();
    await mig.up();
    await mig.migrate({ flash: false, verbose: false });
    await mig.seed();
    await mig.testConn.query(sql`CREATE SCHEMA geo_export`);
    await stage(2021);
    await stage(2022);
    await stage(2023, 10);
  });

  afterAll(async () => {
    await mig.drop();
    await mig.down();
  });

  it("refuses to switch before any import", async () => {
    await assertRejects(() => switchToMillesimes(), Error);
  });

  it("keeps geo.perimeters as the current table while importing", async () => {
    assertEquals((await attach("geo_export.perimeters_2021", 2021))[0].rows, 17);
    assertEquals((await attach("geo_export.perimeters_2022", 2022))[0].rows, 17);
    assertEquals(await partitions(), ["geo.perimeters_2021:17", "geo.perimeters_2022:17"]);

    const [{ year }] = await mig.testConn.query<{ year: number }>(sql`SELECT geo.get_latest_millesime() AS year`);
    assertEquals({ kind: await kind(), year }, { kind: "r", year: 2021 });
  });

  it("renames the partitioned table to geo.perimeters and keeps the former one as backup", async () => {
    await switchToMillesimes();
    await switchToMillesimes();
    parent = "geo.perimeters";
    assertEquals(await kind(), "p");
    assertEquals(await partitions(), ["geo.perimeters_2021:17", "geo.perimeters_2022:17"]);

    const years = await mig.testConn.query<{ year: number; valid_from: string; valid_until: string }>(sql`
      SELECT DISTINCT year, valid_from::text, valid_until::text FROM geo.perimeters ORDER BY year
    `);
    assertEquals(years, [
      { year: 2021, valid_from: "2021-01-01", valid_until: "2022-01-01" },
      { year: 2022, valid_from: "2022-01-01", valid_until: "2023-01-01" },
    ]);

    const [{ count }] = await mig.testConn.query<{ count: number }>(sql`
      SELECT count(*)::int AS count FROM geo.perimeters_legacy
    `);
    assertEquals(count, 17);
  });

  it("serves historical lookups from the imported millesimes", async () => {
    const [years] = await mig.testConn.query<{ latest: number; year: number }>(sql`
      SELECT geo.get_latest_millesime() AS latest, geo.get_latest_millesime_or(2021::smallint) AS year
    `);
    assertEquals(years, { latest: 2022, year: 2021 });

    const [{ count }] = await mig.testConn.query<{ count: number }>(sql`
      SELECT count(*)::int AS count
      FROM geo.perimeters_2021 p, geo.get_by_code(p.arr, 2021::smallint) g
    `);
    assertEquals(count, 17);
  });

  it("refuses to overwrite an existing millesime without replace", async () => {
    await assertRejects(() => attach("geo_export.perimeters_2022", 2022), Error);
  });

  it("refuses a truncated millesime that would become the latest", async () => {
    await assertRejects(() => attach("geo_export.perimeters_2023", 2023), Error);
    assertEquals(await partitions(), ["geo.perimeters_2021:17", "geo.perimeters_2022:17"]);
  });

  it("replaces an existing millesime", async () => {
    assertEquals((await attach("geo_export.perimeters_2022", 2022, true))[0].rows, 17);
    assertEquals(await partitions(), ["geo.perimeters_2021:17", "geo.perimeters_2022:17"]);
  });

  it("refreshes com_evolution for the exported years and keeps older ones", async () => {
    await mig.testConn.query(sql`
      INSERT INTO geo.com_evolution (year, mod, old_com, new_com, l_mod)
      VALUES (2019, 31, '00001', '00002', 'fusion simple'), (2021, 31, '00003', '00004', 'fusion simple')
    `);
    await mig.testConn.query(sql`
      CREATE TABLE geo_export.com_evolution AS
      SELECT * FROM (VALUES
        (2020::smallint, 32::smallint, '00005'::varchar(5), '00006'::varchar(5), 'création de commune nouvelle'::varchar),
        (2021::smallint, 31::smallint, '00007'::varchar(5), '00008'::varchar(5), 'fusion simple'::varchar)
      ) AS t (year, mod, old_com, new_com, l_mod)
    `);
    await mig.testConn.query(
      raw(await Deno.readTextFile(new URL("../../../db/geo/import-com-evolution.sql", import.meta.url))),
    );

    const rows = await mig.testConn.query<{ year: number; old_com: string }>(sql`
      SELECT year, old_com FROM geo.com_evolution ORDER BY year, old_com
    `);
    assertEquals(rows, [
      { year: 2019, old_com: "00001" },
      { year: 2020, old_com: "00005" },
      { year: 2021, old_com: "00007" },
    ]);
  });
});
