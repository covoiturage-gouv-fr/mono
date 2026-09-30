import { assertEquals, assertRejects } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { env_or_fail } from "@/lib/env/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";
import { DenoMigrator } from "./DenoMigrator.ts";

// Seeds : 17 périmètres, millésime 2021, dans geo.perimeters.
describe("geo import", () => {
  const mig = new DenoMigrator(env_or_fail("APP_POSTGRES_URL"));

  // geo_export tel que restauré depuis le dump datalake, avec les 17 périmètres pour chaque année.
  const stage = async (years: number[], limit = 1000) => {
    await mig.testConn.query(sql`DROP SCHEMA IF EXISTS geo_export CASCADE`);
    await mig.testConn.query(sql`CREATE SCHEMA geo_export`);
    await mig.testConn.query(sql`
      CREATE TABLE geo_export.perimeters AS
      SELECT (row_number() OVER ())::integer AS id, y.year::smallint AS year, centroid, geom, geom_simple,
        l_arr, arr, l_com, com, l_epci, epci, l_dep, dep, l_reg, reg, l_country, country, l_aom, aom,
        l_reseau, reseau, pop, surface,
        make_date(y.year, 1, 1) AS valid_from, make_date(y.year + 1, 1, 1) AS valid_until
      FROM (SELECT * FROM public.seeded_perimeters LIMIT ${limit}) p
      CROSS JOIN unnest(${years}::int[]) AS y(year)
    `);
    await mig.testConn.query(sql`
      CREATE TABLE geo_export.com_evolution AS
      SELECT * FROM (VALUES
        (2020::smallint, 32::smallint, '00005'::varchar(5), '00006'::varchar(5), 'création de commune nouvelle'::varchar),
        (2021::smallint, 31::smallint, '00007'::varchar(5), '00008'::varchar(5), 'fusion simple'::varchar)
      ) AS t (year, mod, old_com, new_com, l_mod)
    `);
  };

  const runImport = async () =>
    mig.testConn.query(raw(await Deno.readTextFile(new URL("../../../db/geo/import.sql", import.meta.url))));

  const years = async (table: string) =>
    (await mig.testConn.query<{ year: number; count: number }>(sql`
      SELECT year, count(*)::int AS count FROM ${raw(table)} GROUP BY year ORDER BY year
    `)).map((r) => `${r.year}:${r.count}`);

  beforeAll(async () => {
    await mig.create();
    await mig.up();
    await mig.migrate({ flash: false, verbose: false });
    await mig.seed();
    await mig.testConn.query(sql`CREATE TABLE public.seeded_perimeters AS SELECT * FROM geo.perimeters`);
    await mig.testConn.query(sql`
      INSERT INTO geo.com_evolution (year, mod, old_com, new_com, l_mod)
      VALUES (2019, 31, '00001', '00002', 'fusion simple'), (2021, 31, '00003', '00004', 'fusion simple')
    `);
  });

  afterAll(async () => {
    await mig.drop();
    await mig.down();
  });

  it("replaces geo.perimeters on first import and keeps the former table as backup", async () => {
    await stage([2021, 2022]);
    await runImport();

    assertEquals(await years("geo.perimeters"), ["2021:17", "2022:17"]);
    assertEquals(await years("geo.perimeters_legacy"), ["2021:17"]);

    const [dates] = await mig.testConn.query<{ valid_from: string; valid_until: string }>(sql`
      SELECT valid_from::text, valid_until::text FROM geo.perimeters WHERE year = 2022 LIMIT 1
    `);
    assertEquals(dates, { valid_from: "2022-01-01", valid_until: "2023-01-01" });
  });

  it("keeps geo functions working on the imported millesimes", async () => {
    const [lookup] = await mig.testConn.query<{ latest: number; year: number; count: number }>(sql`
      SELECT geo.get_latest_millesime() AS latest,
        geo.get_latest_millesime_or(2021::smallint) AS year,
        (SELECT count(*)::int FROM geo.perimeters p, geo.get_by_code(p.arr, 2021::smallint) g WHERE p.year = 2021)
          AS count
    `);
    assertEquals(lookup, { latest: 2022, year: 2021, count: 17 });
  });

  it("refreshes com_evolution for the exported years and keeps older ones", async () => {
    const rows = await mig.testConn.query<{ year: number; old_com: string }>(sql`
      SELECT year, old_com FROM geo.com_evolution ORDER BY year, old_com
    `);
    assertEquals(rows, [
      { year: 2019, old_com: "00001" },
      { year: 2020, old_com: "00005" },
      { year: 2021, old_com: "00007" },
    ]);
  });

  it("replaces geo.perimeters again on the next import, backup untouched", async () => {
    await stage([2022, 2023]);
    await runImport();

    assertEquals(await years("geo.perimeters"), ["2022:17", "2023:17"]);
    assertEquals(await years("geo.perimeters_legacy"), ["2021:17"]);
  });

  it("refuses an older or truncated latest millesime and changes nothing", async () => {
    await stage([2021, 2022]);
    await assertRejects(() => runImport(), Error);

    await stage([2023, 2024], 10);
    await assertRejects(() => runImport(), Error);

    assertEquals(await years("geo.perimeters"), ["2022:17", "2023:17"]);
  });
});
