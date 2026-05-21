import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { HonorRepositoryProvider } from "./HonorRepositoryProvider.ts";

type Row = { _id: number; type: string; employer: string };

describe("HonorRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: HonorRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new HonorRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("save inserts a row that round-trips type and employer", async () => {
    await repository.save("public", "ACME");

    const rows = await db.connection.query<Row>(sql`
      SELECT _id, type, employer
      FROM honor.tracking
      WHERE employer = 'ACME'
      ORDER BY _id DESC
      LIMIT 1
    `);

    assertEquals(rows.length, 1);
    assertEquals(rows[0].type, "public");
    assertEquals(rows[0].employer, "ACME");
    assertEquals(typeof rows[0]._id, "number");
  });

  it("save accepts the limited type and an empty employer", async () => {
    await repository.save("limited", "");

    const rows = await db.connection.query<Row>(sql`
      SELECT _id, type, employer
      FROM honor.tracking
      WHERE type = 'limited' AND employer = ''
      ORDER BY _id DESC
      LIMIT 1
    `);

    assertEquals(rows.length, 1);
    assertEquals(rows[0].type, "limited");
    assertEquals(rows[0].employer, "");
  });

  it("save allocates distinct _ids for repeated inserts", async () => {
    await repository.save("public", "ID-A");
    await repository.save("public", "ID-B");

    const rows = await db.connection.query<Row>(sql`
      SELECT _id, employer
      FROM honor.tracking
      WHERE employer IN ('ID-A', 'ID-B')
      ORDER BY _id
    `);

    assertEquals(rows.length, 2);
    assertEquals(rows[0]._id !== rows[1]._id, true);
  });
});
