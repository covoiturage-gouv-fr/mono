import { assertEquals, assertObjectMatch } from "dep:assert";
import { afterAll, afterEach, beforeAll, beforeEach, describe, it } from "dep:testing-bdd";
import { PoolClient } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";
import { LegacyDbContext, makeLegacyDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { Id } from "../interfaces/index.ts";
import { insertableCarpool } from "../mocks/database/carpool.ts";
import { upsertableGeoError, upsertableGeoSuccess } from "../mocks/database/geo.ts";
import { CarpoolGeoRepository } from "./CarpoolGeoRepository.ts";
import { CarpoolRepository } from "./CarpoolRepository.ts";

describe("CarpoolGeoRepository", () => {
  let repository: CarpoolGeoRepository;
  let carpoolRepository: CarpoolRepository;
  let db: LegacyDbContext;
  let carpool_id: Id;
  let conn: PoolClient;

  const { before, after } = makeLegacyDbBeforeAfter();
  beforeAll(async () => {
    db = await before();
    repository = new CarpoolGeoRepository(db.connection);
    carpoolRepository = new CarpoolRepository(db.connection);
    const carpool = await carpoolRepository.register(insertableCarpool);
    carpool_id = carpool._id;
  });
  afterAll(async () => {
    await after(db);
  });
  beforeEach(async () => {
    conn = await db.connection.getClient().connect();
  });

  afterEach(() => {
    conn.release();
  });
  it("Should create geo", async () => {
    const processable = await repository.findProcessable(
      {
        limit: 1,
        from: insertableCarpool.start_datetime,
        to: insertableCarpool.end_datetime,
      },
      conn,
    );
    assertEquals(processable, [
      {
        carpool_id: carpool_id,
        start: insertableCarpool.start_position,
        end: insertableCarpool.end_position,
      },
    ]);
    const data = {
      ...upsertableGeoSuccess,
      carpool_id: processable[0].carpool_id,
    };
    await repository.upsert(data, conn);
    const result = await conn.query(sql`
    SELECT carpool_id, start_geo_code, end_geo_code FROM ${raw(repository.table)}
    WHERE carpool_id = ${carpool_id}
  `);
    assertObjectMatch(result.rows.pop(), data);
  });

  it("Should do nothing if geo exists", async () => {
    const processable = await repository.findProcessable(
      {
        limit: 1,
        from: insertableCarpool.start_datetime,
        to: insertableCarpool.end_datetime,
      },
      conn,
    );
    assertEquals(processable.length, 0);
    await conn.query(sql`
    DELETE FROM ${raw(repository.table)}
    WHERE carpool_id = ${carpool_id}
  `);
  });

  it("Should create status", async () => {
    const processable = await repository.findProcessable(
      {
        limit: 1,
        from: insertableCarpool.start_datetime,
        to: insertableCarpool.end_datetime,
      },
      conn,
    );
    assertEquals(processable, [
      {
        carpool_id: carpool_id,
        start: insertableCarpool.start_position,
        end: insertableCarpool.end_position,
      },
    ]);
    const data = {
      ...upsertableGeoError,
      carpool_id: processable[0].carpool_id,
    };
    await repository.upsert(data, conn);
    const result = await conn.query(sql`
      SELECT errors FROM ${raw(repository.table)}
      WHERE carpool_id = ${carpool_id}
    `);
    assertObjectMatch(result.rows.pop(), {
      errors: JSON.parse(JSON.stringify([data.error])),
    });
  });
});
