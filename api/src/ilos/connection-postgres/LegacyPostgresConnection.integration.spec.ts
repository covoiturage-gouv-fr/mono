import { assert, assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { env } from "@/lib/env/index.ts";
import { LegacyPostgresConnection } from "./LegacyPostgresConnection.ts";

// FIXME : failing because missing clearTimer in Cursor lib
describe("LegacyPostgresConnection", () => {
  const connection = new LegacyPostgresConnection({
    connectionString: env("APP_POSTGRES_URL"),
  });

  beforeAll(async () => {
    await connection.up();
  });

  afterAll(async () => {
    await connection.down();
  });

  it("Cursor 10 entries", async () => {
    const cursor = await connection.getNativeCursor(
      "SELECT * FROM generate_series(1, 20)",
      [],
    );
    assert("read" in cursor);
    assert("release" in cursor);

    const count = 10;
    const parts = await cursor.read(count);
    assertEquals(parts.length, count);

    await cursor.release();
  });

  it("Cursor loop through all entries", async () => {
    const rowCount = 1000;
    const cursor = await connection.getNativeCursor(
      `SELECT * FROM generate_series(1, ${rowCount})`,
      [],
    );
    assert("read" in cursor);
    assert("release" in cursor);

    let total = 0;
    let count = 100;
    do {
      const parts = await cursor.read(100);
      count = parts.length;
      total += parts.length;
    } while (count > 0);

    assertEquals(total, rowCount);

    await cursor.release();
  });
});
