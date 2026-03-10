import { ClientOptions, PostgresError, TransactionError } from "dep:postgres";
import { assert } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { env } from "../../lib/env/index.ts";
import sql from "../../lib/pg/sql.ts";
import { DenoPostgresConnection } from "./DenoPostgresConnection.ts";

describe("DenoPostgresConnection - connection", () => {
  // Restore the original CA when done
  // as manipulating the env variables impacts the whole process.
  const originalCA = Deno.env.get("APP_POSTGRES_CA");
  afterAll(() => {
    if (originalCA) Deno.env.set("APP_POSTGRES_CA", originalCA);
    else Deno.env.delete("APP_POSTGRES_CA");
  });

  async function testConnection(params?: string | ClientOptions | undefined) {
    let connection: DenoPostgresConnection | null = null;
    try {
      connection = new DenoPostgresConnection(params);
      await connection.up();
      assert(await connection.isReady());
      await connection.down();
      assert(await connection.isReady() === false);
    } catch (e) {
      e instanceof Error && console.error(e.message);
      throw e;
    } finally {
      connection && await connection.isReady() && await connection.down();
    }
  }

  it("should connect without parameters", async () => {
    // Force insecure connection to bypass the missing certificate
    Deno.env.set("APP_POSTGRES_INSECURE", "true");
    await testConnection();
  });

  it("should connect with a connection string", async () => {
    // Force insecure connection to bypass the missing certificate
    Deno.env.set("APP_POSTGRES_INSECURE", "true");
    await testConnection(env("APP_POSTGRES_URL"));
  });

  it("should crash on missing certificate without forcing insecure", async () => {
    // Remove the insecure connection
    Deno.env.delete("APP_POSTGRES_CA");
    Deno.env.delete("APP_POSTGRES_INSECURE");

    let connection: DenoPostgresConnection | null = null;
    try {
      connection = new DenoPostgresConnection();
      await connection.up();
      assert(false, "Should have thrown an error");
    } catch (e) {
      assert(e instanceof Error);
      assert(e.message.includes("APP_POSTGRES_CA or APP_POSTGRES_INSECURE must be set"));
    } finally {
      connection && await connection.down();
    }
  });

  it("should crash on rogue certificate", async () => {
    try {
      // Remove the insecure connection
      Deno.env.set("APP_POSTGRES_CA", "rogue-certificate");
      Deno.env.delete("APP_POSTGRES_INSECURE");

      let connection: DenoPostgresConnection | null = null;
      try {
        connection = new DenoPostgresConnection();
        await connection.up();
        assert(false, "Should have thrown an error");
      } catch (_e) {
        assert(_e instanceof Error);
      } finally {
        connection && await connection.down();
      }
    } catch (e) {
      assert(e instanceof Error);
      assert(e.message.includes("The server isn't accepting TLS connections"));
    }
  });

  it("should dispose the connection when leaving scope", async () => {
    // Force insecure connection to bypass the missing certificate
    Deno.env.set("APP_POSTGRES_INSECURE", "true");

    await (async () => {
      await using connection = new DenoPostgresConnection();
      await connection.up();
      assert(await connection.isReady());
    })();

    // @ts-expect-error connection is out of scope
    assert(typeof connection === "undefined");
    // test should not leak as the connection is disposed
  });
});

describe("DenoPostgresConnection - poolSize", () => {
  it("should set the pool size", async () => {
    // Force insecure connection to bypass the missing certificate
    Deno.env.set("APP_POSTGRES_INSECURE", "true");

    // Set the pool size to 5
    Deno.env.set("APP_POSTGRES_POOL_SIZE", "5");
    assert(new DenoPostgresConnection().poolSize === 5);

    // Use the default pool size
    Deno.env.delete("APP_POSTGRES_POOL_SIZE");
    assert(new DenoPostgresConnection().poolSize === 3);
  });
});

describe("DenoPostgresConnection - debug levels", () => {
  it("should set the debug level", async () => {
    // Force insecure connection to bypass the missing certificate
    Deno.env.set("APP_POSTGRES_INSECURE", "true");

    // Set the debug level to queries
    Deno.env.set("APP_POSTGRES_DEBUG", "queries");
    let connection = new DenoPostgresConnection();
    assert(typeof connection.poolConfig.controls!.debug === "object");
    assert(connection.poolConfig.controls!.debug!.queries as boolean === true);
    assert(connection.poolConfig.controls!.debug!.notices as boolean === false);
    assert(connection.poolConfig.controls!.debug!.results as boolean === false);
    assert(connection.poolConfig.controls!.debug!.queryInError as boolean === false);

    // Set the debug level to notices
    Deno.env.set("APP_POSTGRES_DEBUG", "notices");
    connection = new DenoPostgresConnection();
    assert(typeof connection.poolConfig.controls!.debug === "object");
    assert(connection.poolConfig.controls!.debug!.queries as boolean === false);
    assert(connection.poolConfig.controls!.debug!.notices as boolean === true);
    assert(connection.poolConfig.controls!.debug!.results as boolean === false);
    assert(connection.poolConfig.controls!.debug!.queryInError as boolean === false);

    // Set the debug level to notices and results
    Deno.env.set("APP_POSTGRES_DEBUG", "notices,results");
    connection = new DenoPostgresConnection();
    assert(typeof connection.poolConfig.controls!.debug === "object");
    assert(connection.poolConfig.controls!.debug!.queries as boolean === false);
    assert(connection.poolConfig.controls!.debug!.notices as boolean === true);
    assert(connection.poolConfig.controls!.debug!.results as boolean === true);
    assert(connection.poolConfig.controls!.debug!.queryInError as boolean === false);

    // Set the debug level to queries,notices,results,queryInError in uppercase with pipe separator
    Deno.env.set("APP_POSTGRES_DEBUG", "QUERIES|NOTICES|RESULTS|QUERYINERROR");
    connection = new DenoPostgresConnection();
    assert(typeof connection.poolConfig.controls!.debug === "object");
    assert(connection.poolConfig.controls!.debug!.queries as boolean === true);
    assert(connection.poolConfig.controls!.debug!.notices as boolean === true);
    assert(connection.poolConfig.controls!.debug!.results as boolean === true);
    assert(connection.poolConfig.controls!.debug!.queryInError as boolean === true);

    // Set the debug level to all
    Deno.env.set("APP_POSTGRES_DEBUG", "all");
    connection = new DenoPostgresConnection();
    assert(typeof connection.poolConfig.controls!.debug === "boolean");
    assert(connection.poolConfig.controls!.debug as boolean === true);

    Deno.env.delete("APP_POSTGRES_DEBUG");
  });
});

describe("DenoPostgresConnection - query helpers", () => {
  // Force insecure connection to bypass the missing certificate
  Deno.env.set("APP_POSTGRES_INSECURE", "true");
  Deno.env.delete("APP_POSTGRES_DEBUG");

  const connection = new DenoPostgresConnection();
  const query = sql`SELECT * FROM generate_series(1, 10)`;

  beforeAll(async () => {
    await connection.up();
  });

  afterAll(async () => {
    await connection.down();
  });

  it("should use query as queryObject", async () => {
    const results = await connection.query<{ generate_series: number }>(query);
    assert(results.length === 10);
    assert(results[0].generate_series === 1);
    assert(results[9].generate_series === 10);
  });

  it("should use queryArray", async () => {
    const results = await connection.queryArray<[number]>(query);
    assert(results.length === 10);
    assert(results[0][0] === 1);
    assert(results[9][0] === 10);
  });

  it("should use queryObject", async () => {
    const results = await connection.queryObject<{ generate_series: number }>(query);
    assert(results.length === 10);
    assert(results[0].generate_series === 1);
    assert(results[9].generate_series === 10);
  });
});

describe("DenoPostgresConnection - transaction", () => {
  // Force insecure connection to bypass the missing certificate
  Deno.env.set("APP_POSTGRES_INSECURE", "true");
  Deno.env.delete("APP_POSTGRES_DEBUG");

  const connection = new DenoPostgresConnection();

  beforeAll(async () => {
    await connection.up();
    await connection.query(sql`CREATE TABLE IF NOT EXISTS test_transaction (id SERIAL PRIMARY KEY)`);
  });

  afterAll(async () => {
    await connection.query(sql`DROP TABLE IF EXISTS test_transaction`);
    await connection.down();
  });

  it("should use transaction and rollback", async () => {
    using client = await connection.pool.connect();
    const transaction = client.createTransaction(DenoPostgresConnection.id());
    await transaction.begin();
    await transaction.queryObject(sql`INSERT INTO test_transaction (id) VALUES (1)`);
    await transaction.queryObject(sql`INSERT INTO test_transaction (id) VALUES (2)`);
    await transaction.queryObject(sql`INSERT INTO test_transaction (id) VALUES (3)`);

    // Insert entries in a transsaction
    let { rows } = await transaction.queryObject<{ id: number }>(sql`SELECT * FROM test_transaction ORDER BY id`);
    assert(rows.length === 3);
    assert(rows[0].id === 1);
    assert(rows[1].id === 2);
    assert(rows[2].id === 3);

    // Rollback to discard the new entries
    await transaction.rollback();

    // Check that the entries are not present
    ({ rows } = await client.queryObject<{ id: number }>(sql`SELECT * FROM test_transaction ORDER BY id`));
    assert(rows.length === 0);
  });

  it("should crash on insert if read_only", async () => {
    using client = await connection.pool.connect();
    const id = DenoPostgresConnection.id();

    try {
      const transaction = client.createTransaction(id, { read_only: true });
      await transaction.begin();
      await transaction.queryObject(sql`INSERT INTO test_transaction (id) VALUES (1)`);
      await transaction.commit();
    } catch (e) {
      assert(e instanceof TransactionError);
      assert(e.message.includes(`The transaction "${id}" has been aborted`));
    }
  });
});

describe("DenoPostgresConnection - cursor", () => {
  // Force insecure connection to bypass the missing certificate
  Deno.env.set("APP_POSTGRES_INSECURE", "true");
  const connection = new DenoPostgresConnection();

  beforeAll(async () => {
    await connection.up();
    await connection.query(sql`CREATE TABLE IF NOT EXISTS test_cursor (id SERIAL PRIMARY KEY)`);
    await connection.query(sql`TRUNCATE TABLE test_cursor`);
    await connection.query(sql`INSERT INTO test_cursor (id) SELECT generate_series AS id FROM generate_series(1, 100)`);
  });

  afterAll(async () => {
    await connection.query(sql`DROP TABLE IF EXISTS test_cursor`);
    await connection.down();
  });

  it("should use cursor AsyncIterator", async () => {
    await using cursor = await connection.cursor<{ id: number }>(sql`SELECT * FROM test_cursor ORDER BY id`);

    // concat all rows in a big array
    const results = [] as { id: number }[];
    for await (const rows of cursor.read(10)) {
      assert(rows.length === 10);
      results.push(...rows);
    }

    assert(results.length === 100);
    assert(results[0].id === 1);
    assert(results[99].id === 100);
  });

  it("should handle SQL errors in cursor", async () => {
    try {
      await using _cursor = await connection.cursor<{ id: number }>(sql`SELECT blop`);
    } catch (e) {
      assert(e instanceof PostgresError);
      assert(e.message.includes('"blop" does not exist'));
    }
  });
});
