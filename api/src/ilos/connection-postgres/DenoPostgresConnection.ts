import { ClientOptions, Pool } from "dep:postgres";
import { array, assert, boolean, number, object, pattern, refine, string, union } from "dep:superstruct";
import { env_or_default, env_or_false } from "../../lib/env/index.ts";
import { logger } from "../../lib/logger/index.ts";
import sql, { Sql } from "../../lib/pg/sql.ts";
import { ConnectionInterface, DestroyHookInterface, InitHookInterface } from "../common/index.ts";

export class DenoPostgresConnection
  implements ConnectionInterface<Pool>, InitHookInterface, DestroyHookInterface, AsyncDisposable {
  #id = DenoPostgresConnection.id("pool");
  #pool: Pool | null = null;
  #poolSize = 3;
  #poolLazy = false;
  #poolConfig: ClientOptions = {};
  #connectionAttempts = 0;
  #connectionString = "";

  /**
   * Generate a unique transaction name
   */
  static id(prefix: "transaction" | "cursor" | "pool" = "transaction"): string {
    return `${prefix}_${new Date().getTime()}_${Math.random().toString(36).slice(2)}`.toLowerCase();
  }

  constructor(params?: string | ClientOptions | undefined) {
    // Get and parse the config and the connection string
    const { connectionString, config } = this.#configure(params);
    const { hostname, username, password, pathname, port } = new URL(connectionString);

    /**
     * Check or override the TLS certificate
     */
    const certificate: string = env_or_default("APP_POSTGRES_CA", "");
    const forceInsecure: boolean = env_or_false("APP_POSTGRES_INSECURE");
    if (!certificate.length && !forceInsecure) {
      throw new Error("APP_POSTGRES_CA or APP_POSTGRES_INSECURE must be set");
    }

    if (forceInsecure) {
      logger.warn("APP_POSTGRES_INSECURE is set, using insecure connection");
    }

    /**
     * Set the pool size
     *
     * @default 3
     */
    this.#poolSize = Number(env_or_default("APP_POSTGRES_POOL_SIZE", "3"));

    /**
     * Set the debugging level for the postgres connection
     *
     * @url https://deno-postgres.com/#/?id=debugging
     *
     * @example
     * APP_POSTGRES_DEBUG=queries,notices,results,queryInError
     * APP_POSTGRES_DEBUG=queries
     * APP_POSTGRES_DEBUG=NOTICES|RESULTS
     */
    const debugString: string = env_or_default("APP_POSTGRES_DEBUG", "").toLocaleLowerCase();

    this.#poolConfig = this.#validateConfig({
      applicationName: "deno-postgres",
      connection: {
        attempts: 1,
      },
      hostname,
      port: Number(port),
      database: pathname.slice(1),
      user: username,
      password: decodeURIComponent(password),
      tls: {
        enabled: true,
        enforce: forceInsecure ? false : true,
        caCertificates: forceInsecure ? [] : [certificate!],
      },
      controls: {
        debug: debugString === "all" ? true : {
          queries: debugString.includes("queries"),
          notices: debugString.includes("notices"),
          results: debugString.includes("results"),
          queryInError: debugString.includes("queryinerror"),
        },
        decoders: {
          // Postgres int8 / bigint (OID 20). Without this decoder, deno-postgres
          // returns native BigInt for COUNT/SUM results and Express res.json()
          // throws "Do not know how to serialize a BigInt" (issue #3176).
          // Carpool counts and incentive sums stay well below
          // Number.MAX_SAFE_INTEGER (2^53 - 1), so decoding to number is safe
          // and keeps ResultInterface.<field>: number truthful at runtime.
          20: (value: string): number => Number(value),
          // Postgres float4 / float8 (OIDs 700, 701). deno-postgres 0.19.5
          // returns these as strings by default; node-pg returned them as
          // numbers. Without these decoders, callers reading ST_X / ST_Y,
          // AVG(), or any double-precision column get "string" where the
          // result interface declares "number" (ticket #3179 regression
          // surfaced by the company.companies geo round-trip).
          700: (value: string): number => Number(value),
          701: (value: string): number => Number(value),
          // Postgres numeric / decimal (OID 1700). Arbitrary-precision in
          // Postgres; deno-postgres preserves that by returning a string.
          // RPC uses numeric for cent-denominated amounts and aggregated
          // counts (policy.incentives sums, dashboard_stats journeys /
          // incentive_amount, ST_X/ST_Y casts in carpool/policy geo queries).
          // Every plausible value stays well below Number.MAX_SAFE_INTEGER
          // (2^53 - 1 ≈ 9e15), so JS Number is lossless in practice and
          // restores parity with node-pg (which decoded numeric to number).
          // Note: this trades arbitrary precision for ergonomics. If a future
          // ledger or balance column needs > 15 significant digits, fetch it
          // with an explicit ::text cast at the call site.
          1700: (value: string): number => Number(value),
        },
      },
      ...config,
    });
  }

  get id(): string {
    return this.#id;
  }

  get pool(): Pool {
    if (!this.#poolIsValid()) {
      throw new Error("Pool is not initialized");
    }

    return this.#pool!;
  }

  get poolSize(): number {
    return this.#poolSize;
  }

  get poolConfig(): ClientOptions {
    return this.#poolConfig;
  }

  get connectionString(): string {
    return this.#connectionString;
  }
  /**
   * Get the pool instance (and not the client)
   *
   * @returns {Pool} The pool instance
   * @deprecated Use connect() instead
   * @example
   * ```ts
   * // deprecated
   * const pool = await connection.getClient();
   * const client = await pool.connect();
   * const result = await client.query("SELECT * FROM users");
   * await client.release();
   *
   * // new way
   * using client = await pool.connect();
   * const result = await client.query("SELECT * FROM users");
   * // client is a Disposable, so it will be released automatically.
   * ```
   */
  getClient(): Pool {
    if (!this.#poolIsValid()) {
      throw new Error("Pool is not initialized");
    }

    return this.#pool!;
  }

  // InitHookInterface mapping
  async init(): Promise<void> {
    await this.up();
  }

  // DestroyHookInterface mapping
  async destroy(): Promise<void> {
    await this.down();
  }

  // AsyncDisposable interface mapping
  [Symbol.asyncDispose](): Promise<void> {
    return this.down();
  }

  async up(): Promise<void> {
    if (++this.#connectionAttempts > 5) {
      throw new Error("Connection attempts exceeded");
    }

    logger.debug(`[pg] connecting to ${this.#poolConfig.database}`);

    if (await this.isReady()) {
      logger.debug(`[pg] already connected to ${this.#poolConfig.database}`);
      return;
    }

    this.#pool = new Pool(this.#poolConfig, this.#poolSize, this.#poolLazy);
    await this.query(sql`SELECT 1`);

    this.#connectionAttempts = 0;
    logger.debug(`[pg] connected to ${this.#poolConfig.database}`);
  }

  async down(): Promise<void> {
    this.#pool && await this.#pool.end();
    this.#pool = null;
    logger.debug(`[pg] disconnected from ${this.#poolConfig.database}`);
  }

  async isReady(): Promise<boolean> {
    if (!this.#poolIsValid()) {
      return false;
    }

    return await this.#pool!.initialized() > 0;
  }

  async query<TResult>(sql: Sql): Promise<TResult[]> {
    return this.#wrap<TResult>(sql, "object");
  }

  async queryArray<TResult>(sql: Sql): Promise<TResult[]> {
    return this.#wrap<TResult>(sql, "array");
  }

  async queryObject<TResult>(sql: Sql): Promise<TResult[]> {
    return this.#wrap<TResult>(sql, "object");
  }

  async raw<TResult>(sql: Sql): Promise<TResult[]> {
    return this.#wrap<TResult>(sql, "object", false);
  }

  async cursor<TResult>(sql: Sql): Promise<{
    read: (rowCount?: number) => AsyncGenerator<TResult[]>;
    [Symbol.asyncDispose]: () => Promise<void>;
  }> {
    // keep client alive for the lifetime of the cursor object
    let disposed = false;
    const client = await this.#pool!.connect();
    const cursorName = DenoPostgresConnection.id("cursor");

    try {
      await client.queryArray(`BEGIN`);
      logger.debug(`[pg] creating cursor ${cursorName}`);

      await client.queryArray(`DECLARE ${cursorName} CURSOR FOR ${sql.text}`, sql.values);

      const dispose = async () => {
        if (disposed) return;
        disposed = true;

        try {
          await client.queryArray(`CLOSE ${cursorName}`);
          await client.queryArray(`COMMIT`);
        } catch (_e) {
          try {
            await client.queryArray(`ROLLBACK`);
          } catch (_) {
            // swallow rollback error
          }
        } finally {
          client.release();
        }
      };

      return {
        [Symbol.asyncDispose]: dispose,
        read: async function* (rowCount: number = 100): AsyncGenerator<TResult[]> {
          try {
            let rows: TResult[] = [];
            do {
              ({ rows } = await client.queryObject<TResult>(`FETCH FORWARD ${rowCount} FROM ${cursorName}`));
              if (rows.length) yield rows;
            } while (rows.length);
          } finally {
            await dispose();
          }
        },
      };
    } catch (e) {
      try {
        await client.queryArray(`ROLLBACK`);
      } catch (_) {
        // ignore rollback failure
      } finally {
        client.release();
      }

      const message = e instanceof Error ? e.message : String(e);
      logger.error(`[pg] cursor error ${message}`);
      throw e;
    }
  }

  async #wrap<TResult>(sql: Sql, format: "array" | "object", transaction = true): Promise<TResult[]> {
    // Explicit Resource Management handles the disposal of the client
    using client = await this.#pool!.connect();

    try {
      transaction && await client.queryArray(`BEGIN`);

      const result = format === "object"
        ? await client.queryObject<TResult>({ text: sql.text, args: sql.values })
        : await client.queryArray({ text: sql.text, args: sql.values }); // FIXME type queryArray !

      transaction && await client.queryArray(`COMMIT`);
      return result.rows as TResult[];
    } catch (e) {
      transaction && await client.queryArray(`ROLLBACK`);
      e instanceof Error && logger.error("[pg] transaction error", e.message);
      throw e;
    }
  }

  #poolIsValid(): boolean {
    return this.#pool !== null && this.#pool instanceof Pool;
  }

  #configure(config: string | ClientOptions | undefined): { connectionString: string; config: ClientOptions } {
    switch (typeof config) {
      case "string":
        this.#connectionString = config;
        return { connectionString: config, config: {} };
      case "object":
      case "undefined": {
        const connectionString = env_or_default("APP_POSTGRES_URL", "");
        if (!connectionString.length) {
          throw new Error("APP_POSTGRES_URL is not set");
        }
        this.#connectionString = connectionString;
        return { connectionString, config: config || {} };
      }
      default:
        throw new Error("Invalid configuration");
    }
  }

  #validateConfig(config: ClientOptions): ClientOptions {
    // Define custom structs for string formats and numerical limits
    const AppName = refine(string(), "AppName", (value: string) => value.length <= 50);
    const Hostname = pattern(string(), /^[a-zA-Z0-9.-]+$/);
    const Database = pattern(string(), /^[a-zA-Z0-9_-]+$/);
    const Username = pattern(string(), /^[a-zA-Z0-9_-]+$/);
    const Port = refine(number(), "Port", (val: number) => val > 0 && val <= 65535);
    const Attempts = refine(number(), "Attempts", (i: number) => i === undefined || (i >= 1 && i <= 10));

    const ClientOptionsStruct = object({
      applicationName: AppName,
      connection: object({ attempts: Attempts }),
      hostname: Hostname,
      port: Port,
      database: Database,
      user: Username,
      password: string(),
      tls: object({
        enabled: boolean(),
        enforce: boolean(),
        caCertificates: array(string()),
      }),
      controls: object({
        debug: union([
          boolean(),
          object({
            queries: boolean(),
            notices: boolean(),
            results: boolean(),
            queryInError: boolean(),
          }),
        ]),
        decoders: object(),
      }),
    });

    assert(config, ClientOptionsStruct);

    return config;
  }
}
