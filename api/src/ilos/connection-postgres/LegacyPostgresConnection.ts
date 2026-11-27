import { ConnectionInterface, DestroyHookInterface, InitHookInterface } from "@/ilos/common/index.ts";
import "./noDelay.ts";
import { env_or_fail, env_or_int } from "@/lib/env/index.ts";
import { errorCode, taggedLogger } from "@/lib/logger/tagged.ts";
import { v4 } from "@/lib/uuid/index.ts";
import type { PoolConfig } from "dep:pg";
import pg from "dep:pg";
import { Client } from "dep:postgres";

export type NativeCursor<T> = {
  read: (rowCount?: number) => Promise<T[]>;
  release: () => Promise<void>;
};

const log = taggedLogger("[pg]");

export enum PgPoolStatus {
  UP = "UP",
  DOWN = "DOWN",
  ERROR = "ERROR",
}

export class PgPool extends pg.Pool {
  protected _status: PgPoolStatus = PgPoolStatus.DOWN;

  constructor(config?: PoolConfig) {
    super(config);

    /**
     * @doc https://node-postgres.com/apis/pool#events
     *
     * acquire: Whenever the pool establishes a new client connection to the
     *          PostgreSQL backend it will emit the connect event with the
     *          newly connected client.
     * error:   When a client is sitting idly in the pool it can still emit
     *          errors because it is connected to a live backend.
     * release: Whenever a client is released back into the pool.
     * remove:  Whenever a client is closed & removed from the pool.
     */
    this.on("acquire", () => {
      this._status = PgPoolStatus.UP;
    });

    this.on("error", (err: Error) => {
      log.error("pool error", { error: errorCode(err) });
      this._status = PgPoolStatus.ERROR;
    });

    this.on("remove", () => {
      const { totalCount, idleCount, waitingCount } = this;
      log.debug("client removed", { total: totalCount, idle: idleCount, waiting: waitingCount });
    });
  }

  get status(): PgPoolStatus {
    return this._status;
  }

  get connected(): boolean {
    return this._status === PgPoolStatus.UP;
  }

  override async end(): Promise<void> {
    this._status = PgPoolStatus.DOWN;
    await super.end();
  }
}

/**
 * @deprecated replaced by DenoPostgresConnection
 */
export class LegacyPostgresConnection implements ConnectionInterface<PgPool>, InitHookInterface, DestroyHookInterface {
  private readonly pgUrl: string;
  private database: string = "";
  protected pool: PgPool;
  protected _status: PgPoolStatus = PgPoolStatus.DOWN;

  constructor(protected config: PoolConfig) {
    const timeout = env_or_int("APP_POSTGRES_TIMEOUT", 60000);
    this.pgUrl = config.connectionString || env_or_fail("APP_POSTGRES_URL");
    this.database = new URL(this.pgUrl)?.pathname || "";

    this.pool = new PgPool({
      ssl: this.hasSSL(this.pgUrl) ? { rejectUnauthorized: false } : false,
      statement_timeout: timeout,
      query_timeout: timeout,
      idle_in_transaction_session_timeout: timeout,
      ...config,
    });
  }

  /**
   * Helper to the pool status
   */
  get connected(): boolean {
    return this.pool.connected;
  }

  async init(): Promise<void> {
    await this.up();
  }

  async up(): Promise<void> {
    await this.pool.query("SELECT NOW()");
    log.debug("connected", { database: this.database });
  }

  async destroy(): Promise<void> {
    await this.down();
  }

  async down(): Promise<void> {
    log.debug("disconnecting", { database: this.database });
    this.pool.connected && await this.pool.end();
  }

  getClient(): PgPool {
    return this.pool;
  }

  async getNativeCursor<T>(text: string, values: unknown[]): Promise<NativeCursor<T>> {
    const cursor = `cursor-${v4()}`.replace(/[-]/g, "_");

    const client = new Client(this.pgUrl);
    await client.connect();
    await client.queryArray("BEGIN");
    await client.queryArray(`DECLARE ${cursor} CURSOR FOR ${text}`, values);

    return {
      read: async (rowCount: number = 100) => {
        const { rows } = await client.queryObject<T>(`FETCH FORWARD ${rowCount} FROM ${cursor}`);
        return rows;
      },
      release: async () => {
        await client.queryArray(`CLOSE ${cursor}`);
        await client.queryArray("COMMIT");
        await client.end();
      },
    };
  }

  // https://www.postgresql.org/docs/current/libpq-ssl.html
  private hasSSL(strUrl: string): boolean {
    const url = new URL(strUrl);
    if (!url.searchParams.has("sslmode")) return false;
    return url.searchParams.get("sslmode") !== "disable";
  }
}
