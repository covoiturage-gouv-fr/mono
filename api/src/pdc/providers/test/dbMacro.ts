import { DenoPostgresConnection, LegacyPostgresConnection } from "../../../ilos/connection-postgres/index.ts";
import { env_or_fail, env_or_false } from "../../../lib/env/index.ts";
import { logger } from "../../../lib/logger/index.ts";
import { DenoMigrator, LegacyMigrator } from "../migration/index.ts";

interface Config {
  connectionString: string;
}

/**
 * @deprecated replaced by DenoDbContext
 */
export interface LegacyDbContext {
  db: LegacyMigrator;
  connection: LegacyPostgresConnection;
}

/**
 * @deprecated replaced by DenoDbBeforeAfter
 */
export interface LegacyDbBeforeAfter {
  before(): Promise<LegacyDbContext>;
  after(cfg: LegacyDbContext): Promise<void>;
}

/**
 * Export helpers to create a clean up a test database before and after tests
 *
 * You can pass a connectionString in the config to override the default
 * APP_POSTGRES_URL environment variable.
 *
 * @example
 * import { DenoDbContext, makeLegacyDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";
 *
 * const { before, after } = makeLegacyDbBeforeAfter();
 * let db: DenoDbContext;
 *
 * beforeAll(async () => {
 *    db = await before();
 * });
 * afterAll(async () => {
 *   await after(db);
 * });
 *
 * @param {Config} cfg
 * @deprecated replaced by makeDenoDbBeforeAfter
 */
export function makeLegacyDbBeforeAfter(cfg?: Config): LegacyDbBeforeAfter {
  return {
    before: async (): Promise<LegacyDbContext> => {
      const connectionString = cfg?.connectionString || env_or_fail("APP_POSTGRES_URL");
      const db = new LegacyMigrator(connectionString);
      await db.create();
      await db.migrate({ flash: false, verbose: false });
      await db.seed();

      return { db, connection: db.testConn };
    },
    after: async (ctx: LegacyDbContext): Promise<void> => {
      // Test databases can be kept for inspection by setting the env var
      // APP_POSTGRES_KEEP_TEST_DATABASES to 'true'
      // use `just drop_test_databases` in your shell to clear them.
      if (
        env_or_false("APP_POSTGRES_KEEP_TEST_DATABASES")
      ) {
        logger.info(
          `[db-macro] Keeping the test database: ${
            ctx?.db?.dbName || "undefined"
          } run 'just drop_test_databases to clear'`,
        );
      } else {
        await ctx.db.drop();
      }

      ctx && ctx.db && await ctx.db.down();
    },
  };
}

export interface DenoDbContext {
  db: DenoMigrator;
  connection: DenoPostgresConnection;
}

export interface DenoDbBeforeAfter {
  before(): Promise<DenoDbContext>;
  after(cfg: DenoDbContext): Promise<void>;
}

export function makeDenoDbBeforeAfter(cfg?: Config): DenoDbBeforeAfter {
  return {
    before: async (): Promise<DenoDbContext> => {
      const connectionString = cfg?.connectionString || env_or_fail("APP_POSTGRES_URL");
      const mig = new DenoMigrator(connectionString);
      await mig.create();
      await mig.migrate({ flash: false, verbose: false });
      await mig.seed();

      return { db: mig, connection: mig.testConn };
    },
    after: async (ctx: DenoDbContext): Promise<void> => {
      // Test databases can be kept for inspection by setting the env var
      // APP_POSTGRES_KEEP_TEST_DATABASES to 'true'
      // use `just drop_test_databases` in your shell to clear them.
      if (
        env_or_false("APP_POSTGRES_KEEP_TEST_DATABASES")
      ) {
        logger.info(
          `[db-macro] Keeping the test database: ${
            ctx?.db?.dbName || "undefined"
          } run 'just drop_test_databases to clear'`,
        );
      } else {
        await ctx.db.drop();
      }

      ctx && ctx.db && await ctx.db.down();
    },
  };
}
