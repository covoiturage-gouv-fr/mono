import { env_or_fail, env_or_false } from "@/lib/env/index.ts";
import { LegacyMigrator } from "../pdc/providers/migration/LegacyMigrator.ts";

/**
 * Migrate command.
 *
 * Run SQL migrations from the migrations folder.
 */
const migrator = new LegacyMigrator(env_or_fail("APP_POSTGRES_URL"), false);
await migrator.up();
await migrator.migrate({
  skip: env_or_false("MIGRATIONS_SKIP_ALL"),
});
await migrator.down();
