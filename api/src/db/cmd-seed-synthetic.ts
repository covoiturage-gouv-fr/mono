import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { env_or_fail } from "@/lib/env/index.ts";
import { assertEnvAllowed } from "./synthetic/guard.ts";
import { parseReferenceDate } from "./synthetic/profile.ts";
import { getProfile } from "./synthetic/profiles/index.ts";
import { SyntheticSeeder } from "./synthetic/SyntheticSeeder.ts";

/**
 * Synthetic dataset command.
 *
 * Usage: cmd-seed-synthetic.ts <profile> [YYYY-MM-DD]
 * Fills an already migrated database (with geo.perimeters loaded) from `synthetic/profiles`.
 */
const [profileName, dateArg] = Deno.args;
const profile = getProfile(profileName);
assertEnvAllowed(Deno.env.get("APP_ENV"), profile.allowedEnvs);
const referenceDate = parseReferenceDate(dateArg ?? profile.referenceDate);

const connection = new DenoPostgresConnection(env_or_fail("APP_POSTGRES_URL"));
await connection.up();
try {
  const report = await new SyntheticSeeder(connection, profile, referenceDate).run();
  console.log(
    JSON.stringify({ profile: profile.name, referenceDate: referenceDate.toISOString(), ...report }, null, 2),
  );
} finally {
  await connection.down();
}
