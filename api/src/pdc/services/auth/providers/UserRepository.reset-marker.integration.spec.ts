import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { UserRepository } from "./UserRepository.ts";

describe("UserRepository reset deletion marker on login", () => {
  let db: DenoDbContext;
  let repository: UserRepository;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new UserRepository(db.connection);
    await db.connection.query(
      sql`INSERT INTO auth.users (email, firstname, lastname, role, last_login_at, deletion_warned_at)
      VALUES ('login@reset.test', 'L', 'L', 'operator.user', now() - '13 months'::interval, now() - '2 days'::interval)`,
    );
  });
  afterAll(async () => {
    await after(db);
  });

  it("clears deletion_warned_at and refreshes last_login_at", async () => {
    await repository.authenticateByEmail("login@reset.test");
    const rows = await db.connection.query<{ deletion_warned_at: string | null; recent: boolean }>(sql`
      SELECT deletion_warned_at, last_login_at > now() - '1 minute'::interval AS recent
      FROM auth.users WHERE email = 'login@reset.test'`);
    assertEquals(rows[0].deletion_warned_at, null);
    assertEquals(rows[0].recent, true);
  });
});
