import { assertEquals, assertRejects } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { REVOCATION_TTL_SECONDS, RevokedCredentialsRepository } from "./RevokedCredentialsRepository.ts";

function fakeRedis(execResult: [Error | null, unknown][] | null = []) {
  const store = new Map<string, number>();
  const client = {
    multi() {
      const queued: [string, number][] = [];
      return {
        set(key: string, _v: string, _ex: string, ttl: number) {
          queued.push([key, ttl]);
        },
        async exec() {
          for (const [k, ttl] of queued) store.set(k, ttl);
          return execResult;
        },
      };
    },
    async exists(key: string) {
      return store.has(key) ? 1 : 0;
    },
  };
  return { store, connection: { getClient: () => client } };
}

describe("RevokedCredentialsRepository", () => {
  it("marks revoked keys with a TTL and reports them", async () => {
    const { store, connection } = fakeRedis();
    // deno-lint-ignore no-explicit-any
    const repo = new RevokedCredentialsRepository(connection as any);

    await repo.revoke(["a", "b"]);

    assertEquals(store.get("auth:revoked:a"), REVOCATION_TTL_SECONDS);
    assertEquals(await repo.isRevoked("b"), true);
    assertEquals(await repo.isRevoked("c"), false);
  });

  it("does nothing on an empty list", async () => {
    const { store, connection } = fakeRedis();
    // deno-lint-ignore no-explicit-any
    await new RevokedCredentialsRepository(connection as any).revoke([]);
    assertEquals(store.size, 0);
  });

  it("throws when Redis rejects a command", async () => {
    const { connection } = fakeRedis([[new Error("OOM"), null]]);
    // deno-lint-ignore no-explicit-any
    const repo = new RevokedCredentialsRepository(connection as any);
    await assertRejects(() => repo.revoke(["a"]), Error, "OOM");
  });
});
