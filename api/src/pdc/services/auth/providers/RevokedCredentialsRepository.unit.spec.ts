import { assertEquals, assertRejects } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { REVOCATION_TTL_SECONDS, RevokedCredentialsRepository } from "./RevokedCredentialsRepository.ts";

// key -> TTL in seconds, null = no expiry
function fakeRedis(execResult: [Error | null, unknown][] | null = []) {
  const store = new Map<string, number | null>();
  const client = {
    multi() {
      const queued: (() => void)[] = [];
      return {
        set(key: string) {
          queued.push(() => store.set(key, null));
        },
        expire(key: string, ttl: number) {
          queued.push(() => store.has(key) && store.set(key, ttl));
        },
        async exec() {
          queued.forEach((run) => run());
          return execResult;
        },
      };
    },
    async exists(key: string) {
      return store.has(key) ? 1 : 0;
    },
  };
  // deno-lint-ignore no-explicit-any
  return { store, repo: new RevokedCredentialsRepository({ getClient: () => client } as any) };
}

describe("RevokedCredentialsRepository", () => {
  it("marks revoked keys without expiry, then expires them", async () => {
    const { store, repo } = fakeRedis();

    await repo.revoke(["a", "b"]);
    assertEquals(store.get("auth:revoked:a"), null);
    assertEquals(await repo.isRevoked("b"), true);
    assertEquals(await repo.isRevoked("c"), false);

    await repo.expire(["a"]);
    assertEquals(store.get("auth:revoked:a"), REVOCATION_TTL_SECONDS);
    assertEquals(store.get("auth:revoked:b"), null);
  });

  it("does nothing on an empty list", async () => {
    const { store, repo } = fakeRedis();
    await repo.revoke([]);
    assertEquals(store.size, 0);
  });

  it("throws when Redis rejects a command", async () => {
    const { repo } = fakeRedis([[new Error("OOM"), null]]);
    await assertRejects(() => repo.revoke(["a"]), Error, "OOM");
  });
});
