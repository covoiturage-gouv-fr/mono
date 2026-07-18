import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { env_or_default } from "@/lib/env/index.ts";
import { SESSION_KEY_PREFIX } from "@/pdc/proxy/middlewares/sessionMiddleware.ts";
import { Redis } from "dep:redis";
import { SessionRepository } from "./SessionRepository.ts";

describe("SessionRepository.destroyByUser", () => {
  const redisUrl = env_or_default("APP_REDIS_URL", "redis://127.0.0.1:6379");
  let client: Redis;
  let repository: SessionRepository;

  // Config minimale : SessionRepository ne lit que connections.redis.
  const config = { get: (_key: string) => redisUrl } as unknown as ConstructorParameters<typeof SessionRepository>[0];

  const key = (sid: string) => `${SESSION_KEY_PREFIX}${sid}`;

  beforeAll(async () => {
    client = new Redis(redisUrl);
    repository = new SessionRepository(config);
    await client.set(key("sid-a"), JSON.stringify({ cookie: {}, user: { _id: 4242 } }));
    await client.set(key("sid-b"), JSON.stringify({ cookie: {}, user: { _id: 4242 } }));
    await client.set(key("sid-c"), JSON.stringify({ cookie: {}, user: { _id: 9999 } }));
  });

  afterAll(async () => {
    await client.del(key("sid-a"), key("sid-b"), key("sid-c"));
    await client.quit();
  });

  it("destroys every session of the target user, keeps others", async () => {
    await repository.destroyByUser(4242);

    assertEquals(await client.exists(key("sid-a")), 0);
    assertEquals(await client.exists(key("sid-b")), 0);
    assertEquals(await client.exists(key("sid-c")), 1); // autre user intact
  });
});
