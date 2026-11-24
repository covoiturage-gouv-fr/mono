import { assertEquals, assertNotEquals } from "dep:assert";
import express, { Request, Response } from "dep:express";
import { MemoryStore } from "dep:express-rate-limit";
import { describe, it } from "dep:testing-bdd";
import { rateLimiter, rateLimitKey } from "./rateLimiter.ts";

const fakeReq = (
  ip: string,
  stateless?: { user?: { operator_id?: unknown } },
) => ({ ip, stateless } as unknown as Request);

describe("rateLimitKey", () => {
  it("keys an operator token by operator_id", () => {
    assertEquals(rateLimitKey(fakeReq("203.0.113.9", { user: { operator_id: 42 } })), "operator:42");
  });

  it("separates two operators behind the same IPv6 /56", () => {
    const a = rateLimitKey(fakeReq("2001:db8:0:1::1", { user: { operator_id: 1 } }));
    const b = rateLimitKey(fakeReq("2001:db8:0:1::2", { user: { operator_id: 2 } }));
    assertNotEquals(a, b);
  });

  it("keeps the same operator key across IPs", () => {
    const a = rateLimitKey(fakeReq("203.0.113.9", { user: { operator_id: 5 } }));
    const b = rateLimitKey(fakeReq("2001:db8::1", { user: { operator_id: 5 } }));
    assertEquals(a, b);
  });

  it("falls back to the /56 IP key without identity", () => {
    const a = rateLimitKey(fakeReq("2001:db8:0:1::1"));
    assertEquals(a, rateLimitKey(fakeReq("2001:db8:0:1:ffff::2")));
    assertEquals(a, "2001:db8::/56");
  });

  it("returns the raw IP for IPv4", () => {
    assertEquals(rateLimitKey(fakeReq("203.0.113.9")), "203.0.113.9");
  });

  it("ignores a stateless user without operator_id", () => {
    assertEquals(rateLimitKey(fakeReq("203.0.113.9", { user: {} })), "203.0.113.9");
  });

  it("ignores a non-integer or negative operator_id", () => {
    assertEquals(rateLimitKey(fakeReq("203.0.113.9", { user: { operator_id: "1; DEL *" } })), "203.0.113.9");
    assertEquals(rateLimitKey(fakeReq("203.0.113.9", { user: { operator_id: -3 } })), "203.0.113.9");
  });
});

// Les limiteurs d'authentification passent avant toute identité : sans jeton ils restent par IP.
describe("rateLimiter keying", () => {
  it("counts per operator when a token identity is present, per IP otherwise", async () => {
    const app = express();
    app.set("trust proxy", 1);
    app.use((req: Request, _res: Response, next: () => void) => {
      const id = req.headers["x-op"];
      if (id) (req as unknown as { stateless: unknown }).stateless = { user: { operator_id: Number(id) } };
      next();
    });
    app.use(rateLimiter({ max: 1, store: new MemoryStore() }));
    app.get("/", (_req: Request, res: Response) => res.send("ok"));
    const server = app.listen(0);
    await new Promise((resolve) => server.once("listening", resolve));
    const { port } = server.address() as { port: number };
    const hit = async (op?: string) => {
      const res = await fetch(`http://127.0.0.1:${port}/`, {
        headers: { "x-forwarded-for": "2001:db8:0:1::1", ...(op ? { "x-op": op } : {}) },
      });
      await res.text();
      return res.status;
    };
    try {
      assertEquals([await hit("1"), await hit("2"), await hit("1"), await hit(), await hit()], [
        200,
        200,
        429,
        200,
        429,
      ]);
    } finally {
      await new Promise((resolve) => server.close(resolve));
    }
  });
});

// Serveur Express réel derrière un proxy de confiance : req.ip vient de X-Forwarded-For.
async function withServer(max: number, fn: (hit: (ip: string) => Promise<number>) => Promise<void>) {
  const app = express();
  app.set("trust proxy", 1);
  app.use(rateLimiter({ max, store: new MemoryStore() }));
  app.get("/", (_req: Request, res: Response) => res.send("ok"));
  const server = app.listen(0);
  await new Promise((resolve) => server.once("listening", resolve));
  const { port } = server.address() as { port: number };
  const hit = async (ip: string) => {
    const res = await fetch(`http://127.0.0.1:${port}/`, { headers: { "x-forwarded-for": ip } });
    await res.text();
    return res.status;
  };
  try {
    await fn(hit);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

describe("rateLimiter", () => {
  it("blocks the third request from the same IPv4 address", async () => {
    await withServer(2, async (hit) => {
      const codes = [await hit("203.0.113.9"), await hit("203.0.113.9"), await hit("203.0.113.9")];
      assertEquals(codes, [200, 200, 429]);
    });
  });

  // Un client IPv6 dispose d'un /64 entier : compter par /128 revient à ne rien limiter.
  it("buckets IPv6 addresses of the same /64 together", async () => {
    await withServer(2, async (hit) => {
      const codes = [await hit("2001:db8:0:1::1"), await hit("2001:db8:0:1::2"), await hit("2001:db8:0:1::3")];
      assertEquals(codes, [200, 200, 429]);
    });
  });
});
