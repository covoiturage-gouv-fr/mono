import { assertEquals } from "dep:assert";
import express, { NextFunction, Request, Response } from "dep:express";
import { describe, it } from "dep:testing-bdd";
import { AccessLogFields, accessLogMiddleware } from "./accessLogMiddleware.ts";

type Hit = (path: string, init?: RequestInit) => Promise<AccessLogFields>;
type Get = (path: string, init?: RequestInit) => Promise<void>;

// Serveur Express réel derrière un proxy de confiance, comme en production.
async function withServer(fn: (hit: Hit, lines: AccessLogFields[], get: Get) => Promise<void>) {
  const lines: AccessLogFields[] = [];
  let logged = () => {};
  const app = express();
  app.set("trust proxy", 1);
  app.use(accessLogMiddleware((fields) => {
    lines.push(fields);
    logged();
  }));
  app.use(express.json());
  app.use((req: Request, _res: Response, next: NextFunction) => {
    const user = { _id: 12, role: "territory.admin", email: "jane@example.org" };
    if (req.headers["x-test-auth"] === "session") Object.assign(req, { session: { user } });
    if (req.headers["x-test-auth"] === "token") {
      Object.assign(req, { stateless: { user: { operator_id: 9, role: "operator.application", email: "key-id" } } });
    }
    next();
  });
  app.get("/health", (req: Request, res: Response) => res.status(req.headers["x-test-fail"] ? 503 : 200).send("ok"));
  app.get("/metrics", (_req: Request, res: Response) => res.send("ok"));
  app.post("/:api_version/journeys/:id", (_req: Request, res: Response) => res.status(201).json({ ok: true }));
  app.get("/slow", (_req: Request, res: Response) => setTimeout(() => res.send("ok"), 30));

  const server = app.listen(0);
  await new Promise((resolve) => server.once("listening", resolve));
  const { port } = server.address() as { port: number };
  const get: Get = async (path, init = {}) => {
    await (await fetch(`http://127.0.0.1:${port}${path}`, init)).text();
  };
  const hit: Hit = async (path, init = {}) => {
    const done = new Promise<void>((resolve) => (logged = resolve));
    await get(path, init);
    await done;
    return lines.at(-1)!;
  };
  try {
    await fn(hit, lines, get);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

describe("accessLogMiddleware", () => {
  it("logs method, matched route, status and duration when the response finishes", async () => {
    await withServer(async (hit) => {
      const line = await hit("/v3/journeys/abc-123?token=secret", { method: "POST" });
      assertEquals(line.method, "POST");
      assertEquals(line.route, "/:api_version/journeys/:id");
      assertEquals(line.status, 201);
      assertEquals(Number.isInteger(line.duration_ms), true);

      const slow = await hit("/slow");
      assertEquals(slow.duration_ms >= 25, true);
    });
  });

  it("takes the IP added by the trusted proxy, not a forged left-most one", async () => {
    await withServer(async (hit) => {
      const line = await hit("/slow", { headers: { "x-forwarded-for": "10.0.0.1, 203.0.113.9" } });
      assertEquals(line.ip, "203.0.113.9");
    });
  });

  it("ignores a forged X-Real-IP", async () => {
    await withServer(async (hit) => {
      const line = await hit("/slow", { headers: { "x-real-ip": "10.0.0.1", "x-forwarded-for": "203.0.113.9" } });
      assertEquals(line.ip, "203.0.113.9");
    });
  });

  it("keeps the request id sent by the proxy, and no key without it", async () => {
    await withServer(async (hit) => {
      const id = "5f2a9c0e7b1d4e3f8a6b5c4d3e2f1a0b";
      assertEquals((await hit("/slow", { headers: { "x-request-id": id } })).request_id, id);
      assertEquals("request_id" in (await hit("/slow")), false);
    });
  });

  it("bounds and filters the logged header values", async () => {
    await withServer(async (hit) => {
      const line = await hit("/slow", {
        headers: { "x-request-id": "a".repeat(200), "x-pentest-id": 'EY JD","status":200,"x":"' },
      });
      assertEquals(line.request_id, "a".repeat(64));
      assertEquals(line.pentest_id, "EY_JD___status_:200__x_:_");
    });
  });

  it("has no pentest_id key without the header", async () => {
    await withServer(async (hit) => {
      assertEquals("pentest_id" in (await hit("/slow")), false);
      assertEquals((await hit("/slow", { headers: { "x-pentest-id": "EY-JD" } })).pentest_id, "EY-JD");
    });
  });

  it("logs sub and role of a session user or of an operator token, nothing when anonymous", async () => {
    await withServer(async (hit) => {
      const session = await hit("/slow", { headers: { "x-test-auth": "session" } });
      assertEquals([session.sub, session.role], ["user:12", "territory.admin"]);

      const token = await hit("/slow", { headers: { "x-test-auth": "token" } });
      assertEquals([token.sub, token.role], ["operator:9", "operator.application"]);

      const anonymous = await hit("/slow");
      assertEquals("sub" in anonymous || "role" in anonymous, false);
    });
  });

  it("never logs query string, body, cookie, token or email", async () => {
    await withServer(async (hit) => {
      const line = await hit("/v3/journeys/abc-123?token=qs-secret", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "authorization": "Bearer bearer-secret",
          "cookie": "sid=cookie-secret",
          "x-test-auth": "session",
        },
        body: JSON.stringify({ password: "body-secret" }),
      });
      const raw = JSON.stringify(line);
      for (const secret of ["qs-secret", "bearer-secret", "cookie-secret", "body-secret", "abc-123", "example.org"]) {
        assertEquals(raw.includes(secret), false, secret);
      }
      assertEquals(
        Object.keys(line).sort(),
        ["duration_ms", "ip", "method", "role", "route", "status", "sub"],
      );
    });
  });

  it("logs unmatched requests without their raw path", async () => {
    await withServer(async (hit) => {
      const line = await hit("/nowhere/abc-123?x=1");
      assertEquals(line.status, 404);
      assertEquals("route" in line, false);
    });
  });

  it("skips successful probes but keeps their failures", async () => {
    await withServer(async (hit, lines, get) => {
      await get("/health");
      await get("/metrics");
      const failed = await hit("/health", { headers: { "x-test-fail": "1" } });
      assertEquals([failed.route, failed.status], ["/health", 503]);
      assertEquals(lines.length, 1);
    });
  });
});
