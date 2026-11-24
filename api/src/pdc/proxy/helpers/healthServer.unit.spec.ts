import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { HealthServerOptions, startHealthServer } from "./healthServer.ts";

type Reply = { status: number; body: string };

async function withHealthServer(
  opts: Partial<HealthServerOptions>,
  fn: (get: (path: string, init?: RequestInit) => Promise<Reply>) => Promise<void>,
) {
  const server = await startHealthServer(0, {
    checkReady: () => Promise.resolve(),
    isShuttingDown: () => false,
    ...opts,
  });
  const { port } = server.address() as { port: number };
  const get = async (path: string, init: RequestInit = {}) => {
    const res = await fetch(`http://127.0.0.1:${port}${path}`, init);
    return { status: res.status, body: await res.text() };
  };
  try {
    await fn(get);
  } finally {
    await new Promise((resolve) => {
      server.close(resolve);
      server.closeAllConnections();
    });
  }
}

const OK = JSON.stringify({ status: "ok" });
const ERROR = JSON.stringify({ status: "error" });

describe("healthServer", () => {
  it("live and ready answer 200 when healthy", async () => {
    await withHealthServer({}, async (get) => {
      assertEquals(await get("/health/live"), { status: 200, body: OK });
      assertEquals(await get("/health/ready"), { status: 200, body: OK });
    });
  });

  it("ready answers 503 after SIGTERM, live stays 200", async () => {
    await withHealthServer({ isShuttingDown: () => true }, async (get) => {
      assertEquals(await get("/health/ready"), { status: 503, body: ERROR });
      assertEquals(await get("/health/live"), { status: 200, body: OK });
    });
  });

  it("ready answers 503 without detail when the database fails, live stays 200", async () => {
    let calls = 0;
    const checkReady = () => {
      calls++;
      return Promise.reject(new Error("connect ECONNREFUSED 10.0.0.1:5432"));
    };
    await withHealthServer({ checkReady }, async (get) => {
      assertEquals(await get("/health/ready"), { status: 503, body: ERROR });
      assertEquals(await get("/health/live"), { status: 200, body: OK });
      assertEquals(calls, 1);
    });
  });

  it("ready answers 503 within the timeout when the database hangs", async () => {
    const checkReady = () => new Promise<void>(() => {});
    await withHealthServer({ checkReady, timeoutMs: 50 }, async (get) => {
      const start = performance.now();
      assertEquals(await get("/health/ready"), { status: 503, body: ERROR });
      assertEquals(performance.now() - start < 500, true);
    });
  });

  it("any other path or method answers 404", async () => {
    await withHealthServer({}, async (get) => {
      assertEquals((await get("/health")).status, 404);
      assertEquals((await get("/metrics")).status, 404);
      assertEquals((await get("/health/live/x")).status, 404);
      assertEquals((await get("/health/ready?x=1")).status, 200);
      assertEquals((await get("/health/live", { method: "POST" })).status, 404);
    });
  });

  it("is not rate limited", async () => {
    await withHealthServer({}, async (get) => {
      for (let i = 0; i < 200; i++) {
        assertEquals((await get(i % 2 ? "/health/live" : "/health/ready")).status, 200);
      }
    });
  });
});
