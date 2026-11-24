import { createServer, IncomingMessage, Server, ServerResponse } from "dep:http";
import { errorCode, taggedLogger } from "@/lib/logger/tagged.ts";

const log = taggedLogger("[health]");

export type HealthServerOptions = {
  checkReady: () => Promise<unknown>;
  isShuttingDown: () => boolean;
  timeoutMs?: number;
};

function reply(res: ServerResponse, status: number, ok: boolean): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ status: ok ? "ok" : "error" }));
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(Object.assign(new Error("timeout"), { code: "TIMEOUT" })), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

// Port interne des sondes Kubernetes : aucun middleware de l'application, ni Redis, ni limite de débit.
export function startHealthServer(port: number, opts: HealthServerOptions): Promise<Server> {
  const { checkReady, isShuttingDown, timeoutMs = 500 } = opts;

  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    const path = (req.url ?? "").split("?")[0];
    if (req.method !== "GET") return reply(res, 404, false);

    if (path === "/health/live") return reply(res, 200, true);

    if (path === "/health/ready") {
      if (isShuttingDown()) return reply(res, 503, false);
      try {
        await withTimeout(checkReady(), timeoutMs);
        return reply(res, 200, true);
      } catch (err) {
        log.error("readiness failed", { error: errorCode(err) });
        return reply(res, 503, false);
      }
    }

    return reply(res, 404, false);
  });

  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, () => {
      server.off("error", reject);
      log.info("listening", { port });
      resolve(server);
    });
  });
}
