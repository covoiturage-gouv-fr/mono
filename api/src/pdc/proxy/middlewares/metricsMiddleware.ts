import { safeCompare } from "@/lib/crypto/safeCompare.ts";
import { env } from "@/lib/env/index.ts";
import { getHostName } from "@/lib/net/index.ts";
import { NextFunction, Request, Response } from "dep:express";
import { rateLimiter } from "./rateLimiter.ts";

export function metricsMiddleware(endpoint: string) {
  const rl = rateLimiter(
    { windowMs: 60 * 1000 * 5, max: 100 },
    `rate-${endpoint}-${getHostName()}`,
  );
  const token = env("APP_MONITORING_TOKEN");
  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    if (token && safeCompare(String(req.headers["x-monitoring-token"] ?? ""), token)) {
      return next();
    }
    return rl(req, res, next);
  };
}
