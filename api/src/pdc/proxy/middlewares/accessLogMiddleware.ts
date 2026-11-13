import { logger } from "@/lib/logger/index.ts";
import { safeValue } from "@/lib/logger/tagged.ts";
import { NextFunction, Request, Response } from "dep:express";

export type AccessLogFields = {
  ip?: string;
  request_id?: string;
  pentest_id?: string;
  method: string;
  route?: string;
  status: number;
  duration_ms: number;
  sub?: string;
  role?: string;
};

// Sondes k8s et Prometheus : plusieurs appels par minute et par pod, sans intérêt tant qu'elles réussissent.
const PROBE_ROUTES = new Set(["/health", "/metrics"]);

// En-tête fourni par l'appelant : borné et filtré avant d'entrer dans le journal.
function header(req: Request, name: string): string | undefined {
  const value = req.headers[name];
  return value === undefined || value === "" ? undefined : safeValue(String(value));
}

function actor(req: Request): Pick<AccessLogFields, "sub" | "role"> {
  const session = req.session?.user;
  if (session) return { sub: `user:${session._id}`, role: safeValue(session.role) };
  const token = req.stateless?.user;
  if (token) return { sub: `operator:${token.operator_id}`, role: safeValue(token.role) };
  return {};
}

function write(fields: AccessLogFields): void {
  logger.info("[access]", fields);
}

export function accessLogMiddleware(log: (fields: AccessLogFields) => void = write) {
  return (req: Request, res: Response, next: NextFunction): void => {
    const start = performance.now();

    res.on("finish", () => {
      // Motif de la route, jamais l'URL : pas d'identifiant ni de query string dans le journal.
      const route = req.route ? `${req.baseUrl}${req.route.path}` : undefined;
      if (route && PROBE_ROUTES.has(route) && res.statusCode < 400) return;

      try {
        const fields: AccessLogFields = {
          // req.ip : même adresse que le limiteur de débit (dernier saut de confiance, non falsifiable).
          ip: req.ip,
          request_id: header(req, "x-request-id"),
          pentest_id: header(req, "x-pentest-id"),
          method: req.method,
          route,
          status: res.statusCode,
          duration_ms: Math.round(performance.now() - start),
          ...actor(req),
        };
        // Champ absent plutôt que vide : pas de clé pentest_id hors pentest.
        log(Object.fromEntries(Object.entries(fields).filter(([, v]) => v !== undefined)) as AccessLogFields);
      } catch {
        // le journal d'accès ne doit jamais faire échouer une requête
      }
    });

    next();
  };
}
