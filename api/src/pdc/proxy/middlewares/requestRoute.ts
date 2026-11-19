import { safeValue } from "@/lib/logger/tagged.ts";
import { Request } from "dep:express";

// operator_journey_id fait jusqu'à 256 caractères : 64 tronquerait /v3.x/journeys/:id/cancel.
const MAX_PATH_LENGTH = 512;

// originalUrl n'est jamais réécrite par les sous-routeurs (baseUrl/path le sont) ; chemin fourni par l'appelant : filtré et borné.
export function requestPath(req: Request): string | undefined {
  const path = req.originalUrl?.split("?")[0];
  return path ? safeValue(path, { maxLength: MAX_PATH_LENGTH }) : undefined;
}

export function routePattern(req: Request): string | undefined {
  return req.route ? `${req.baseUrl}${req.route.path}` : undefined;
}

// req.params est vidé quand une erreur remonte au gestionnaire global : relire la version dans le chemin réel.
export function routeApiVersion(req: Request): string | undefined {
  if (!routePattern(req)?.startsWith("/:api_version/")) return undefined;
  return req.originalUrl?.split("?")[0].split("/")[1] || undefined;
}
