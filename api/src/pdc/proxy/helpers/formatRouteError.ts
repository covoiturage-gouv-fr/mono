import { errorCode, taggedLogger } from "@/lib/logger/tagged.ts";

const log = taggedLogger("[http]");

type RouteError = { httpCode?: number; rpcError?: { data?: unknown }; message?: string; stack?: string };

type TermsViolation = { terms_violation_labels: string[]; terms_violation_error_details?: unknown[] };

const isStringList = (v: unknown): v is string[] =>
  Array.isArray(v) && v.length > 0 && v.every((s) => typeof s === "string");

// Ne relaie du détail d'une erreur 4xx que des messages de violation,
// ou le motif de refus d'un trajet (contrat 422 de POST /journeys).
export function violationMessages(data: unknown): string | string[] | TermsViolation | undefined {
  if (typeof data === "string") return data;
  if (isStringList(data)) return data;
  const { terms_violation_labels, terms_violation_error_details } = (data ?? {}) as Partial<TermsViolation>;
  if (isStringList(terms_violation_labels)) {
    return Array.isArray(terms_violation_error_details)
      ? { terms_violation_labels, terms_violation_error_details }
      : { terms_violation_labels };
  }
  return undefined;
}

export function formatRouteError(e: unknown, rpcAnswerOnFailure: boolean): { status: number; body: unknown } {
  const err = (typeof e === "object" && e !== null ? e : {}) as RouteError;
  const status = err.httpCode || 500;

  if (status >= 500) {
    log.error("route failed", { status, error: errorCode(e) });
    const message = "Internal Server Error";
    return {
      status,
      body: rpcAnswerOnFailure
        ? { jsonrpc: "2.0", id: 1, error: { code: status, data: "Error", message } }
        : { error: message },
    };
  }

  const violations = violationMessages(err.rpcError?.data);

  if (rpcAnswerOnFailure) {
    const rpcError = err.rpcError ? { ...err.rpcError, data: violations } : { message: err.message };
    return { status, body: { jsonrpc: "2.0", id: 1, error: rpcError } };
  }
  return { status, body: violations ?? { error: err.message ?? "An unexpected error occurred." } };
}
