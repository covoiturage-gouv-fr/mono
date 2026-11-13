import { useCallback, useEffect, useRef, useState } from "react";

interface PaginateAPIResponse<T> {
  meta?: { totalPages: number };
  data?: T[];
}

type ApiResponse<T> = PaginateAPIResponse<T> | T;

export const UNREACHABLE_MESSAGE = "Le service est momentanément injoignable. Réessayez dans quelques instants.";

// Session perdue (401) : AuthProvider écoute cet événement et revérifie /auth/me.
export const AUTH_UNAUTHORIZED_EVENT = "auth:unauthorized";

export const signalUnauthorized = () => {
  if (typeof window === "undefined") return;
  window.dispatchEvent(new Event(AUTH_UNAUTHORIZED_EVENT));
};

// fetch rejette un TypeError anglophone quand le serveur ne répond pas : on parle français à l'utilisateur.
export const toUserError = (e: unknown): Error =>
  e instanceof TypeError ? new Error(UNREACHABLE_MESSAGE) : e instanceof Error ? e : new Error(String(e));

// L'API répond tantôt { message }, tantôt un tableau de violations, tantôt une chaîne nue ou rien.
export const apiErrorMessage = (status: number, body: unknown): string => {
  const fromBody = (): string | undefined => {
    // Relais borné : seules des chaînes courtes, jamais un objet imprévu.
    if (typeof body === "string" && body.trim()) return body.slice(0, 500);
    if (Array.isArray(body) && body.every((v) => typeof v === "string")) return body.join("\n").slice(0, 500);
    if (body && typeof body === "object") {
      const { message, error } = body as { message?: unknown; error?: unknown };
      if (typeof message === "string" && message) return message;
      if (typeof error === "string" && error && status < 500) return error;
    }
    return undefined;
  };
  switch (status) {
    case 401:
      return "Votre session a expiré, reconnectez-vous.";
    case 403:
      return "Vous n'avez pas les droits nécessaires pour cette action.";
    case 404:
      return "Élément introuvable.";
    default:
      if (status >= 500) return `Le service a rencontré une erreur interne (${status}). Réessayez plus tard.`;
      return fromBody() ?? `La requête a été refusée (${status}).`;
  }
};

export const parseBody = (text: string): unknown => {
  if (!text) return undefined;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
};

export const useApi = <T>(url: string | URL, paginate = false, init?: RequestInit, reloadDependency?: unknown) => {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  // Seule la dernière requête écrit l'état : une réponse plus lente d'une URL précédente est ignorée.
  const lastRequest = useRef(0);
  const fetchData = useCallback(async () => {
    const request = ++lastRequest.current;
    const isStale = () => request !== lastRequest.current;
    try {
      setError(null);
      setLoading(true);
      const response = await fetch(url, { ...init, credentials: "include" });
      const text = await response.text();
      if (isStale()) return;
      const parsed = parseBody(text);
      if (!response.ok) {
        if (response.status === 401) signalUnauthorized();
        throw new Error(apiErrorMessage(response.status, parsed));
      }
      const res = (typeof parsed === "object" ? parsed : null) as ApiResponse<T> | null;

      if (paginate && ((res as PaginateAPIResponse<T> | null)?.meta?.totalPages ?? 0) > 1) {
        const paginateResponse = res as PaginateAPIResponse<T>;

        setData({
          meta: paginateResponse.meta,
          data: paginateResponse.data,
        } as T);
      } else {
        setData((res ?? undefined) as T | undefined);
      }
    } catch (e) {
      if (isStale()) return;
      setError(toUserError(e));
      setData(undefined);
    } finally {
      if (!isStale()) setLoading(false);
    }
  }, [url, init, paginate]);
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- chargement de données : loading/error remis à zéro avant la requête
    void fetchData();
  }, [fetchData, reloadDependency]);
  return { data, error, loading, refetch: fetchData };
};
