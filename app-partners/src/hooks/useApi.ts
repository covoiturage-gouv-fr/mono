import { useCallback, useEffect, useState } from "react";

interface PaginateAPIResponse<T> {
  meta?: { totalPages: number };
  data?: T[];
}

interface ErrorResponse {
  message: string;
}

type ApiResponse<T> = PaginateAPIResponse<T> | T | ErrorResponse;

export const UNREACHABLE_MESSAGE = "Le service est momentanément injoignable. Réessayez dans quelques instants.";

// fetch rejette un TypeError anglophone quand le serveur ne répond pas : on parle français à l'utilisateur.
export const toUserError = (e: unknown): Error =>
  e instanceof TypeError ? new Error(UNREACHABLE_MESSAGE) : e instanceof Error ? e : new Error(String(e));

export const useApi = <T>(
  url: string | URL,
  paginate = false,
  init?: RequestInit,
  reloadDependency?: unknown,
) => {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const fetchData = useCallback(async () => {
    try {
      setError(null);
      setLoading(true);
      const response = await fetch(url, { ...init, credentials: "include" });
      const text = await response.text();
      let res: ApiResponse<T> | null = null;
      if (text.length > 0) {
        try {
          res = JSON.parse(text) as ApiResponse<T>;
        } catch {
          if (!response.ok) {
            throw new Error(
              response.statusText || `Erreur ${response.status}`,
            );
          }
          res = null;
        }
      }
      if (!response.ok) {
        throw new Error(
          (res as ErrorResponse | null)?.message ??
            response.statusText ??
            "Une erreur est survenue",
        );
      }

      if (
        paginate &&
        ((res as PaginateAPIResponse<T> | null)?.meta?.totalPages ?? 0) > 1
      ) {
        const paginateResponse = res as PaginateAPIResponse<T>;

        setData({
          meta: paginateResponse.meta,
          data: paginateResponse.data,
        } as T);
      } else {
        setData((res ?? undefined) as T | undefined);
      }
    } catch (e) {
      setError(toUserError(e));
      setData(undefined);
    } finally {
      setLoading(false);
    }
  }, [url, init, paginate]);
  useEffect(() => {
    void fetchData();
  }, [fetchData, reloadDependency]);
  return { data, error, loading, refetch: fetchData };
};
