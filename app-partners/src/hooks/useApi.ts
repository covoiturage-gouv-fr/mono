import { useCallback, useEffect, useState } from "react";

interface PaginateAPIResponse<T> {
  meta?: { totalPages: number };
  data?: T[];
}

interface ErrorResponse {
  message: string;
}

type ApiResponse<T> = PaginateAPIResponse<T> | T | ErrorResponse;

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
      const res = (await response.json()) as ApiResponse<T>;
      if (!response.ok) {
        throw new Error(
          (res as ErrorResponse).message ?? "Une erreur est survenue",
        );
      }

      if (
        paginate &&
        ((res as PaginateAPIResponse<T>).meta?.totalPages ?? 0) > 1
      ) {
        const paginateResponse = res as PaginateAPIResponse<T>;

        setData({
          meta: paginateResponse.meta,
          data: paginateResponse.data,
        } as T);
      } else {
        setData(res as T);
      }
    } catch (e) {
      setError(e as Error);
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
