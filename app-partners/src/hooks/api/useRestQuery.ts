import { getApiUrl } from "@/helpers/api";
import { useApi } from "@/hooks/useApi";
import { useMemo } from "react";

type QueryParams = Record<string, string | number | boolean | undefined>;

interface RestQueryOptions {
  method?: "GET" | "POST";
  paginate?: boolean;
}

/**
 * Base hook for REST API calls.
 * - GET: query params appended to URL
 * - POST: params sent in JSON body
 */
export function useRestQuery<T>(
  version: string,
  path: string,
  params?: QueryParams,
  options: RestQueryOptions = {},
  deps: unknown[] = [],
) {
  const { method = "GET", paginate = false } = options;
  const url = useMemo(() => {
    const baseUrl = getApiUrl(version, path);

    console.log(baseUrl);
    console.log(params);
    console.log(method);
    // For POST, don't append query params to URL
    if (method === "POST" || !params) return baseUrl;

    const searchParams = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined) {
        searchParams.append(key, String(value));
      }
    });

    const queryString = searchParams.toString();
    return queryString ? `${baseUrl}?${queryString}` : baseUrl;
  }, deps);

  const init = useMemo(() => {
    if (method !== "POST") return undefined;

    return {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(params ?? {}),
    };
  }, deps);

  return useApi<T>(url, paginate, init);
}
