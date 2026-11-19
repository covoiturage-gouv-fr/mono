import { useEffect, useState } from "react";

// 4xx : seul un `detail`/`message` texte d'un corps JSON est relayé (borné) ; le reste vient d'un proxy.
export const toErrorMessage = (status: number, text: string): string => {
  try {
    const { detail, message } = (JSON.parse(text) ?? {}) as { detail?: unknown; message?: unknown };
    const relayed = [detail, message].find((v): v is string => typeof v === "string" && v !== "");
    if (relayed) return relayed.slice(0, 500);
  } catch {
    // corps non JSON
  }
  return `La requête a été refusée (${status}).`;
};

const isMaintenance = (text: string): boolean => {
  try {
    return (JSON.parse(text) as { status?: unknown })?.status === "maintenance";
  } catch {
    return false;
  }
};

// 5xx : corps jamais affiché ; seul api-datalake signale sa maintenance par `{ status: "maintenance" }`.
export const errorMessageFor = (status: number, text: string): string => {
  if (status === 429) return "Trop de requêtes, réessayez dans quelques instants.";
  if (status === 503 && isMaintenance(text)) return "Le service est en maintenance. Réessayez plus tard.";
  if (status >= 500) return `Le service a rencontré une erreur interne (${status}). Réessayez plus tard.`;
  return toErrorMessage(status, text);
};

export const useApi = <T>(input: RequestInfo | URL, init?: RequestInit) => {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    const target = input instanceof Request ? input.url : String(input);
    const fetchData = async () => {
      setError(null);
      setLoading(true);
      try {
        const response = await fetch(input, init);
        if (response.ok) {
          const res = (await response.json()) as T;
          setData(res);
        } else {
          const text = await response.text();
          console.error(`API ${response.status} on ${target}`, text);
          setError(errorMessageFor(response.status, text));
          setData(undefined);
        }
      } catch (e) {
        console.error(`API request failed on ${target}`, e);
        setError(e instanceof Error ? e.message : "Network error");
        setData(undefined);
      } finally {
        setLoading(false);
      }
    };
    void fetchData();
  }, [input, init]);
  return { data, error, loading };
};
