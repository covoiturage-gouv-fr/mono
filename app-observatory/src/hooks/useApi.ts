import { useEffect, useState } from "react";

// Corps d'erreur : JSON (`{ detail }` FastAPI, `{ message }`) ou texte brut.
export const toErrorMessage = (text: string): string => {
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    return text;
  }
  if (typeof body === "string") return body;
  if (typeof body === "object" && body !== null) {
    const { detail, message } = body as { detail?: unknown; message?: unknown };
    if (typeof detail === "string") return detail;
    if (typeof message === "string") return message;
  }
  return "Erreur inattendue de l'API";
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
          setError(toErrorMessage(text));
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
