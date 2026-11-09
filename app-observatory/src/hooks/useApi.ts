import { useEffect, useState } from "react";
export const useApi = <T>(input: RequestInfo | URL, init?: RequestInit) => {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    const target = input instanceof Request ? input.url : String(input);
    const fetchData = async () => {
      setError(null);
      setLoading(true);
      try {
        const response = await fetch(input, init);
        if (response.ok) {
          const res = await response.json();
          setData(res);
        } else {
          const text = await response.text();
          let errorMessage: any;
          try {
            errorMessage = JSON.parse(text);
          } catch {
            errorMessage = text;
          }
          console.error(`API ${response.status} on ${target}`, errorMessage);
          setError(errorMessage);
          setData(undefined);
        }
      } catch (e: any) {
        console.error(`API request failed on ${target}`, e);
        setError(e.message ?? "Network error");
        setData(undefined);
      } finally {
        setLoading(false);
      }
    };
    void fetchData();
  }, [input, init]);
  return { data, error, loading };
};
