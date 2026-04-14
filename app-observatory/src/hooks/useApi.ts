import { useEffect, useState } from "react";
export const useApi = <T>(input: RequestInfo | URL, init?: RequestInit) => {
  const [data, setData] = useState<T>();
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
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
          setError(errorMessage);
          setData(undefined);
        }
      } catch (e: any) {
        setError(e.message ?? "Network error");
        setData(undefined);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, [input, init]);
  return { data, error, loading };
};
