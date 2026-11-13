import { useState, useEffect } from 'react';
export const useJson = <T>(input: RequestInfo | URL, init?: RequestInit) => {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    const fetchData = async () => {
      const response = await fetch(input, init);
      if (response.ok) {
        setData((await response.json()) as T);
        setError(null);
      } else {
        const res = (await response.json()) as { error?: unknown };
        setError(res.error);
        setData(null);
      }
      setLoading(false);
    };
    void fetchData();
  }, [input, init]);
  return { data, error, loading };
};
