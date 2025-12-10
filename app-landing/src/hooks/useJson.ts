import { useState, useEffect } from 'react';
export const useJson = <T>(input: RequestInfo | URL, init?: RequestInit) => {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    const fetchData = async () => {
      const response = await fetch(input, init);
      const res = await response.json();
      if (response.ok) {
        setData(res);
        setError(null);
      } else {
        setError(res.error);
        setData(null);
      }
      setLoading(false);
    };
    fetchData();
  }, [input, init]);
  return { data, error, loading };
};
