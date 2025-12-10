import { useEffect, useState } from "react";
export const useApi = <T>(input: RequestInfo | URL, init?: RequestInit) => {
  const [data, setData] = useState<T>();
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    const fetchData = async () => {
      setError(null);
      setLoading(true);
      const response = await fetch(input, init);
      const res = await response.json();
      if (response.ok) {
        setData(res);
        setLoading(false);
      } else {
        setError(res);
        setData(undefined);
        setLoading(false);
      }
    };
    fetchData();
  }, [input, init]);
  return { data, error, loading };
};
