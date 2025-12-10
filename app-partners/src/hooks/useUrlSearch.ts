import { useRouter, useSearchParams } from "next/navigation";
import { useEffect, useState } from "react";
import { useDebounce } from "./useDebounce";

interface UseUrlSearchOptions {
  paramName?: string;
  debounceMs?: number;
}

interface UseUrlSearchReturn {
  search: string;
  debouncedSearch: string;
  setSearch: (value: string) => void;
  onChangeSearch: (value: string) => void;
}

export function useUrlSearch(options: UseUrlSearchOptions = {}): UseUrlSearchReturn {
  const { paramName = "q", debounceMs = 500 } = options;

  const router = useRouter();
  const searchParams = useSearchParams();
  const [search, setSearch] = useState("");
  const debouncedSearch = useDebounce(search, debounceMs);

  // Initialize search from URL param
  useEffect(() => {
    const param = searchParams.get(paramName);
    if (param) {
      setSearch(param);
    }
  }, [searchParams, paramName]);

  // Sync debounced search back to URL
  useEffect(() => {
    const params = new URLSearchParams(searchParams.toString());
    if (debouncedSearch) {
      params.set(paramName, debouncedSearch);
    } else {
      params.delete(paramName);
    }
    router.replace(`?${params.toString()}`, { scroll: false });
  }, [debouncedSearch, router, searchParams, paramName]);

  const onChangeSearch = (value: string) => {
    setSearch(value);
  };

  return {
    search,
    debouncedSearch,
    setSearch,
    onChangeSearch,
  };
}
