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

// null = URL déjà à jour : un replace inutile écrase une navigation en cours (onglet cliqué pendant le montage).
export function nextSearchQuery(current: URLSearchParams, paramName: string, value: string): string | null {
  if ((current.get(paramName) ?? "") === value) return null;
  const params = new URLSearchParams(current.toString());
  if (value) params.set(paramName, value);
  else params.delete(paramName);
  return params.toString();
}

export function useUrlSearch(options: UseUrlSearchOptions = {}): UseUrlSearchReturn {
  const { paramName = "q", debounceMs = 500 } = options;

  const router = useRouter();
  const searchParams = useSearchParams();
  const [search, setSearch] = useState("");
  const debouncedSearch = useDebounce(search, debounceMs);

  // Initialize search from URL param
  const [prevSearchParams, setPrevSearchParams] = useState<typeof searchParams | null>(null);
  const [prevParamName, setPrevParamName] = useState(paramName);
  if (prevSearchParams !== searchParams || prevParamName !== paramName) {
    setPrevSearchParams(searchParams);
    setPrevParamName(paramName);
    const param = searchParams.get(paramName);
    if (param) {
      setSearch(param);
    }
  }

  // Sync debounced search back to URL
  useEffect(() => {
    const query = nextSearchQuery(searchParams, paramName, debouncedSearch);
    if (query !== null) router.replace(`?${query}`, { scroll: false });
  }, [debouncedSearch, router, searchParams, paramName]);

  const onChangeSearch = (value: string) => {
    setSearch(value);
  };

  return { search, debouncedSearch, setSearch, onChangeSearch };
}
