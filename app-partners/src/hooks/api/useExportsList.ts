import { useRestQuery } from "./useRestQuery";

interface ExportRecord {
  uuid: string;
  start_date: Date;
  end_date: Date;
  geo_selector: string[];
  download_url: string;
  filename: string;
  file_size: number;
  status: string;
}

interface UseExportsListParams {
  days?: number;
  refreshTrigger?: number;
}

interface ExportsListResponse {
  data: ExportRecord[];
  meta: null;
}

/**
 * Hook to fetch exports list.
 *
 * @example
 * const { data, loading, error, refetch } = useExportsList({ days: 30 });
 */
export function useExportsList(
  { days = 30, refreshTrigger }: UseExportsListParams = {},
) {
  return useRestQuery<ExportsListResponse>("v3", "exports/list", { days }, {
    method: "POST",
  }, [
    refreshTrigger,
  ]);
}
