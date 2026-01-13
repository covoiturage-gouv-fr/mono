import { getApiUrl } from "@/helpers/api";
import { TerritorySelectorsInterface } from "@/interfaces/dataInterface";
import { useCallback, useState } from "react";

export interface ExportCreateParams {
  tz: string;
  start_at: Date;
  end_at: Date;
  recipients?: string[];
  operator_id?: number[];
  geo_selector?: TerritorySelectorsInterface;
  territory_id?: number[];
}

enum ExportStatus {
  PENDING = "pending",
  RUNNING = "running",
  UPLOADING = "uploading",
  UPLOADED = "uploaded",
  NOTIFY = "notify",
  SUCCESS = "success",
  FAILURE = "failure",
}

enum ExportTarget {
  OPENDATA = "opendata",
  OPERATOR = "operator",
  TERRITORY = "territory",
}

interface ExportResponse {
  uuid: string;
  target: ExportTarget;
  status: ExportStatus;
  start_at: Date;
  end_at: Date;
}

/**
 * Hook to create an export.
 *
 * @example
 * const { createExport, loading, error, data } = useExportCreate();
 * await createExport({ tz: "Europe/Paris", start_at: new Date(), end_at: new Date() });
 */
export function useExportCreate() {
  const [data, setData] = useState<ExportResponse>();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const createExport = useCallback(
    async (params: ExportCreateParams): Promise<ExportResponse> => {
      try {
        setLoading(true);
        setError(null);
        setData(undefined);

        const response = await fetch(getApiUrl("v3", "exports"), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(params),
          credentials: "include",
        });

        if (!response.ok) {
          const errorData = (await response.json()) as { message?: string };
          throw new Error(errorData.message ?? "Failed to create export");
        }

        const json = (await response.json()) as ExportResponse;
        setData(json);
        return json;
      } catch (e) {
        const err = e as Error;
        setError(err);
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  const reset = useCallback(() => {
    setData(undefined);
    setError(null);
    setLoading(false);
  }, []);

  return { createExport, loading, error, data, reset };
}
