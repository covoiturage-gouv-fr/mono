import { getApiUrl } from "@/helpers/api";
import { apiErrorMessage, parseBody, toUserError } from "@/hooks/useApi";
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

  const createExport = useCallback(async (params: ExportCreateParams): Promise<ExportResponse> => {
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

      const parsed = parseBody(await response.text());
      if (!response.ok) {
        throw new Error(apiErrorMessage(response.status, parsed));
      }

      const json = parsed as ExportResponse;
      setData(json);
      return json;
    } catch (e) {
      const err = toUserError(e);
      setError(err);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  const reset = useCallback(() => {
    setData(undefined);
    setError(null);
    setLoading(false);
  }, []);

  return { createExport, loading, error, data, reset };
}
