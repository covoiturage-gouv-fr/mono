import { getApiUrl } from "@/helpers/api";
import { useCallback, useState } from "react";

interface DownloadLinkResponse {
  url: string;
}

/**
 * Hook to fetch a one-time download link for an export.
 *
 * @example
 * const { downloadLink, loading, error } = useExportDownloadLink();
 * const url = await downloadLink(uuid);
 */
export function useExportDownloadLink() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const downloadLink = useCallback(async (uuid: string): Promise<string> => {
    try {
      setLoading(true);
      setError(null);

      const response = await fetch(
        getApiUrl("v3", `exports/download-link/${uuid}`),
        { credentials: "include" },
      );

      if (!response.ok) {
        throw new Error("Failed to generate download link");
      }

      const json = (await response.json()) as DownloadLinkResponse;

      return json.url;
    } catch (e) {
      const err = e as Error;
      setError(err);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  return { downloadLink, loading, error };
}
