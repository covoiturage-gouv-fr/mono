import Loading from "@/components/layout/Loading";
import { useExportDownloadLink, useExportsList } from "@/hooks/api";
import { fr } from "@codegouvfr/react-dsfr";
import Badge from "@codegouvfr/react-dsfr/Badge";
import Download from "@codegouvfr/react-dsfr/Download";
import Pagination from "@codegouvfr/react-dsfr/Pagination";
import { Table } from "@codegouvfr/react-dsfr/Table";
import { type ReactNode, useEffect, useMemo, useState } from "react";
import AlertMessage from "../common/AlertMessage";

interface ExportListItemInterface {
  uuid: string;
  start_date: Date;
  end_date: Date;
  geo_selector: string[];
  download_url: string;
  filename: string;
  file_size: number;
  status: string;
}

const formatFileSize = (bytes: number): string => {
  if (!bytes || bytes === 0) return "Taille inconnue";
  const sizes = ["octets", "Ko", "Mo", "Go"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${sizes[i]}`;
};

const getStatusBadge = (status: string) => {
  const statusConfig: Record<string, { severity: "success" | "error" | "info" | "warning" | "new"; label: string }> = {
    pending: { severity: "info", label: "En cours de traitement" },
    running: { severity: "info", label: "En cours de traitement" },
    uploading: { severity: "info", label: "En cours de traitement" },
    uploaded: { severity: "info", label: "En cours de traitement" },
    notify: { severity: "info", label: "En cours de traitement" },
    success: { severity: "success", label: "Succès" },
    failure: { severity: "error", label: "Échec du traitement" },
  };

  const config = statusConfig[status] || { severity: "info" as const, label: status };
  return (
    <Badge severity={config.severity} small>
      {config.label}
    </Badge>
  );
};

interface ExportListProps {
  refreshTrigger?: number;
  days?: number;
  pageSize?: number;
}

export default function ExportList({ refreshTrigger, days = 30, pageSize = 25 }: ExportListProps) {
  const [page, setPage] = useState(1);
  const [alert, setAlert] = useState<string | undefined>();
  const { data: response, loading } = useExportsList({ days, refreshTrigger });
  const { downloadLink } = useExportDownloadLink();

  const allData = useMemo(() => {
    return response?.data ?? [];
  }, [response]);

  const handleDownload = async (uuid: string, filename: string) => {
    try {
      const url = await downloadLink(uuid);
      Object.assign(document.createElement("a"), {
        href: url,
        download: filename,
        target: "_blank",
      }).click();
    } catch {
      setAlert("Erreur lors du téléchargement du fichier.");
    }
  };

  const dataTableFull = Array.isArray(allData)
    ? (allData.map((d: ExportListItemInterface) => [
        new Date(d.start_date).toLocaleDateString("fr-FR"),
        new Date(d.end_date).toLocaleDateString("fr-FR"),
        d.geo_selector?.length > 0 ? d.geo_selector.join(", ") : "-",
        d.download_url && d.filename ? (
          <Download
            label={d.filename}
            details={`${d.filename.split(".").pop()?.toUpperCase()} • ${formatFileSize(d.file_size)}`}
            linkProps={{
              href: "#",
              onClick: (e) => {
                e.preventDefault();
                void handleDownload(d.uuid, d.filename);
              },
            }}
          />
        ) : (
          getStatusBadge(d.status)
        ),
      ]) as ReactNode[][])
    : [];

  const pageCount = Math.max(1, Math.ceil(dataTableFull.length / pageSize));
  const dataTable = dataTableFull.slice((page - 1) * pageSize, page * pageSize);

  useEffect(() => {
    setPage(1);
  }, [refreshTrigger]);

  const tableHeaders = ["Date de début", "Date de fin", "Zone géographique", "Fichier"];

  if (loading) {
    return <Loading />;
  }

  if (dataTable.length === 0) {
    return null;
  }

  return (
    <>
      {alert && (
        <AlertMessage
          title="Une erreur s'est produite"
          message={alert}
          typeAlert="error"
          onClose={() => setAlert(undefined)}
        />
      )}
      <div className={fr.cx("fr-mt-4w")}>
        <h3 className={fr.cx("fr-callout__title")}>Historique des exports des 30 derniers jours</h3>
        <Table data={dataTable} headers={tableHeaders} colorVariant="blue-ecume" fixed />
        <div className={fr.cx("fr-grid-row", "fr-mt-5w")}>
          <div className={fr.cx("fr-mx-auto")}>
            <Pagination
              defaultPage={page}
              count={pageCount}
              getPageLinkProps={(value) => ({
                onClick: () => setPage(value),
                href: "#",
              })}
            />
          </div>
        </div>
      </div>
    </>
  );
}
