import Loading from "@/components/layout/Loading";
import { useApdfList, useOperatorsList } from "@/hooks/api";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import { Download } from "@codegouvfr/react-dsfr/Download";
import Table from "@codegouvfr/react-dsfr/Table";
import { sendEvent } from "@socialgouv/matomo-next";
import { type ReactNode } from "react";

export default function APDFTable(props: { title: string; campaignId: number; operatorId: number | null }) {
  const { user, simulate } = useAuth();

  const { data, loading, error } = useApdfList({
    campaign_id: props.campaignId,
    ...(props.operatorId && { operator_id: props.operatorId }),
  });

  const operatorsList = useOperatorsList();
  const isRegistryAdmin = user?.role === "registry.admin" && simulate === false;

  const headers: string[] = isRegistryAdmin
    ? ["Mois", "Opérateur", "Trajets", "Trajets incités", "Montant d'incitations", ""]
    : ["Mois", "Opérateur", "Trajets incités", "Montant d'incitations", ""];

  const operatorName = (id: number | string) => operatorsList.data?.data?.find((o) => o.id === id)?.name ?? "inconnu";

  const formatAmount = (amount: number | string) => `${(Number(amount) / 100).toLocaleString()} €`;

  const formatSize = (size: number | string) =>
    `${(Number(size) / 1000).toLocaleString("fr-FR", { maximumFractionDigits: 1 })} Ko`;

  const dataTable = data
    ?.map((d, i) => {
      const baseCols: ReactNode[] = [d.datetime.slice(0, 7), operatorName(d.operator_id)];

      const adminCols: ReactNode[] = isRegistryAdmin ? [d.trips.toLocaleString()] : [];

      const commonCols: ReactNode[] = [
        d.subsidized.toLocaleString(),
        formatAmount(d.amount),
        <Download
          key={i}
          details={`xlsx - ${formatSize(d.size)}`}
          label="Télécharger"
          linkProps={{ href: d.signed_url }}
          onClick={() => {
            sendEvent({
              category: "campagne",
              action: "Consultation des APDF",
              name: `Consultation APDF ${d.datetime.slice(0, 7)}`,
              value: `Campaign ID: ${campaignId} / Operator ID: ${props.operatorId}`,
            });
          }}
        />,
      ];

      return [...baseCols, ...adminCols, ...commonCols];
    })
    .reverse();

  if (loading) return <Loading />;
  if (error) return <div>Erreur lors du chargement des APDF</div>;
  if (!dataTable) return <>Pas d&apos;APDF disponibles</>;
  return (
    <div className={fr.cx("fr-my-4w")}>
      <h3 className={fr.cx("fr-callout__title")}>{props.title}</h3>
      <Table data={dataTable} headers={headers} colorVariant="blue-ecume" fixed />
    </div>
  );
}
