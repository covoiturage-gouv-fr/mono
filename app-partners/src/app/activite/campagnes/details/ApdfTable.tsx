import { Config } from "@/config";
import { getApiUrl } from "@/helpers/api";
import { useApi } from "@/hooks/useApi";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import { Download } from "@codegouvfr/react-dsfr/Download";
import Table from "@codegouvfr/react-dsfr/Table";
import { type ReactNode, useMemo } from "react";
import Loading from "../../../../components/layout/Loading";
import { ApdfRecord } from "../../../../interfaces/apdfInterface";
import { OperatorResult } from "../../../../interfaces/operatorInterface";

export default function ApdfTable(props: { title: string; campaignId: number; operatorId: number | null }) {
  const { user, simulate } = useAuth();
  const url = `${Config.get<string>("auth.domain")}/rpc?methods=apdf:list`;
  const init = useMemo(() => {
    const params = {
      campaign_id: props.campaignId,
      ...(props.operatorId !== null && { operator_id: props.operatorId }),
    };
    return {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        method: "apdf:list",
        params: params,
        id: 1,
      }),
    };
  }, [props.campaignId, props.operatorId]);
  const { data, loading, error } = useApi<{ id: number; result: { meta: null; data: ApdfRecord[] }; jsonrpc: string }>(
    url,
    false,
    init,
  );
  const operatorsApiUrl = getApiUrl("v3", `dashboard/operators`);
  const operatorsList = useApi<{ data: OperatorResult[] }>(operatorsApiUrl);
  const isRegistryAdmin = user?.role === "registry.admin" && simulate === false;

  const headers: string[] = isRegistryAdmin
    ? ["Mois", "Opérateur", "Trajets", "Trajets incités", "Montant d'incitations", ""]
    : ["Mois", "Opérateur", "Trajets incités", "Montant d'incitations", ""];

  const operatorName = (id: number | string) => operatorsList.data?.data?.find((o) => o.id === id)?.name ?? "inconnu";

  const formatAmount = (amount: number | string) => `${(Number(amount) / 100).toLocaleString()} €`;

  const formatSize = (size: number | string) =>
    `${(Number(size) / 1000).toLocaleString("fr-FR", { maximumFractionDigits: 1 })} Ko`;

  const dataTable = data?.result.data
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
        />,
      ];

      return [...baseCols, ...adminCols, ...commonCols];
    })
    .reverse();

  if (loading) return <Loading />;
  if (error) return <div>Erreur lors du chargement des données</div>;
  if (!dataTable) return <>Pas d&apos;APDF...</>;
  return (
    <div className={fr.cx("fr-my-4w")}>
      <h3 className={fr.cx("fr-callout__title")}>{props.title}</h3>
      <Table data={dataTable} headers={headers} colorVariant="blue-ecume" fixed />
    </div>
  );
}
