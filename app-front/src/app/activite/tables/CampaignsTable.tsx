import Loading from "@/components/layout/Loading";
import { Config } from "@/config";
import { getApiUrl } from "@/helpers/api";
import { useApi } from "@/hooks/useApi";
import { type TerritoriesInterface } from "@/interfaces/dataInterface";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import Alert from "@codegouvfr/react-dsfr/Alert";
import Button from "@codegouvfr/react-dsfr/Button";
import Pagination from "@codegouvfr/react-dsfr/Pagination";
import { Table } from "@codegouvfr/react-dsfr/Table";
import { type ReactNode, useEffect, useMemo, useState } from "react";
import JourneysGraph from "../graphs/JourneysGraph";
import OperatorsGraph from "../graphs/OperatorsGraph";
import ApdfTable from "./ApdfTable";

export default function CampaignsTable(props: {
  title: string;
  territoryId: number | null;
  operatorId: number | null;
}) {
  const [campaignId, setCampaignId] = useState<number>();
  const { user, simulatedRole } = useAuth();
  const pageSize = 15;
  const [page, setPage] = useState(1);
  const url = `${Config.get<string>("auth.domain")}/rpc?methods=campaign:list`;
  const init = useMemo(() => {
    const params = {
      ...(props.territoryId && { territory_id: props.territoryId }),
      ...(props.operatorId && { operator_id: props.operatorId }),
    };
    return {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        method: "campaign:list",
        params: params,
        id: 1,
      }),
    };
  }, [props.territoryId, props.operatorId]);
  const { data, loading } = useApi<{
    id: number;
    result: { meta: null; data: Record<string, string | number>[] };
    jsonrpc: string;
  }>(url, false, init);
  const territoriesApiUrl = getApiUrl("v3", `dashboard/territories?limit=200`);
  const { data: territoriesData } = useApi<TerritoriesInterface>(
    territoriesApiUrl,
    false,
    undefined,
    user?.territory_id,
  );
  const territoriesList = () => {
    if (user?.territory_id && territoriesData?.data) {
      return [territoriesData?.data.find((t) => t._id === user?.territory_id)] as TerritoriesInterface["data"];
    }
    return territoriesData?.data ?? [];
  };
  const getIcon = (value: string) => {
    return value === "finished" ? (
      <span className={fr.cx("ri-close-circle-fill", "fr-badge--error")} aria-hidden="true"></span>
    ) : value === "active" ? (
      <span className={fr.cx("ri-verified-badge-fill", "fr-badge--success")} aria-hidden="true"></span>
    ) : (
      value
    );
  };
  const active = data?.result.data
    .filter((d) => d.status === "active")
    .sort((a, b) => new Date(b.start_date).getTime() - new Date(a.start_date).getTime());

  const others = data?.result.data
    .filter((d) => d.status !== "active")
    .sort((a, b) => new Date(b.start_date).getTime() - new Date(a.start_date).getTime());
  const dataTableFull = [...(active ?? []), ...(others ?? [])].map((d, i) => [
    getIcon(d.status as string),
    new Date(d.start_date).toLocaleDateString(),
    new Date(d.end_date).toLocaleDateString(),
    territoriesList().find((t) => t._id === d.territory_id)?.name ?? d.territory_id,
    d.name,
    `${(Number(d.incentive_sum) / 100).toLocaleString()} €`,
    `${(Number(d.max_amount) / 100).toLocaleString()} €`,
    <Button key={i} size="small" onClick={() => setCampaignId(Number(d._id))}>
      Détails
    </Button>,
  ]) as ReactNode[][];
  const pageCount = Math.max(1, Math.ceil(dataTableFull.length / pageSize));
  const dataTable = dataTableFull.slice((page - 1) * pageSize, page * pageSize);

  // ⚠️ si les données changent, on revient à la première page
  useEffect(() => {
    setPage(1);
  }, [dataTableFull.length]);

  const headers = [
    "Statut",
    "Date de début",
    "Date de fin",
    "Territoire",
    "Nom de la campagne",
    "Dépense estimée",
    "Budget",
    "",
  ];

  const currentCampaign = data?.result.data.find((d) => Number(d._id) === campaignId);
  if (loading) return <Loading />;
  return (
    <>
      {!campaignId && (
        <>
          {dataTable.length > 0 ? (
            <>
              <h3 className={fr.cx("fr-callout__title")}>{props.title}</h3>
              <Table data={dataTable} headers={headers} colorVariant="blue-ecume" />
              <div className={fr.cx("fr-grid-row", "fr-mt-5w")}>
                <div className={fr.cx("fr-mx-auto")}>
                  <Pagination
                    defaultPage={page}
                    count={pageCount}
                    getPageLinkProps={(value) => ({
                      onClick: () => setPage(value),
                      href: "#",
                    })}
                    showFirstLast
                  />
                </div>
              </div>
            </>
          ) : (
            <Alert
              title={"Pas de campagnes en cours"}
              severity="info"
              description={
                <p>
                  A date, nous n&apos;effectuons pas le suivi de vos campagnes d&apos;incitations financières,
                  n&apos;hésitez pas à nous contacter en cas de besoin. Vous avez par contre accès à la fonctionnalité
                  d&apos;export de données.
                </p>
              }
            />
          )}
        </>
      )}
      {campaignId && currentCampaign && (
        <>
          <a
            href="#"
            className={fr.cx("fr-link", "fr-icon-arrow-right-line", "fr-link--icon-right")}
            target="_self"
            onClick={() => setCampaignId(undefined)}
          >
            Revenir à toutes les campagnes
          </a>
          <h3 className={fr.cx("fr-callout__title", "fr-mt-2w")}>Campagne {currentCampaign.name}</h3>
          <div className={fr.cx("fr-callout")}>
            <div className={fr.cx("fr-callout__title")}>{currentCampaign.territory_name}</div>
            <div>
              <b>Nom de la campagne :</b> {currentCampaign.name}
            </div>
            <div>
              <b>Durée de la campagne :</b> Du {new Date(currentCampaign.start_date).toLocaleDateString()} au{" "}
              {new Date(currentCampaign.end_date).toLocaleDateString()}
            </div>
            <div>
              <b>Estimation de la consommation du budget* :</b>{" "}
              {`${(Number(currentCampaign.incentive_sum) / 100).toLocaleString()} € sur ${(Number(currentCampaign.max_amount) / 100).toLocaleString()} €`}
            </div>
            <i>
              * A noter que le budget est le montant dédié aux incitations uniquement et qu’il s’agit ici d’une
              estimation de la consommation en quasi temps réel.{" "}
            </i>
          </div>
          <JourneysGraph title="Evolution des trajets" campaignId={campaignId} />
          {["registry", "territory"].includes(user?.role.split(".")[0] ?? "") && simulatedRole !== "operator" && (
            <OperatorsGraph title="Evolution des trajets par opérateurs" campaignId={campaignId} />
          )}
          <ApdfTable
            title="Fichiers d’appels de fonds recalculés par covoiturage.beta.gouv"
            campaignId={campaignId}
            operatorId={props.operatorId}
          />
        </>
      )}
    </>
  );
}
