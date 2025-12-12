"use client";
import { fr } from "@codegouvfr/react-dsfr";
import { notFound, useRouter, useSearchParams } from "next/navigation";
import { useMemo } from "react";
import DescriptiveSheetUrlEditor from "../../../../components/campaign/DescriptiveSheetUrlEditor";
import Loading from "../../../../components/layout/Loading";
import { Config } from "../../../../config";
import { useApi } from "../../../../hooks/useApi";
import { Campaign } from "../../../../interfaces/campaignInterface";
import { useAuth } from "../../../../providers/AuthProvider";
import ApdfTable from "./ApdfTable";
import JourneysGraph from "./graphs/JourneysGraph";
import OperatorsGraph from "./graphs/OperatorsGraph";

export default function CampaignDetails() {
  const { user, simulatedRole } = useAuth();
  const router = useRouter();
  const params = useSearchParams();
  const formatDate = (dateStr: string): string => {
    const date = new Date(dateStr);
    return isNaN(date.getTime()) ? "Date invalide" : date.toLocaleDateString("fr-FR");
  };

  const url = `${Config.get<string>("auth.domain")}/rpc?methods=campaign:find`;
  const init = useMemo(
    () => ({
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        method: "campaign:find",
        params: { _id: Number(params.get("id")) },
        id: 1,
      }),
    }),
    [params.get("id")],
  );

  const { data, loading, error } = useApi<{
    id: number;
    result: { meta: null; data: Campaign };
    jsonrpc: string;
  }>(url, false, init);

  const currentCampaign = data?.result?.data;

  if (loading) return <Loading />;
  if (!currentCampaign || error) notFound();
  return (
    <>
      <a
        href="#"
        className={fr.cx("fr-link", "fr-icon-arrow-right-line", "fr-link--icon-right")}
        target="_self"
        onClick={() => router.push("/activite/campagnes")}
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
          <b>Durée de la campagne :</b> Du {formatDate(currentCampaign.start_date)} au{" "}
          {formatDate(currentCampaign.end_date)}
        </div>
        <div>
          <b>Estimation de la consommation du budget* :</b>{" "}
          {`${(Number(currentCampaign?.incentive_sum) / 100).toLocaleString()} € sur ${(Number(currentCampaign.max_amount) / 100).toLocaleString()} €`}
        </div>
        <i>
          * A noter que le budget est le montant dédié aux incitations uniquement et qu'il s'agit ici d'une estimation
          de la consommation en quasi temps réel.{" "}
        </i>
        <DescriptiveSheetUrlEditor
          campaignId={currentCampaign._id}
          initialValue={currentCampaign.descriptive_sheet_url}
        />
      </div>
      <JourneysGraph title="Evolution des trajets" campaignId={currentCampaign._id} />
      {["registry", "territory"].includes(user?.role.split(".")[0] ?? "") && simulatedRole !== "operator" && (
        <OperatorsGraph title="Evolution des trajets par opérateurs" campaignId={currentCampaign._id} />
      )}
      <ApdfTable
        title="Fichiers d'appels de fonds recalculés par covoiturage.beta.gouv"
        campaignId={currentCampaign._id}
        operatorId={user?.operator_id ?? null}
      />
    </>
  );
}
