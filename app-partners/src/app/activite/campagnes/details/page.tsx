"use client";
import DescriptiveSheetUrlEditor from "@/components/campaign/DescriptiveSheetUrlEditor";
import Loading from "@/components/layout/Loading";
import { useCampaignFind } from "@/hooks/api";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import { notFound, useRouter, useSearchParams } from "next/navigation";
import APDFTable from "./APDFTable";
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

  const { data: currentCampaign, loading, error } = useCampaignFind({ id: Number(params.get("id")) });

  if (loading) return <Loading />;
  if (!currentCampaign || error) notFound();
  return (
    <>
      <div className="flex justify-between items-center">
        <h2 className={fr.cx("fr-callout__title", "fr-mt-2w")}>Campagne {currentCampaign.name}</h2>
        <a
          href="#"
          className={fr.cx("fr-link", "fr-icon-arrow-left-s-line", "fr-link--icon-left")}
          target="_self"
          onClick={() => router.push("/activite/campagnes")}
          style={{ marginLeft: "auto" }}
        >
          Revenir à la liste des campagnes
        </a>
      </div>
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
      <APDFTable
        title="Appels de fonds calculés par le RPC"
        campaignId={currentCampaign._id}
        operatorId={user?.operator_id ?? null}
      />
    </>
  );
}
