"use client";
import { campaignListScope, emptyExternalCampaign } from "@/helpers/externalCampaign";
import { useOperatorsList } from "@/hooks/api";
import { saveExternalCampaign, useExternalCampaigns } from "@/hooks/api/useExternalCampaigns";
import { fr } from "@codegouvfr/react-dsfr";
import Alert from "@codegouvfr/react-dsfr/Alert";
import { useRouter } from "next/navigation";
import { useState } from "react";
import ActionErrorAlert from "./ActionErrorAlert";
import ExternalCampaignForm from "./ExternalCampaignForm";
import { useCampaignContext } from "./useCampaignContext";

export const CAMPAGNES_URL = "/administration/campagnes/";

// id : null = déclaration, sinon modification de la campagne.
export default function CampagneFormPage({ id }: { id: number | null }) {
  const { user, simulatedRole } = useCampaignContext();
  if (!user) return null;
  const scope = campaignListScope(user, simulatedRole);
  if (scope.pending) {
    return (
      <p>
        Choisissez {simulatedRole === "operator" ? "un opérateur" : "un territoire"} dans « Mon profil » pour gérer ses
        campagnes déclarées.
      </p>
    );
  }
  return <CampagneForm id={id} params={scope.params} />;
}

function CampagneForm({ id, params }: { id: number | null; params: { operator_id?: number; territory_id?: number } }) {
  const router = useRouter();
  const { registry, operatorId, simulatingTerritory, territoryId, territoryLabel } = useCampaignContext();
  // Sans id, la requête liste tout : seule la modification s'en sert.
  const { rows, error: loadError, loading } = useExternalCampaigns(id === null ? params : { ...params, id });
  const { data: operatorsData } = useOperatorsList({ limit: 1000 });
  const operators = (operatorsData?.data ?? []).filter((o): o is { id: number; name: string; siret: string } => !!o.id);
  const [actionError, setActionError] = useState<string>();

  if (id !== null) {
    if (loadError) return <Alert severity="error" small description={loadError.message} />;
    if (loading) return <p>Chargement…</p>;
  }
  const current = id === null ? undefined : rows.find((r) => r.id === id);
  if (id !== null && !current) return <Alert severity="error" small description="Campagne introuvable." />;

  const initial = current ?? {
    ...emptyExternalCampaign(),
    // Compte opérateur, ou équipe RPC simulant un opérateur : pré-rempli.
    operator_ids: operatorId ? [operatorId] : [],
    // Valeur provisoire pour la validation du formulaire : l'API impose le territoire du compte.
    territory:
      territoryId === null ? null : { type: "custom" as const, code: String(territoryId), label: territoryLabel ?? "" },
  };

  return (
    <>
      {actionError && <ActionErrorAlert message={actionError} onClose={() => setActionError(undefined)} />}
      <div className={fr.cx("fr-grid-row")}>
        <div className={fr.cx("fr-col-12", "fr-col-lg-8")}>
          <h3 className={fr.cx("fr-callout__title")}>{current ? "Modifier la campagne" : "Déclarer une campagne"}</h3>
          {!current && <DeclarationIntro />}
          {current?.status === "validated" && (
            <p className={fr.cx("fr-text--sm")}>
              Cette campagne est validée : toute modification la repasse en brouillon jusqu&apos;à sa revalidation.
            </p>
          )}
          <ExternalCampaignForm
            initial={initial}
            operators={operators}
            lockedOperatorId={registry ? null : operatorId}
            lockedTerritoryLabel={simulatingTerritory && current ? current.territory.label : territoryLabel}
            onCancel={() => router.push(CAMPAGNES_URL)}
            onSubmit={(values) => {
              setActionError(undefined);
              saveExternalCampaign(
                values,
                current?.id ?? null,
                simulatingTerritory ? (territoryId ?? undefined) : undefined,
              )
                .then(() => router.push(`${CAMPAGNES_URL}?ok=enregistree`))
                .catch((e: unknown) => setActionError(e instanceof Error ? e.message : String(e)));
            }}
          />
        </div>
      </div>
    </>
  );
}

// Texte à valider par le PO avant la mise en production.
function DeclarationIntro() {
  return (
    <>
      <p>
        Ce formulaire sert à déclarer une campagne d&apos;incitation au covoiturage qui n&apos;est pas suivie par le
        Registre de preuve de covoiturage (RPC), c&apos;est-à-dire dont les incitations ne sont pas calculées par le
        RPC. Les campagnes suivies par le RPC sont déjà connues : inutile de les déclarer ici.
      </p>
      <p>
        Une fois validée par l&apos;équipe RPC, la campagne est intégrée aux statistiques de l&apos;observatoire du
        covoiturage (carte des campagnes d&apos;incitation) et publiée sur data.gouv.fr dans le jeu de données «{" "}
        <a
          href="https://www.data.gouv.fr/fr/datasets/64a436118c609995b0386541/"
          target="_blank"
          rel="noopener"
          title="Conditions des campagnes d'incitation financière au covoiturage - nouvelle fenêtre"
        >
          Conditions des campagnes d&apos;incitation financière au covoiturage
        </a>{" "}
        ». Tant qu&apos;elle n&apos;est pas validée, elle reste en brouillon et n&apos;est pas publiée.
      </p>
      <p>Les informations saisies sont donc publiques : n&apos;y indiquez aucune donnée personnelle.</p>
    </>
  );
}
