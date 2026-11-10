"use client";
import AdminTable from "@/components/common/AdminTable";
import { isRegistry } from "@/helpers/auth";
import {
  campaignListScope,
  canCreateCampaign,
  canEditCampaign,
  emptyCampaignCondition,
  type CampaignCondition,
} from "@/helpers/campaignCondition";
import { castPerimeterType } from "@/helpers/search";
import { useOperatorsList } from "@/hooks/api";
import {
  deleteCampaignCondition,
  saveCampaignCondition,
  useCampaignConditions,
  validateCampaignCondition,
} from "@/hooks/api/useCampaignConditions";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import Alert from "@codegouvfr/react-dsfr/Alert";
import Badge from "@codegouvfr/react-dsfr/Badge";
import Button from "@codegouvfr/react-dsfr/Button";
import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";
import { useState } from "react";
import CampaignConditionForm from "./CampaignConditionForm";

const date = (d: string | null) => (d ? new Date(d).toLocaleDateString("fr-FR") : "—");

export default function CampagnesDeclareesPage() {
  const { user, simulatedRole } = useAuth();
  if (!user) return null;
  const scope = campaignListScope(user, simulatedRole);
  if (scope.pending) {
    return (
      <p>
        Choisissez {simulatedRole === "operator" ? "un opérateur" : "un territoire"} dans « Mon profil » pour voir ses
        campagnes déclarées.
      </p>
    );
  }
  return <CampagnesDeclarees params={scope.params} />;
}

function CampagnesDeclarees({ params }: { params: { operator_id?: number; territory_id?: number } }) {
  const { user, activeScope, simulatedRole, simulatedLabel } = useAuth();
  const registry = isRegistry(user?.role);
  const operatorId = user?.operator_id ?? null;
  const simulatingTerritory = registry && simulatedRole === "territory";
  const territoryId = simulatingTerritory
    ? (user?.territory_id ?? null)
    : !registry && operatorId === null
      ? (user?.territory_id ?? null)
      : null;
  const territoryLabel =
    territoryId === null
      ? null
      : simulatingTerritory
        ? (simulatedLabel ?? "Territoire simulé")
        : (activeScope?.label ?? user?.organisation ?? "Votre territoire");
  // L'API ne renvoie à un opérateur que les campagnes où il est listé.
  const { rows, error: loadError, loading, refetch } = useCampaignConditions(params);
  const { data: operatorsData } = useOperatorsList({ limit: 1000 });
  const operators = (operatorsData?.data ?? []).filter((o): o is { id: number; name: string; siret: string } => !!o.id);
  const operatorName = (id: number) => operators.find((o) => o.id === id)?.name ?? `#${id}`;
  // null = formulaire fermé, 0 = création
  const [editing, setEditing] = useState<number | null>(null);
  const [actionError, setActionError] = useState<string>();
  const [saved, setSaved] = useState<string>();

  if (!user) return null;

  const run = async (action: () => Promise<void>, success: string) => {
    setActionError(undefined);
    setSaved(undefined);
    try {
      await action();
      setSaved(success);
      setEditing(null);
      await refetch();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    }
  };

  const errorAlert = actionError && (
    <Alert
      className={fr.cx("fr-mb-3w")}
      severity="error"
      small
      description={actionError}
      closable
      onClose={() => setActionError(undefined)}
    />
  );

  if (editing !== null) {
    const current = rows.find((r) => r.id === editing);
    const initial = current ?? {
      ...emptyCampaignCondition(),
      // Compte opérateur, ou équipe RPC simulant un opérateur : pré-rempli.
      operator_ids: operatorId ? [operatorId] : [],
      // Valeur provisoire pour la validation du formulaire : l'API impose le territoire du compte.
      territory:
        territoryId === null
          ? null
          : { type: "custom" as const, code: String(territoryId), label: territoryLabel ?? "" },
    };
    return (
      <>
        {errorAlert}
        <h3 className={fr.cx("fr-callout__title")}>{current ? "Modifier la campagne" : "Déclarer une campagne"}</h3>
        {current?.status === "validated" && (
          <p className={fr.cx("fr-text--sm")}>
            Cette campagne est validée : toute modification la repasse en brouillon jusqu&apos;à sa revalidation.
          </p>
        )}
        <CampaignConditionForm
          initial={initial}
          operators={operators}
          lockedOperatorId={registry ? null : operatorId}
          lockedTerritoryLabel={simulatingTerritory && current ? current.territory.label : territoryLabel}
          onCancel={() => {
            setActionError(undefined);
            setEditing(null);
          }}
          onSubmit={(values) =>
            void run(
              () =>
                saveCampaignCondition(
                  values,
                  current?.id ?? null,
                  simulatingTerritory ? (territoryId ?? undefined) : undefined,
                ),
              "La campagne est enregistrée en brouillon.",
            )
          }
        />
      </>
    );
  }

  const headers = ["Territoire", "Opérateurs", "Début", "Fin", "Statut", "Actions"];
  const data = rows.map((r: CampaignCondition) => {
    const editable = canEditCampaign(r, { role: user.role, operator_id: operatorId, territory_id: territoryId });
    return [
      <span key="t">
        {r.territory.label}
        <br />
        <span className={fr.cx("fr-text--xs")}>
          {castPerimeterType(r.territory.type)} {r.territory.code}
        </span>
      </span>,
      r.operator_ids.map(operatorName).join(", "),
      date(r.date_debut),
      date(r.date_fin),
      r.status === "validated" ? (
        <Badge key="s" severity="success" small>
          Validée
        </Badge>
      ) : (
        <Badge key="s" severity="info" small>
          Brouillon
        </Badge>
      ),
      editable ? (
        <ButtonsGroup
          key="a"
          buttonsSize="small"
          inlineLayoutWhen="lg and up"
          buttons={[
            {
              children: "modifier",
              iconId: "fr-icon-edit-line",
              priority: "secondary",
              onClick: () => setEditing(r.id),
            },
            ...(registry && r.status === "draft"
              ? [
                  {
                    children: "valider",
                    iconId: "fr-icon-check-line" as const,
                    onClick: () => void run(() => validateCampaignCondition(r.id), "La campagne est validée."),
                  },
                ]
              : []),
            {
              children: "supprimer",
              iconId: "fr-icon-delete-bin-line",
              priority: "tertiary",
              onClick: () => {
                if (window.confirm("Supprimer cette campagne ?")) {
                  void run(() => deleteCampaignCondition(r.id), "La campagne est supprimée.");
                }
              },
            },
          ]}
        />
      ) : (
        <span key="a" className={fr.cx("fr-text--xs")}>
          Lecture seule : contactez l&apos;équipe RPC pour une correction
        </span>
      ),
    ];
  });

  return (
    <>
      {errorAlert}
      {saved && (
        <Alert
          className={fr.cx("fr-mb-3w")}
          severity="success"
          small
          description={saved}
          closable
          onClose={() => setSaved(undefined)}
        />
      )}
      <h3 className={fr.cx("fr-callout__title")}>Campagnes d&apos;incitation déclarées</h3>
      <p>
        Ces campagnes alimentent la carte de l&apos;observatoire et le jeu de données publié sur data.gouv.fr, après
        validation par l&apos;équipe RPC.
        {operatorId !== null &&
          !registry &&
          " Les campagnes impliquant plusieurs opérateurs sont saisies par l'équipe RPC ou le territoire."}
      </p>
      {canCreateCampaign(user.role) && (
        <Button
          iconId="fr-icon-add-circle-line"
          size="small"
          className={fr.cx("fr-mb-2w")}
          onClick={() => setEditing(0)}
        >
          Déclarer une campagne
        </Button>
      )}
      {loadError ? (
        <Alert severity="error" small description={loadError.message} />
      ) : loading ? (
        <p>Chargement…</p>
      ) : data.length > 0 ? (
        <AdminTable headers={headers} data={data} />
      ) : (
        <p>Aucune campagne déclarée.</p>
      )}
    </>
  );
}
