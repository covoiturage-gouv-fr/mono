"use client";
import AdminTable from "@/components/common/AdminTable";
import {
  campaignListScope,
  canCreateCampaign,
  canEditCampaign,
  type ExternalCampaign,
} from "@/helpers/externalCampaign";
import { castPerimeterType } from "@/helpers/search";
import { useOperatorsList } from "@/hooks/api";
import {
  deleteExternalCampaign,
  useExternalCampaigns,
  validateExternalCampaign,
} from "@/hooks/api/useExternalCampaigns";
import { fr } from "@codegouvfr/react-dsfr";
import Alert from "@codegouvfr/react-dsfr/Alert";
import Badge from "@codegouvfr/react-dsfr/Badge";
import Button from "@codegouvfr/react-dsfr/Button";
import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useEffect, useState } from "react";
import ActionErrorAlert from "./ActionErrorAlert";
import { useCampaignContext } from "./useCampaignContext";

const DECLARER_URL = "/administration/campagnes/declarer/";
const MODIFIER_URL = "/administration/campagnes/modifier/";

const date = (d: string | null) => (d ? new Date(d).toLocaleDateString("fr-FR") : "—");

export default function CampagnesDeclareesPage() {
  const { user, simulatedRole } = useCampaignContext();
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
  const { user, registry, operatorId, territoryId } = useCampaignContext();
  // L'API ne renvoie à un opérateur que les campagnes où il est listé.
  const { rows, error: loadError, loading, refetch } = useExternalCampaigns(params);
  const { data: operatorsData } = useOperatorsList({ limit: 1000 });
  const operators = (operatorsData?.data ?? []).filter((o): o is { id: number; name: string; siret: string } => !!o.id);
  const operatorName = (id: number) => operators.find((o) => o.id === id)?.name ?? `#${id}`;
  const [actionError, setActionError] = useState<string>();
  const router = useRouter();
  const pathname = usePathname();
  const justSaved = useSearchParams().get("ok") === "enregistree";
  const [saved, setSaved] = useState<string | undefined>(
    justSaved ? "La campagne est enregistrée en brouillon." : undefined,
  );

  // Le message reste affiché, mais un rechargement ne le rejoue pas.
  useEffect(() => {
    if (justSaved) router.replace(pathname);
  }, [justSaved, pathname, router]);

  if (!user) return null;

  const run = async (action: () => Promise<void>, success: string) => {
    setActionError(undefined);
    setSaved(undefined);
    try {
      await action();
      setSaved(success);
      await refetch();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    }
  };

  const headers = ["Territoire", "Opérateurs", "Début", "Fin", "Statut", "Actions"];
  const data = rows.map((r: ExternalCampaign) => {
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
              linkProps: { href: `${MODIFIER_URL}?id=${r.id}` },
            },
            ...(registry && r.status === "draft"
              ? [
                  {
                    children: "valider",
                    iconId: "fr-icon-check-line" as const,
                    onClick: () => void run(() => validateExternalCampaign(r.id), "La campagne est validée."),
                  },
                ]
              : []),
            {
              children: "supprimer",
              iconId: "fr-icon-delete-bin-line",
              priority: "tertiary",
              onClick: () => {
                if (window.confirm("Supprimer cette campagne ?")) {
                  void run(() => deleteExternalCampaign(r.id), "La campagne est supprimée.");
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
      {actionError && <ActionErrorAlert message={actionError} onClose={() => setActionError(undefined)} />}
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
          linkProps={{ href: DECLARER_URL }}
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
