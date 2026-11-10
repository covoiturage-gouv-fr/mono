"use client";
import { useAuth } from "@/providers/AuthProvider";
import { Notice } from "@codegouvfr/react-dsfr/Notice";

interface Selection {
  simulatedRole?: "operator" | "territory";
  operator_id?: number | null;
  territory_id?: number | null;
}

// L'id choisi fait foi : le libellé peut manquer alors qu'une sélection existe.
export function hasSimulatedSelection({ simulatedRole, operator_id, territory_id }: Selection): boolean {
  if (simulatedRole === "operator") return !!operator_id;
  if (simulatedRole === "territory") return !!territory_id;
  return false;
}

// Bandeau global sous le header tant qu'un admin RPC simule un opérateur ou un territoire choisi.
export function SimulationBanner() {
  const { simulate, simulatedRole, simulatedLabel, onChangeSimulate, user } = useAuth();
  const selected = hasSimulatedSelection({
    simulatedRole,
    operator_id: user?.operator_id,
    territory_id: user?.territory_id,
  });
  if (!simulate || !selected) return null;
  const kind = simulatedRole === "operator" ? "de l'opérateur" : "du territoire";

  return (
    <Notice
      className="simulation-banner"
      severity="warning"
      title={`Simulation : vous consultez l'espace ${kind}${simulatedLabel ? ` ${simulatedLabel}` : ""}.`}
      description="Les données sont filtrées sur ce partenaire ; vos actions restent celles de l'équipe RPC."
      // Le Notice ouvre ses liens dans un nouvel onglet par défaut, où la simulation (en mémoire) serait perdue.
      link={{
        linkProps: { href: "/administration/profil", target: "_self", rel: undefined },
        text: "Modifier la simulation",
      }}
      isClosable
      isClosed={false}
      onClose={() => onChangeSimulate(false)}
    />
  );
}
