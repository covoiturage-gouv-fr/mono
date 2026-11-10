"use client";
import { useAuth } from "@/providers/AuthProvider";
import { Notice } from "@codegouvfr/react-dsfr/Notice";

// Bandeau global sous le header tant qu'un admin RPC simule un opérateur ou un territoire.
export function SimulationBanner() {
  const { simulate, simulatedRole, simulatedLabel, onChangeSimulate } = useAuth();
  if (!simulate || !simulatedRole) return null;
  const kind = simulatedRole === "operator" ? "de l'opérateur" : "du territoire";

  return (
    <Notice
      severity="warning"
      title={simulatedLabel ? `Simulation : vous consultez l'espace ${kind} ${simulatedLabel}.` : "Simulation activée."}
      description={
        simulatedLabel
          ? "Les données sont filtrées sur ce partenaire ; vos actions restent celles de l'équipe RPC."
          : `Choisissez ${simulatedRole === "operator" ? "un opérateur" : "un territoire"} dans votre profil.`
      }
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
