import { features } from "@/config/features";
export interface AdministrationTab {
  id: string;
  href: string;
  label: string;
}

export const administrationTabs: AdministrationTab[] = [
  { id: "profil", href: "/administration/profil/", label: "Mon profil" },
  { id: "utilisateurs", href: "/administration/utilisateurs/", label: "Utilisateurs et accès" },
  { id: "operateurs", href: "/administration/operateurs/", label: "Opérateurs" },
  { id: "territoires", href: "/administration/territoires/", label: "Territoires" },
  { id: "cles-api", href: "/administration/cles-api/", label: "Clés d'API" },
  { id: "campagnes", href: "/administration/campagnes/", label: "Campagnes déclarées" },
];

// Onglet dont l'URL est le préfixe du chemin (campagnes/declarer/ active « Campagnes déclarées »).
export const tabIdFromPathname = (pathname: string): string | undefined => {
  const path = pathname.endsWith("/") ? pathname : `${pathname}/`;
  return administrationTabs.find((t) => path.startsWith(t.href))?.id;
};

export const visibleTabIds = (
  user: { role: string; operator_id: number | null; territory_id: number | null } | undefined,
  simulating: boolean,
  externalCampaigns: boolean = features.externalCampaigns,
): string[] => {
  if (!user) return ["profil"];
  const ids = ["profil"];
  if (["registry.admin", "operator.admin", "territory.admin"].includes(user.role)) ids.push("utilisateurs");
  if (user.role === "registry.admin" && !simulating) ids.push("operateurs", "territoires");
  // Les clés d'API sont réservées aux administrateurs (permission credentials.*), pas à tout compte opérateur.
  if (user.operator_id && ["operator.admin", "registry.admin"].includes(user.role)) ids.push("cles-api");
  if (externalCampaigns && (user.role.startsWith("registry.") || user.operator_id || user.territory_id))
    ids.push("campagnes");
  return ids;
};
