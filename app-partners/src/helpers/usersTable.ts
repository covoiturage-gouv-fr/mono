import { type UserScopeInput } from "@/interfaces/dataInterface";

// Colonne « Territoires » : libellé = périmètre par défaut, suffixe (+N) et infobulle avec tous les noms.
// nameOf résout les noms manquants (scopes masqués hors registry.admin : seul territory_id est renvoyé).
export const territoriesCell = (
  scopes: UserScopeInput[] | undefined,
  fallbackId?: number,
  nameOf: (id: number) => string | undefined = () => undefined,
): { label: string; extra: number; all: string[] } => {
  const list = scopes?.length ? scopes : fallbackId ? [{ territory_id: fallbackId, is_default: true }] : [];
  if (!list.length) return { label: "", extra: 0, all: [] };
  const name = (s: UserScopeInput): string =>
    s.territory_name ?? (s.territory_id ? nameOf(s.territory_id) : undefined) ?? `#${s.territory_id}`;
  const def = list.find((s) => s.is_default) ?? list[0];
  return { label: name(def), extra: list.length - 1, all: list.map(name) };
};

const ROLE_COLORS = { registry: "blue", operator: "orange", territory: "green" } as const;

// Légende du tableau : une pastille par famille de rôle, puis pleine/cerclée pour admin/utilisateur.
export const ROLE_LEGEND = [
  { className: `role-pill__dot--${ROLE_COLORS.registry}`, label: "Registre (RPC)" },
  { className: `role-pill__dot--${ROLE_COLORS.operator}`, label: "Opérateur" },
  { className: `role-pill__dot--${ROLE_COLORS.territory}`, label: "Territoire" },
  { className: "role-pill__dot--grey", label: "Administrateur" },
  { className: "role-pill__dot--grey role-pill__dot--outline", label: "Utilisateur" },
] as const;

// Classes de la pastille : couleur de la famille, cerclée si le rôle n'est pas admin.
export const roleDotClass = (role: string): string => {
  const color = roleColor(role) ?? "grey";
  const outline = role.endsWith(".admin") ? "" : " role-pill__dot--outline";
  return `role-pill__dot role-pill__dot--${color}${outline}`;
};

// Pastille de la colonne « Rôle » : la couleur suit la famille (préfixe avant le point).
export const roleColor = (role: string): (typeof ROLE_COLORS)[keyof typeof ROLE_COLORS] | undefined =>
  ROLE_COLORS[role.split(".")[0] as keyof typeof ROLE_COLORS];
