// Périmètre autorisé d'un utilisateur (opérateur XOR territoire).
export interface UserScope {
  operator_id?: number;
  territory_id?: number;
  label: string;
  siret?: string;
}

export interface UserInterface {
  email: string;
  name?: string;
  role: Role;
  permissions: string[];
  operator_id: number | null;
  territory_id: number | null;
  siret?: string;
  login_siren?: string | null;
  scopes?: UserScope[];
  analytics_id?: string;
  organisation?: string;
}

export interface AuthContextProps {
  isAuth: boolean;
  setIsAuth: (newIsAuth: boolean) => void;
  user?: UserInterface;
  scopes: UserScope[];
  activeScope?: UserScope;
  simulate: boolean;
  simulatedRole?: "operator" | "territory";
  onChangeTerritory: (id: number | null) => void;
  onChangeOperator: (id: number | null) => void;
  onChangeSimulate: (state: boolean) => void;
  onChangeSimulatedRole: (value: "operator" | "territory" | undefined) => void;
  // Bascule serveur du périmètre actif (distincte du mode simulate admin).
  switchScope: (territory_id: number) => Promise<void>;
  // Signale qu'un formulaire est en cours d'édition (garde-fou avant bascule).
  setFormEditing: (editing: boolean) => void;
  logout: () => void;
}

// Libellé du périmètre pour un registry.admin sans scope attribué.
export const ADMIN_SCOPE_LABEL = "Administration RPC";

export const roles = [
  "anonymous",
  "registry.admin",
  "territory.user",
  "territory.admin",
  "operator.user",
  "operator.admin",
] as const;

export type Role = (typeof roles)[number];
export type RoleKind = "registry" | "territory" | "operator";
export type RoleLevel = "admin" | "user";
