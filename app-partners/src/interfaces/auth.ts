export interface UserInterface {
  email: string;
  name?: string;
  role: Role;
  permissions: string[];
  operator_id: number | null;
  territory_id: number | null;
  siret?: string;
  analytics_id?: string;
  organisation?: string;
}

export interface AuthContextProps {
  isAuth: boolean;
  setIsAuth: (newIsAuth: boolean) => void;
  user?: UserInterface;
  simulate: boolean;
  simulatedRole?: "operator" | "territory";
  onChangeTerritory: (id: number | null) => void;
  onChangeOperator: (id: number | null) => void;
  onChangeSimulate: (state: boolean) => void;
  onChangeSimulatedRole: (value: "operator" | "territory" | undefined) => void;
  logout: () => void;
}

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
