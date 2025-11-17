import { type enumRoles } from "../helpers/auth";

export interface AuthContextProps {
  isAuth: boolean;
  setIsAuth: (newIsAuth: boolean) => void;
  user?: {
    email: string;
    name?: string;
    role: (typeof enumRoles)[number];
    permissions: string[];
    operator_id: number | null;
    territory_id: number | null;
    siret?: string;
  };
  simulate: boolean;
  simulatedRole?: "operator" | "territory";
  onChangeTerritory: (id: number | null) => void;
  onChangeOperator: (id: number | null) => void;
  onChangeSimulate: (state: boolean) => void;
  onChangeSimulatedRole: (value: "operator" | "territory" | undefined) => void;
  logout: () => void;
}
