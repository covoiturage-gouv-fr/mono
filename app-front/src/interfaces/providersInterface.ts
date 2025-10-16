import { type enumRoles } from "../helpers/auth";

export interface AuthContextProps {
  isAuth: boolean;
  setIsAuth: (newIsAuth: boolean) => void;
  user?: {
    email: string;
    name?: string;
    role: (typeof enumRoles)[number];
    permissions: string[];
    operator_id?: number;
    territory_id?: number;
    siret?: string;
  };
  simulate: boolean;
  simulatedRole?: "operator" | "territory";
  onChangeTerritory: (id?: number) => void;
  onChangeOperator: (id?: number) => void;
  onChangeSimulate: () => void;
  onChangeSimulatedRole: (value?: "operator" | "territory") => void;
  logout: () => Promise<void>;
}
