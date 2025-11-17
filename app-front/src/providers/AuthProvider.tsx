"use client";
import { type AuthContextProps } from "@/interfaces/providersInterface";
import { useRouter } from "next/navigation";
import { createContext, useContext, useEffect, useState } from "react";
import { Config } from "../config";

const AuthContext = createContext<AuthContextProps | undefined>(undefined);
export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [isAuth, setIsAuth] = useState(false);
  const [user, setUser] = useState<AuthContextProps["user"]>();
  const [simulate, setSimulate] = useState(false);
  const [simulatedRole, setSimulatedRole] = useState<"operator" | "territory" | undefined>(undefined);
  const [loading, setLoading] = useState(true);
  const router = useRouter();

  const checkAuth = async () => {
    const response = await fetch(`${Config.get<string>("auth.domain")}/auth/me`, { credentials: "include" });

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const data: AuthContextProps["user"] = await response.json();
    if (data?.role && data?.role !== "anonymous") {
      setIsAuth(true);
      setUser(data);
    } else {
      setIsAuth(false);
      setUser(data);
    }
    setLoading(false);
  };

  useEffect(() => {
    void checkAuth();
  }, []);

  // Redirect to default /activite page after login
  useEffect(() => {
    if (!loading) {
      if (!isAuth) {
        router.push("/");
      } else {
        router.push("/activite");
      }
    }
  }, [loading, isAuth]);

  // clean up user on simulatedRole change
  useEffect(() => {
    if (user) {
      setUser(prev =>
        prev
          ? {
              ...prev,
              territory_id: simulatedRole === "territory" ? prev.territory_id : null,
              operator_id: simulatedRole === "operator" ? prev.operator_id : null,
            }
          : prev
      );
    }
  }, [simulatedRole, user]);

  const onChangeTerritory = (id: number | null) => {
    const territory_id = simulatedRole === "territory" && id ? id : null;
    if (user) {
      setUser({ ...user, territory_id });
    }
  };

  const onChangeOperator = (id: number | null) => {
    const operator_id = simulatedRole === "operator" && id ? id : null;
    if (user) {
      setUser({ ...user, operator_id });
    }
  };

  const onChangeSimulate = (toggleState: boolean) => {
    setSimulate(toggleState);
    if (!toggleState) {
      setSimulatedRole(undefined);
    }
  };

  const onChangeSimulatedRole = (value: "operator" | "territory" | undefined) => {
    switch (value) {
      case "operator":
      case "territory":
        setSimulatedRole(value);
        break;
      default:
        setSimulatedRole(undefined);
    }
  };

  const logout = () => {
    setIsAuth(false);
    setUser(undefined);
  };

  return (
    <AuthContext.Provider
      value={{
        isAuth,
        setIsAuth,
        user,
        simulate,
        simulatedRole,
        onChangeTerritory,
        onChangeOperator,
        onChangeSimulate,
        onChangeSimulatedRole,
        logout,
      }}
    >
      {!loading && children}
    </AuthContext.Provider>
  );
}

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthContext Provider");
  }
  return context;
};
