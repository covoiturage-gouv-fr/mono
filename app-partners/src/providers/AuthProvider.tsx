"use client";
import { getUserSession } from "@/helpers/auth";
import { type AuthContextProps } from "@/interfaces/auth";
import { usePathname, useRouter } from "next/navigation";
import { createContext, useContext, useEffect, useState } from "react";

const AuthContext = createContext<AuthContextProps | undefined>(undefined);
export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [isAuth, setIsAuth] = useState(false);
  const [user, setUser] = useState<AuthContextProps["user"]>();
  const [simulate, setSimulate] = useState(false);
  const [simulatedRole, setSimulatedRole] = useState<"operator" | "territory" | undefined>(undefined);
  const [loading, setLoading] = useState(true);
  const router = useRouter();
  const pathname = usePathname();

  const checkAuth = async () => {
    const data = await getUserSession();
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

  useEffect(() => {
    if (!loading) {
      if (!isAuth) {
        router.push("/");
      } else if (isAuth && pathname === "/") {
        router.push("/activite");
      }
    }
  }, [loading, isAuth, pathname, router]);

  // clean up user on simulatedRole reset
  useEffect(() => {
    if (!simulatedRole) {
      setUser((prev) => (prev ? { ...prev, territory_id: null, operator_id: null } : prev));
    }
  }, [simulatedRole]);

  const onChangeTerritory = (id: number | null) => {
    const territory_id = simulatedRole === "territory" && id ? id : null;
    if (user) {
      setUser((prev) => (prev ? { ...prev, territory_id, operator_id: null } : prev));
    }
  };

  const onChangeOperator = (id: number | null) => {
    const operator_id = simulatedRole === "operator" && id ? id : null;
    if (user) {
      setUser((prev) => (prev ? { ...prev, operator_id, territory_id: null } : prev));
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
