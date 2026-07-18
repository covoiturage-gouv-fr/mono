"use client";
import { activeScopeLabel, getActiveScope, getUserSession, postAuthContext } from "@/helpers/auth";
import { type AuthContextProps } from "@/interfaces/auth";
import { fr } from "@codegouvfr/react-dsfr";
import Alert from "@codegouvfr/react-dsfr/Alert";
import { usePathname, useRouter } from "next/navigation";
import { createContext, useContext, useEffect, useRef, useState } from "react";

// Clés du miroir client (affichage + ré-assertion après reload).
const MIRROR_KEY = "rpc.active_territory_id";
const TOAST_KEY = "rpc.scope_switch_toast";

const AuthContext = createContext<AuthContextProps | undefined>(undefined);
export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [isAuth, setIsAuth] = useState(false);
  const [user, setUser] = useState<AuthContextProps["user"]>();
  const [simulate, setSimulate] = useState(false);
  const [simulatedRole, setSimulatedRole] = useState<"operator" | "territory" | undefined>(undefined);
  const [loading, setLoading] = useState(true);
  const [switchToast, setSwitchToast] = useState<string>();
  const formEditingRef = useRef(false);
  const router = useRouter();
  const pathname = usePathname();

  const checkAuth = async () => {
    const data = await getUserSession();
    if (data?.role && data?.role !== "anonymous") {
      // Ré-assertion : si la session serveur est repartie sur le défaut, restaurer le scope choisi.
      const mirror = Number(sessionStorage.getItem(MIRROR_KEY)) || null;
      const stillGranted = mirror && data.scopes?.some((s) => s.territory_id === mirror);
      if (stillGranted && data.territory_id !== mirror) {
        try {
          await postAuthContext(mirror);
          data.territory_id = mirror;
          data.operator_id = null;
        } catch {
          sessionStorage.removeItem(MIRROR_KEY);
        }
      }
      setIsAuth(true);
      setUser(data);
      // Toast en attente après le reload consécutif à une bascule.
      const pending = sessionStorage.getItem(TOAST_KEY);
      if (pending) {
        setSwitchToast(pending);
        sessionStorage.removeItem(TOAST_KEY);
      }
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

  // Réconciliation avec le serveur au retour d'onglet (le miroir ne fait jamais autorité).
  useEffect(() => {
    const reconcile = () => {
      if (document.visibilityState !== "visible" || !isAuth) return;
      void getUserSession().then((data) => {
        if (data?.role && data.role !== "anonymous") setUser(data);
      });
    };
    document.addEventListener("visibilitychange", reconcile);
    window.addEventListener("focus", reconcile);
    return () => {
      document.removeEventListener("visibilitychange", reconcile);
      window.removeEventListener("focus", reconcile);
    };
  }, [isAuth]);

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

  const setFormEditing = (editing: boolean) => {
    formEditingRef.current = editing;
  };

  // Bascule serveur du périmètre actif puis recharge pour ré-hydrater toutes les données de page.
  const switchScope = async (territory_id: number) => {
    if (user?.territory_id === territory_id) return;
    if (formEditingRef.current && !window.confirm("Un formulaire est en cours d'édition. Changer de périmètre ?")) {
      return;
    }
    const { label } = await postAuthContext(territory_id);
    sessionStorage.setItem(MIRROR_KEY, String(territory_id));
    sessionStorage.setItem(TOAST_KEY, `Périmètre actif : ${label}`);
    window.location.reload();
  };

  const logout = () => {
    setIsAuth(false);
    setUser(undefined);
    sessionStorage.removeItem(MIRROR_KEY);
  };

  return (
    <AuthContext.Provider
      value={{
        isAuth,
        setIsAuth,
        user,
        scopes: user?.scopes ?? [],
        activeScope: getActiveScope(user),
        simulate,
        simulatedRole,
        onChangeTerritory,
        onChangeOperator,
        onChangeSimulate,
        onChangeSimulatedRole,
        switchScope,
        setFormEditing,
        logout,
      }}
    >
      {switchToast && (
        <div aria-live="polite" role="status" className={fr.cx("fr-container")} style={{ position: "fixed", top: "1rem", left: 0, right: 0, zIndex: 1000 }}>
          <Alert
            severity="success"
            title="Périmètre changé"
            description={switchToast}
            closable
            onClose={() => setSwitchToast(undefined)}
          />
        </div>
      )}
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

export { activeScopeLabel };
