"use client";
import { activeScopeLabel, getActiveScope, getUserSession, postAuthContext } from "@/helpers/auth";
import { toUserError } from "@/hooks/useApi";
import { type AuthContextProps, type UserInterface } from "@/interfaces/auth";
import { fr } from "@codegouvfr/react-dsfr";
import Alert from "@codegouvfr/react-dsfr/Alert";
import Button from "@codegouvfr/react-dsfr/Button";
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
  // API injoignable au chargement : page d'erreur lisible plutôt qu'un écran vide.
  const [authError, setAuthError] = useState<Error>();
  const [switchToast, setSwitchToast] = useState<{ severity: "success" | "error"; description: string }>();
  const formEditingRef = useRef(false);
  const router = useRouter();
  const pathname = usePathname();

  // Ré-assertion : si la session serveur est repartie sur le défaut, restaurer le scope choisi.
  const assertMirror = async (data: UserInterface) => {
    const mirror = Number(sessionStorage.getItem(MIRROR_KEY)) || null;
    const stillGranted = mirror && data.scopes?.some((s) => s.territory_id === mirror);
    if (!stillGranted || data.territory_id === mirror) return;
    try {
      await postAuthContext(mirror);
      data.territory_id = mirror;
      data.operator_id = null;
    } catch {
      sessionStorage.removeItem(MIRROR_KEY);
    }
  };

  const checkAuth = async () => {
    let data: UserInterface | undefined;
    try {
      data = await getUserSession();
      setAuthError(undefined);
    } catch (e) {
      setAuthError(toUserError(e));
      setLoading(false);
      return;
    }
    if (data?.role && data?.role !== "anonymous") {
      await assertMirror(data);
      setIsAuth(true);
      setUser(data);
      // Toast en attente après le reload consécutif à une bascule.
      const pending = sessionStorage.getItem(TOAST_KEY);
      if (pending) {
        setSwitchToast({ severity: "success", description: pending });
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
    if (!loading && !authError) {
      if (!isAuth) {
        router.push("/");
      } else if (isAuth && pathname === "/") {
        router.push("/activite");
      }
    }
  }, [loading, authError, isAuth, pathname, router]);

  // Réconciliation avec le serveur au retour d'onglet (le miroir ne fait jamais autorité).
  useEffect(() => {
    const reconcile = () => {
      if (document.visibilityState !== "visible" || !isAuth) return;
      getUserSession()
        .then(async (data) => {
          if (!data?.role || data.role === "anonymous") return;
          await assertMirror(data);
          // Le tuple simulé n'existe qu'en local : le serveur ne doit pas l'écraser.
          setUser((prev) =>
            prev && simulate ? { ...data, territory_id: prev.territory_id, operator_id: prev.operator_id } : data,
          );
        })
        .catch(() => {
          // Reconcile opportuniste : un /auth/me en échec ne doit pas casser la page.
        });
    };
    document.addEventListener("visibilitychange", reconcile);
    window.addEventListener("focus", reconcile);
    return () => {
      document.removeEventListener("visibilitychange", reconcile);
      window.removeEventListener("focus", reconcile);
    };
  }, [isAuth, simulate]);

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
    try {
      const { label } = await postAuthContext(territory_id);
      sessionStorage.setItem(MIRROR_KEY, String(territory_id));
      if (label) sessionStorage.setItem(TOAST_KEY, `Périmètre actif : ${label}`);
      window.location.reload();
    } catch (error) {
      const description = error instanceof Error ? error.message : "Bascule de périmètre impossible";
      setSwitchToast({ severity: "error", description });
      // La session serveur fait foi après un refus : on réaligne l'affichage.
      await checkAuth().catch(() => undefined);
    }
  };

  const logout = () => {
    setIsAuth(false);
    setUser(undefined);
    sessionStorage.removeItem(MIRROR_KEY);
    sessionStorage.removeItem(TOAST_KEY);
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
        <div
          aria-live="polite"
          role="status"
          className={fr.cx("fr-container")}
          style={{
            position: "fixed",
            top: "1rem",
            left: 0,
            right: 0,
            zIndex: 1000,
            backgroundColor: "var(--background-default-grey)",
          }}
        >
          <Alert
            severity={switchToast.severity}
            title={switchToast.severity === "error" ? "Bascule impossible" : "Périmètre changé"}
            description={switchToast.description}
            closable
            onClose={() => setSwitchToast(undefined)}
          />
        </div>
      )}
      {authError ? (
        <div className={fr.cx("fr-container", "fr-my-7w")}>
          <Alert severity="error" title="Une erreur s'est produite" description={authError.message} />
          <Button
            className={fr.cx("fr-mt-3w")}
            iconId="fr-icon-refresh-line"
            onClick={() => {
              setLoading(true);
              void checkAuth();
            }}
          >
            Réessayer
          </Button>
        </div>
      ) : (
        !loading && children
      )}
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
