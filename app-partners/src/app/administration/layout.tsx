"use client";
import PageTitle from "@/components/common/PageTitle";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import { Tabs } from "@codegouvfr/react-dsfr/Tabs";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";

const routeToTabId: Record<string, string> = {
  "/administration/profil/": "1",
  "/administration/utilisateurs/": "2",
  "/administration/operateurs/": "3",
  "/administration/territoires/": "4",
  "/administration/cles-api/": "5",
};

const tabIdToRoute: Record<string, string> = {
  "1": "/administration/profil/",
  "2": "/administration/utilisateurs/",
  "3": "/administration/operateurs/",
  "4": "/administration/territoires/",
  "5": "/administration/cles-api/",
};

export default function AdministrationLayout({ children }: { children: React.ReactNode }) {
  const { user, simulatedRole } = useAuth();
  const router = useRouter();
  const pathname = usePathname();
  const [activeTab, setActiveTab] = useState("1");

  const tabs = [
    {
      tabId: "1",
      label: "Mon profil",
    },
  ];
  if (["registry.admin", "operator.admin", "territory.admin"].includes(user?.role ?? "")) {
    tabs.push({
      tabId: "2",
      label: "Utilisateurs et accès",
    });
  }
  if (user?.role === "registry.admin" && !simulatedRole) {
    tabs.push({
      tabId: "3",
      label: "Opérateurs",
    });
  }
  if (user?.role === "registry.admin" && !simulatedRole) {
    tabs.push({
      tabId: "4",
      label: "Territoires",
    });
  }
  // Les clés d'API sont réservées aux administrateurs (permission credentials.*), pas à tout compte opérateur.
  if (user?.operator_id && ["operator.admin", "registry.admin"].includes(user.role)) {
    tabs.push({
      tabId: "5",
      label: "Clés d'API",
    });
  }

  // Une page hors des onglets du rôle n'est pas servie : renvoi sur le profil (l'API refuserait de toute façon).
  const allowedTabs = tabs.map((t) => t.tabId).join(",");
  useEffect(() => {
    const tabId = routeToTabId[pathname];
    if (tabId) {
      if (allowedTabs.split(",").includes(tabId)) {
        setActiveTab(tabId);
      } else {
        router.replace("/administration/profil");
      }
    } else if (pathname === "/administration") {
      router.push("/administration/profil");
    }
  }, [pathname, router, allowedTabs]);

  const handleTabChange = (tabId: string) => {
    const route = tabIdToRoute[tabId];
    if (route) {
      router.push(route);
    }
  };

  return (
    <div className={fr.cx("fr-container")}>
      <div id="content">
        <PageTitle title="Gérez votre espace" />
        <Tabs key={activeTab} tabs={tabs} selectedTabId={activeTab} onTabChange={handleTabChange}>
          {children}
        </Tabs>
      </div>
    </div>
  );
}
