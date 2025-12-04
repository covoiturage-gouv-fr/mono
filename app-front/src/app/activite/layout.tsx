"use client";
import PageTitle from "@/components/common/PageTitle";
import { fr } from "@codegouvfr/react-dsfr";
import { Tabs } from "@codegouvfr/react-dsfr/Tabs";
import Tile from "@codegouvfr/react-dsfr/Tile";
import Image from "next/image";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";

const tabs = [
  {
    tabId: "1",
    label: "Export des données",
  },
  {
    tabId: "2",
    label: `Suivi des campagnes`,
  },
];

const routeToTabId: Record<string, string> = {
  "/activite/export/": "1",
  "/activite/campagnes/": "2",
};

const tabIdToRoute: Record<string, string> = {
  "1": "/activite/export/",
  "2": "/activite/campagnes/",
};

export default function ActiviteLayout({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const [activeTab, setActiveTab] = useState("1");

  useEffect(() => {
    let tabId = undefined;
    for (const [route, id] of Object.entries(routeToTabId)) {
      if (pathname === route || pathname.startsWith(route)) {
        tabId = id;
        break;
      }
    }

    if (tabId) {
      setActiveTab(tabId);
    } else if (pathname === "/activite") {
      router.push("/activite/export");
    }
  }, [pathname, router]);

  const handleTabChange = (tabId: string) => {
    const route = tabIdToRoute[tabId];
    if (route) {
      router.push(route);
    }
  };

  return (
    <div className={fr.cx("fr-container")}>
      <div id="content">
        <PageTitle title={`Suivez votre activité`} />
        <Tabs key={activeTab} tabs={tabs} selectedTabId={activeTab} onTabChange={handleTabChange}>
          {children}
        </Tabs>
        <Tile
          enlargeLinkOrButton
          linkProps={{
            href: "https://observatoire.covoiturage.gouv.fr/observatoire/territoire/",
            target: "_blank",
          }}
          orientation="horizontal"
          title="Accédez à de nombreux autres graphiques et indicateurs sur votre tableau de bord territoire de l’observatoire national du covoiturage"
          titleAs="h3"
          start={
            <Image src="https://static.covoiturage.beta.gouv.fr/Obs_021e3f4a41.svg" alt="" width={70} height={70} />
          }
          className={fr.cx("fr-mt-10v")}
        />
      </div>
    </div>
  );
}
