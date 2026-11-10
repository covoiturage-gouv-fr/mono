"use client";
import { features } from "@/config/features";
import PageTitle from "@/components/common/PageTitle";
import { administrationTabs, tabIdFromPathname, visibleTabIds } from "@/helpers/administrationTabs";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect } from "react";

export default function AdministrationLayout({ children }: { children: React.ReactNode }) {
  const { user, simulatedRole } = useAuth();
  const router = useRouter();
  const pathname = usePathname();

  const visibleIds = visibleTabIds(user ?? undefined, !!simulatedRole, features.externalCampaigns);
  const tabs = administrationTabs.filter((t) => visibleIds.includes(t.id));
  const activeId = tabIdFromPathname(pathname);
  const allowedIds = visibleIds.join(",");

  // Une page hors des onglets du rôle n'est pas servie : renvoi sur le profil (l'API refuserait de toute façon).
  useEffect(() => {
    const id = tabIdFromPathname(pathname);
    if (id) {
      if (!allowedIds.split(",").includes(id)) router.replace("/administration/profil/");
    } else if (pathname.replace(/\/$/, "") === "/administration") {
      router.replace("/administration/profil/");
    }
  }, [pathname, router, allowedIds]);

  return (
    <div className={fr.cx("fr-container")}>
      <div id="content">
        <PageTitle title="Gérez votre espace" />
        {/* Onglets-liens : l'URL fait foi, les <button> du composant Tabs ne gardent pas l'état en synchro avec elle. */}
        <div className={fr.cx("fr-tabs")}>
          <ul className={fr.cx("fr-tabs__list")}>
            {tabs.map((t) => (
              <li key={t.id}>
                <Link
                  href={t.href}
                  className={fr.cx("fr-tabs__tab")}
                  aria-current={t.id === activeId ? "page" : undefined}
                  aria-selected={t.id === activeId}
                >
                  {t.label}
                </Link>
              </li>
            ))}
          </ul>
          <div className={fr.cx("fr-tabs__panel", "fr-tabs__panel--selected")}>{children}</div>
        </div>
      </div>
    </div>
  );
}
