"use client";
import { Config } from "@/config";
import { features } from "@/config/features";
import { activeScopeLabel, labelRole, switchableScopes } from "@/helpers/auth";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import { useId } from "react";

// Accès rapide « compte » du header : identité, périmètre actif, bascule de territoire, profil, déconnexion.
// Même mécanique DSFR que le sélecteur de langue (bouton + fr-collapse), rendue deux fois par le header
// (barre + menu mobile), d'où l'id généré par instance.
export function UserMenu() {
  const { isAuth, user, activeScope, switchScope, logout } = useAuth();
  const menuId = `user-menu-${useId()}`;
  if (!isAuth || !user) return null;

  const scopeLabel = activeScopeLabel(user);
  const territories = switchableScopes(user, features.multiSiret);

  const authLogout = () => {
    logout();
    // eslint-disable-next-line @next/next/no-location-assign-relative-destination -- URL absolue de l'API, rechargement complet voulu
    window.location.href = `${Config.get<string>("auth.domain")}/auth/logout`;
  };

  return (
    <nav className={`${fr.cx("fr-translate", "fr-nav")} user-menu`} aria-label="Compte utilisateur">
      <div className={fr.cx("fr-nav__item")}>
        <button
          type="button"
          className={fr.cx("fr-translate__btn", "fr-btn", "fr-btn--tertiary")}
          aria-controls={menuId}
          aria-expanded={false}
          title="Compte utilisateur"
        >
          <span className={fr.cx("fr-icon-account-circle-line", "fr-mr-1w")} aria-hidden="true" />
          <span className="user-menu__summary">
            <span>{user.name ?? user.email}</span>
            {scopeLabel && <span className={fr.cx("fr-text--xs", "fr-mb-0")}>{scopeLabel}</span>}
          </span>
        </button>
        <div className={fr.cx("fr-collapse", "fr-translate__menu", "fr-menu")} id={menuId}>
          <ul className={fr.cx("fr-menu__list")}>
            <li className={`${fr.cx("fr-p-2w")} user-menu__identity`}>
              <p className={fr.cx("fr-text--sm", "fr-mb-0")}>
                <b>{user.name ?? "—"}</b>
              </p>
              <p className={fr.cx("fr-text--xs", "fr-mb-0")}>{user.email}</p>
              <p className={fr.cx("fr-text--xs", "fr-mb-0")}>{labelRole(user.role)}</p>
              {user.login_siren && (
                <p className={fr.cx("fr-text--xs", "fr-mb-0")}>SIREN de connexion : {user.login_siren}</p>
              )}
            </li>
            {territories.length > 0 && (
              <li>
                <p className={`${fr.cx("fr-text--xs", "fr-mb-0", "fr-px-2w", "fr-pt-1w")} user-menu__heading`}>
                  Périmètre actif
                </p>
                <ul className={fr.cx("fr-menu__list")} aria-label="Territoires">
                  {territories.map((t) => {
                    const isActive = t.territory_id === activeScope?.territory_id;
                    return (
                      <li key={t.territory_id}>
                        <button
                          type="button"
                          className={fr.cx("fr-nav__link")}
                          aria-current={isActive ? "true" : undefined}
                          onClick={() => {
                            // L'échec de bascule est déjà remonté en toast par switchScope.
                            if (t.territory_id) switchScope(t.territory_id).catch(() => undefined);
                          }}
                        >
                          {t.label}
                        </button>
                      </li>
                    );
                  })}
                </ul>
              </li>
            )}
            <li>
              <a className={fr.cx("fr-nav__link", "fr-icon-user-line", "fr-link--icon-left")} href="/administration/profil">
                Mon profil
              </a>
            </li>
            <li>
              <button
                type="button"
                className={fr.cx("fr-nav__link", "fr-icon-logout-box-r-line", "fr-link--icon-left")}
                onClick={authLogout}
              >
                Déconnexion
              </button>
            </li>
          </ul>
        </div>
      </div>
    </nav>
  );
}
