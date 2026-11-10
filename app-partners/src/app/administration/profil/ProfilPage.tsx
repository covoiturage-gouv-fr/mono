"use client";
import SelectOperator from "@/components/common/SelectOperator";
import SelectTerritory from "@/components/common/SelectTerritory";
import { Config } from "@/config";
import { activeScopeLabel, labelRole, proconnectAccountUrl } from "@/helpers/auth";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import { RadioButtons } from "@codegouvfr/react-dsfr/RadioButtons";
import { ToggleSwitch } from "@codegouvfr/react-dsfr/ToggleSwitch";
import { useEffect } from "react";

export default function ProfilPage() {
  const {
    user,
    scopes,
    activeScope,
    simulate,
    simulatedRole,
    onChangeSimulate,
    onChangeSimulatedRole,
    onChangeTerritory,
    onChangeOperator,
  } = useAuth();

  useEffect(() => {
    if (user?.role === "registry.admin" && simulate === false) {
      onChangeSimulatedRole(undefined);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- ne réagir qu'à la bascule du mode simulate
  }, [simulate]);

  return (
    <>
      {user && (
        <div className={fr.cx("fr-grid-row", "fr-grid-row--gutters")}>
          <div className={fr.cx("fr-col-12", "fr-col-md-6")}>
            <h3 className={fr.cx("fr-callout__title")}>Mon profil</h3>
            {user.name && (
              <p>
                <b>Nom:</b> {user.name}
              </p>
            )}
            <p>
              <b>Mail:</b> {user.email}
            </p>
            <p>
              <b>Rôle:</b> {labelRole(user.role)}
            </p>
            {activeScopeLabel(user) && (
              <p>
                <b>Périmètre actif:</b> {activeScopeLabel(user)}
                {activeScope?.siret && ` (SIRET ${activeScope.siret})`}
              </p>
            )}
            {user.login_siren && (
              <p>
                <b>SIREN de connexion ProConnect:</b> {user.login_siren}
              </p>
            )}
            {scopes.length > 1 && (
              <>
                <p className={fr.cx("fr-mb-1w")}>
                  <b>Périmètres accessibles:</b>
                </p>
                <ul className={fr.cx("fr-mb-3w")}>
                  {scopes.map((s) => (
                    <li key={s.operator_id ?? s.territory_id}>
                      {s.label}
                      {s.siret && ` (SIRET ${s.siret})`}
                    </li>
                  ))}
                </ul>
              </>
            )}
            <p>
              <a
                href={proconnectAccountUrl(Config.get<string>("auth.pc_uri", ""))}
                target="_blank"
                rel="noopener noreferrer"
                aria-label="Modifier mes informations sur ProConnect (s'ouvre dans un nouvel onglet)"
              >
                Modifier mes informations sur ProConnect
              </a>
            </p>
          </div>
          {user.role === "registry.admin" && (
            <div className={fr.cx("fr-col-12", "fr-col-md-6")}>
              <h3 className={fr.cx("fr-callout__title")}>Simulation</h3>
              <ToggleSwitch
                className={fr.cx("fr-mb-3w")}
                label="S'identifier en tant qu'opérateur ou territoire"
                checked={simulate}
                onChange={(state) => {
                  onChangeSimulate(state);
                }}
              />
              {simulate && (
                <>
                  <RadioButtons
                    legend="S'identifier en tant que :"
                    name="radio"
                    options={[
                      {
                        label: "Territoire",
                        nativeInputProps: {
                          checked: simulatedRole === "territory",
                          onChange: () => onChangeSimulatedRole("territory"),
                        },
                      },
                      {
                        label: "opérateur",
                        nativeInputProps: {
                          checked: simulatedRole === "operator",
                          onChange: () => onChangeSimulatedRole("operator"),
                        },
                      },
                    ]}
                    orientation="horizontal"
                  />
                  {simulatedRole === "territory" && (
                    <SelectTerritory defaultValue={user.territory_id} onChange={onChangeTerritory} />
                  )}
                  {simulatedRole === "operator" && (
                    <SelectOperator defaultValue={user.operator_id} onChange={onChangeOperator} />
                  )}
                </>
              )}
            </div>
          )}
        </div>
      )}
    </>
  );
}
