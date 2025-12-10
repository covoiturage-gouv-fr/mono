"use client";
import SelectOperator from "@/components/common/SelectOperator";
import SelectTerritory from "@/components/common/SelectTerritory";
import { labelRole } from "@/helpers/auth";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import { RadioButtons } from "@codegouvfr/react-dsfr/RadioButtons";
import { ToggleSwitch } from "@codegouvfr/react-dsfr/ToggleSwitch";
import { useEffect } from "react";

export default function ProfilPage() {
  const {
    user,
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
  }, [simulate]);

  return (
    <>
      {user && (
        <>
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
          {user.operator_id && (
            <p>
              <b>Identifiant de l&apos;opérateur:</b> {user.operator_id}
            </p>
          )}
          {user.territory_id && (
            <p>
              <b>Identifiant du territoire:</b> {user.territory_id}
            </p>
          )}
          <p>
            <a
              href={`https://identite.proconnect.gouv.fr/personal-information`}
              target="_blank"
              rel="noopener noreferrer"
              aria-label="Modifier mes informations sur ProConnect (s'ouvre dans un nouvel onglet)"
            >
              Modifier mes informations sur ProConnect
            </a>
          </p>
          {user.role === "registry.admin" && (
            <>
              <ToggleSwitch
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
            </>
          )}
        </>
      )}
    </>
  );
}
