import UserScopesEditor from "@/components/administration/UserScopesEditor";
import AlertMessage from "@/components/common/AlertMessage";
import { Modal } from "@/components/common/Modal";
import Pagination from "@/components/common/Pagination";
import { getRolesList, labelRole } from "@/helpers/auth";
import { useOperatorsList, useTerritoriesList, useUsersList } from "@/hooks/api";
import { useActionsModal } from "@/hooks/useActionsModal";
import { useUrlSearch } from "@/hooks/useUrlSearch";
import { roles } from "@/interfaces/auth";
import {
  UsersInterface,
  type OperatorsInterface,
  type TerritoriesInterface,
  type UserScopeInput,
} from "@/interfaces/dataInterface";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import Button from "@codegouvfr/react-dsfr/Button";
import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";
import Input from "@codegouvfr/react-dsfr/Input";
import Select from "@codegouvfr/react-dsfr/Select";
import Table from "@codegouvfr/react-dsfr/Table";
import { useEffect, useState } from "react";
import { z } from "zod";

export default function UsersTable(props: { title: string; territoryId: number | null; operatorId: number | null }) {
  const { user, simulatedRole, setFormEditing } = useAuth();
  const [currentPage, setCurrentPage] = useState(1);
  const { search, debouncedSearch, onChangeSearch: setSearchValue } = useUrlSearch();
  const modal = useActionsModal<UsersInterface["data"][0]>();
  const [alert, setAlert] = useState<"create" | "update" | "delete" | "error">();
  const onChangePage = (id: number) => {
    setCurrentPage(id);
  };
  const onChangeSearch = (search: string) => {
    setSearchValue(search);
    setCurrentPage(1);
  };

  // registry.admin seul manipule login_siren / octroi de scope (miroir de la permission back).
  const canManageScopes = user?.role === "registry.admin";

  // Garde-fou : signale une édition en cours pour la confirmation de bascule de périmètre.
  useEffect(() => {
    setFormEditing(modal.openModal && (modal.typeModal === "create" || modal.typeModal === "update"));
    return () => setFormEditing(false);
  }, [modal.openModal, modal.typeModal, setFormEditing]);

  const { data, refetch: refetchUsers } = useUsersList({
    territoryId: props.territoryId,
    operatorId: props.operatorId,
    page: currentPage,
    search: debouncedSearch || undefined,
  });
  const totalPages = data?.meta.totalPages ?? 1;
  const totalRecords = data?.meta.total ?? 0;

  const headers = ["Prénom", "Nom", "Adresse mail", "Rôle", "Opérateur", "Territoire", "Actions"];
  const { data: operatorsData, refetch: refetchOperators } = useOperatorsList({ limit: 100 });
  const operatorsList = () => {
    if (user?.operator_id) {
      return [operatorsData?.data.find((t) => t.id === user?.operator_id)] as OperatorsInterface["data"];
    }
    return operatorsData?.data ?? [];
  };
  const { data: territoriesData, refetch: refetchTerritories } = useTerritoriesList({ limit: 200 });
  const territoriesList = () => {
    if (user?.territory_id) {
      return [territoriesData?.data.find((t) => t._id === user?.territory_id)] as TerritoriesInterface["data"];
    }
    return territoriesData?.data ?? [];
  };

  // Périmètres initiaux d'une ligne (fallback sur la colonne legacy si l'API ne renvoie pas encore scopes).
  const initialScopes = (row: Partial<UsersInterface["data"][0]>): UserScopeInput[] => {
    if (row.scopes?.length) return row.scopes;
    if (row.territory_id) return [{ territory_id: row.territory_id, is_default: true }];
    return [];
  };

  // Suggestion login_siren = 9 premiers chiffres du SIRET du territoire par défaut.
  const suggestSiren = (scopes: UserScopeInput[]): string => {
    const def = scopes.find((s) => s.is_default) ?? scopes[0];
    const siret = territoriesList().find((t) => t?._id === def?.territory_id)?.siret;
    return siret ? siret.slice(0, 9) : "";
  };

  const dataTable =
    data?.data?.map((d) => [
      d.firstname,
      d.lastname,
      d.email,
      labelRole(d.role),
      operatorsList().find((o) => o?.id === d.operator_id)?.name,
      territoriesList().find((t) => t?._id === d.territory_id)?.name,
      <ButtonsGroup
        key={d.id}
        buttons={
          d.email !== user?.email
            ? [
                {
                  children: "modifier",
                  iconId: "fr-icon-refresh-line",
                  priority: "secondary",
                  onClick: () => {
                    modal.setCurrentRow({ ...d, scopes: initialScopes(d) });
                    modal.setErrors({});
                    modal.setOpenModal(true);
                    modal.setTypeModal("update");
                  },
                },
                {
                  children: "supprimer",
                  iconId: "fr-icon-delete-bin-line",
                  onClick: () => {
                    modal.setCurrentRow(d);
                    modal.setOpenModal(true);
                    modal.setTypeModal("delete");
                  },
                },
              ]
            : [
                {
                  children: "modifier",
                  iconId: "fr-icon-refresh-line",
                  priority: "secondary",
                  onClick: () => {
                    modal.setCurrentRow({ ...d, scopes: initialScopes(d) });
                    modal.setErrors({});
                    modal.setOpenModal(true);
                    modal.setTypeModal("update");
                  },
                },
              ]
        }
        buttonsSize="small"
        inlineLayoutWhen="lg and up"
      />,
    ]) ?? [];

  const formSchema = z.object({
    firstname: z.string().min(3, { message: "Le prénom doit contenir au moins 3 caractères" }),
    lastname: z.string().min(3, { message: "Le nom doit contenir au moins 3 caractères" }),
    email: z.string().email({ message: `L'adresse mail n'est pas valide` }),
    operator_id: z.coerce.number({ message: "L'identifiant n'est pas un nombre" }).nullable(),
    territory_id: z.coerce.number({ message: "L'identifiant n'est pas un nombre" }).nullable(),
    role: z.enum(roles, { message: "Le rôle n'est pas valide" }),
    login_siren: z
      .union([z.string().regex(/^\d{9}$/, { message: "Le SIREN doit contenir 9 chiffres" }), z.literal(""), z.null()])
      .optional(),
    scopes: z
      .array(
        z.object({
          territory_id: z.number().optional(),
          operator_id: z.number().optional(),
          is_default: z.boolean().optional(),
        }),
      )
      .optional(),
  });
  const roleList = () => {
    if (simulatedRole) {
      if (user?.territory_id) {
        return getRolesList("territory.admin");
      }
      if (user?.operator_id) {
        return getRolesList("operator.admin");
      }
    }
    return getRolesList(user?.role ?? "anonymous");
  };

  const currentRole = (modal.currentRow.role ?? "") as string;
  const isOperatorTarget = currentRole.split(".")[0] === "operator" || !!user?.operator_id;
  const isTerritoryTarget = currentRole.split(".")[0] === "territory" || !!user?.territory_id;

  // Met à jour les périmètres, resynchronise territory_id (dual-write legacy) et suggère le SIREN si vide.
  const onChangeScopes = (scopes: UserScopeInput[]) => {
    const def = scopes.find((s) => s.is_default) ?? scopes[0];
    modal.setCurrentRow((prev) => ({
      ...prev,
      scopes,
      territory_id: def?.territory_id,
      login_siren: (prev.login_siren as string) || suggestSiren(scopes),
    }));
  };

  return (
    <>
      {alert === "delete" && (
        <AlertMessage
          title="Suppression réussie"
          message="L'utilisateur a été supprimé."
          typeAlert={alert}
          onClose={() => setAlert(undefined)}
        />
      )}
      {alert === "create" && (
        <AlertMessage
          title="Utilisateur ajouté avec succès"
          message="L'utilisateur a été enregistré dans la base de données."
          typeAlert={alert}
          onClose={() => setAlert(undefined)}
        />
      )}
      {alert === "update" && (
        <AlertMessage
          title="Utilisateur modifié avec succès"
          message="L'utilisateur a été enregistré dans la base de données."
          typeAlert={alert}
          onClose={() => setAlert(undefined)}
        />
      )}
      {alert === "error" && (
        <AlertMessage
          title="Une erreur s'est produite"
          message={Object.values(modal.errors!).join(" | ")}
          typeAlert={alert}
          onClose={() => setAlert(undefined)}
        />
      )}

      <h3 className={fr.cx("fr-callout__title")}>{props.title}</h3>
      {user?.role.split(".")[1] === "admin" && (
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", marginBottom: "1rem" }}>
          <Button
            iconId="fr-icon-add-circle-line"
            onClick={() => {
              modal.setCurrentRow({
                firstname: "",
                lastname: "",
                email: "",
                operator_id: user?.operator_id ?? undefined,
                territory_id: user?.territory_id ?? undefined,
                scopes: user?.territory_id ? [{ territory_id: user.territory_id, is_default: true }] : [],
                role: `${user?.role === "registry.admin" ? user?.role : `${user?.role.split(".")[0]}.user`}`,
              });
              modal.setOpenModal(true);
              modal.setErrors({});
              modal.setTypeModal("create");
            }}
            title="Ajouter un utilisateur"
            size="small"
          >
            Ajouter
          </Button>
          <Input
            label="Rechercher"
            state={search !== "" ? (totalRecords <= 0 ? "error" : "success") : "default"}
            stateRelatedMessage={totalRecords + " résultats"}
            hintText="Nom / Prénom / Adresse mail / Opérateur / Territoire"
            nativeInputProps={{
              type: "text",
              value: search ?? "",
              onChange: (e) => onChangeSearch(e.target.value),
            }}
          />
        </div>
      )}
      <Table data={dataTable} headers={headers} colorVariant="blue-ecume" fixed />
      <Pagination count={totalPages} defaultPage={currentPage} onChange={onChangePage} />
      <Modal
        open={modal.openModal}
        title={modal.modalTitle(modal.typeModal)}
        onOpen={async () => {
          if (modal.typeModal === "update" || modal.typeModal === "create") {
            await refetchOperators();
            await refetchTerritories();
          }
        }}
        onClose={() => modal.setOpenModal(false)}
        onSubmit={async () => {
          try {
            await modal.submitModal("dashboard/user", formSchema);
            setAlert(modal.typeModal);
          } catch {
            setAlert("error");
          }
          await refetchUsers();
        }}
      >
        <>
          {(modal.typeModal === "update" || modal.typeModal === "create") && (
            <>
              <fieldset className={fr.cx("fr-fieldset")}>
                <legend className={fr.cx("fr-fieldset__legend")}>Identité</legend>
                <Input
                  label="Prénom"
                  state={modal.errors?.firstname ? "error" : "default"}
                  stateRelatedMessage={modal.errors?.firstname ?? ""}
                  nativeInputProps={{
                    type: "text",
                    value: (modal.currentRow.firstname as string) ?? "",
                    onChange: (e) => modal.validateInputChange(formSchema, "firstname", e.target.value),
                  }}
                />
                <Input
                  label="Nom"
                  state={modal.errors?.lastname ? "error" : "default"}
                  stateRelatedMessage={modal.errors?.lastname ?? ""}
                  nativeInputProps={{
                    type: "text",
                    value: (modal.currentRow.lastname as string) ?? "",
                    onChange: (e) => modal.validateInputChange(formSchema, "lastname", e.target.value),
                  }}
                />
                <Input
                  label="Adresse mail"
                  state={modal.errors?.email ? "error" : "default"}
                  stateRelatedMessage={modal.errors?.email ?? ""}
                  nativeInputProps={{
                    type: "text",
                    value: (modal.currentRow.email as string) ?? "",
                    onChange: (e) => modal.validateInputChange(formSchema, "email", e.target.value),
                  }}
                />
                <Select
                  label="Rôle"
                  nativeSelectProps={{
                    value: (modal.currentRow.role ?? "") as string,
                    onChange: (e) => modal.validateInputChange(formSchema, "role", e.target.value),
                  }}
                >
                  {roleList().map((r: string, i: number) => (
                    <option key={i} value={r}>
                      {labelRole(r)}
                    </option>
                  ))}
                </Select>
              </fieldset>

              {/* Connexion : login_siren réservé registry.admin, masqué (pas grisé) sinon. */}
              {canManageScopes && (
                <fieldset className={fr.cx("fr-fieldset")}>
                  <legend className={fr.cx("fr-fieldset__legend")}>Connexion</legend>
                  <Input
                    label="SIREN de connexion (ProConnect)"
                    hintText="9 chiffres — distinct du SIRET du territoire"
                    state={modal.errors?.login_siren ? "error" : "default"}
                    stateRelatedMessage={modal.errors?.login_siren ?? ""}
                    nativeInputProps={{
                      inputMode: "numeric",
                      maxLength: 9,
                      value: (modal.currentRow.login_siren as string) ?? suggestSiren((modal.currentRow.scopes as UserScopeInput[]) ?? []),
                      onChange: (e) => modal.validateInputChange(formSchema, "login_siren", e.target.value),
                    }}
                  />
                </fieldset>
              )}

              {/* Périmètres : masqués pour territory.admin ; opérateur = Select unique, territoire = table éditable. */}
              {(canManageScopes || isOperatorTarget) && (
                <fieldset className={fr.cx("fr-fieldset")}>
                  <legend className={fr.cx("fr-fieldset__legend")}>Périmètres</legend>
                  {isOperatorTarget && (
                    <Select
                      label="Opérateur"
                      nativeSelectProps={{
                        value: (modal.currentRow.operator_id as number) ?? undefined,
                        onChange: (e) => modal.validateInputChange(formSchema, "operator_id", e.target.value),
                      }}
                    >
                      {canManageScopes && <option value={undefined}>aucun</option>}
                      {operatorsList().map((o, i) => (
                        <option key={i} value={o?.id}>
                          {o?.name}
                        </option>
                      ))}
                    </Select>
                  )}
                  {canManageScopes && isTerritoryTarget && !isOperatorTarget && (
                    <UserScopesEditor
                      scopes={(modal.currentRow.scopes as UserScopeInput[]) ?? []}
                      territories={territoriesList()}
                      onChange={onChangeScopes}
                    />
                  )}
                </fieldset>
              )}
            </>
          )}
          {modal.typeModal === "delete" &&
            `Êtes-vous sûr de vouloir supprimer l'utilisateur ${modal.currentRow?.firstname as string} ${modal.currentRow?.lastname as string} ?`}
        </>
      </Modal>
    </>
  );
}
