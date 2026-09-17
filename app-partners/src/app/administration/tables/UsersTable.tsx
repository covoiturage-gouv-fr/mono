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
  const [deleteOutcome, setDeleteOutcome] = useState<"user_deleted" | "scope_released">();
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
  // Limite large : la modale doit résoudre le nom de tous les territoires, l'Autocomplete filtre en local.
  const { data: territoriesData, refetch: refetchTerritories } = useTerritoriesList({ limit: 1000 });
  // Liste complète : un registry.admin porteur d'un scope a un territory_id sans être limité à ce territoire.
  const territoriesList = (): TerritoriesInterface["data"] => territoriesData?.data ?? [];

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

  // currentRow EST le corps de la requête : n'y mettre que des champs acceptés par l'API.
  // scopes_count est une sortie de la liste, et les champs privilégiés sont refusés aux autres rôles.
  const openUpdateModal = (row: UsersInterface["data"][0]) => {
    const scopes = initialScopes(row);
    modal.setCurrentRow({
      id: row.id,
      firstname: row.firstname,
      lastname: row.lastname,
      email: row.email,
      role: row.role,
      operator_id: row.operator_id ?? null,
      territory_id: row.territory_id ?? null,
      ...(canManageScopes ? { scopes, login_siren: row.login_siren ?? suggestSiren(scopes) } : {}),
    });
    modal.setErrors({});
    modal.setOpenModal(true);
    modal.setTypeModal("update");
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
                  onClick: () => openUpdateModal(d),
                },
                {
                  children: "supprimer",
                  iconId: "fr-icon-delete-bin-line",
                  onClick: () => {
                    modal.setCurrentRow(d);
                    modal.setOpenModal(true);
                    setDeleteOutcome(undefined);
                    modal.setTypeModal("delete");
                  },
                },
              ]
            : [
                {
                  children: "modifier",
                  iconId: "fr-icon-refresh-line",
                  priority: "secondary",
                  onClick: () => openUpdateModal(d),
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

  // Un admin de territoire ne fait que libérer son périmètre : le compte survit s'il en porte d'autres.
  const isScopedDelete = !canManageScopes && ((modal.currentRow.role ?? "") as string).startsWith("territory.");

  const targetName = () => `${modal.currentRow?.firstname as string} ${modal.currentRow?.lastname as string}`;

  const defaultDeleteConfirmation = () => `Êtes-vous sûr de vouloir supprimer l'utilisateur ${targetName()} ?`;

  // scopes_count absent tant que l'API ne l'expose pas : on ne préjuge alors d'aucune des deux issues.
  const scopedDeleteConfirmation = () => {
    const count = modal.currentRow?.scopes_count as number | undefined;
    if (count === undefined) {
      return `Confirmez-vous le retrait de ${targetName()} de votre territoire ? Si ce compte n'est rattaché à aucun autre territoire, il sera définitivement supprimé.`;
    }
    return count > 1
      ? `Confirmez-vous le retrait de ${targetName()} de votre territoire ? Le compte sera conservé si la personne dispose d'autres rattachements.`
      : `Confirmez-vous la suppression du compte de ${targetName()} ? Cette action est définitive.`;
  };

  // Type de formulaire décidé par le rôle de l'utilisateur édité, jamais par le contexte de l'admin connecté.
  const targetScopeType = ((modal.currentRow.role ?? "") as string).split(".")[0];
  const isOperatorTarget = targetScopeType === "operator";
  const isTerritoryTarget = targetScopeType === "territory";

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
          title={deleteOutcome === "scope_released" ? "Retrait du territoire réussi" : "Suppression réussie"}
          message={
            deleteOutcome === "scope_released"
              ? "L'utilisateur n'a plus accès à votre territoire. Son compte n'a pas été supprimé."
              : "L'utilisateur a été supprimé."
          }
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
              const scopes: UserScopeInput[] = user?.territory_id
                ? [{ territory_id: user.territory_id, is_default: true }]
                : [];
              modal.setCurrentRow({
                firstname: "",
                lastname: "",
                email: "",
                operator_id: user?.operator_id ?? undefined,
                territory_id: user?.territory_id ?? undefined,
                role: `${user?.role === "registry.admin" ? user?.role : `${user?.role.split(".")[0]}.user`}`,
                // Sans la permission, le périmètre du nouveau compte vient du seul territory_id.
                ...(canManageScopes ? { scopes, login_siren: suggestSiren(scopes) } : {}),
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
            const result = await modal.submitModal("dashboard/user", formSchema);
            setDeleteOutcome(result?.outcome === "scope_released" ? "scope_released" : "user_deleted");
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
              {/* Chaque contrôle est enveloppé dans fr-fieldset__element : le fieldset DSFR est en
                  flex, sans cette enveloppe les champs se dimensionnent au contenu et s'alignent mal. */}
              <fieldset className={fr.cx("fr-fieldset")}>
                <legend className={fr.cx("fr-fieldset__legend")}>Identité</legend>
                <div className={fr.cx("fr-fieldset__element")}>
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
                </div>
                <div className={fr.cx("fr-fieldset__element")}>
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
                </div>
                <div className={fr.cx("fr-fieldset__element")}>
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
                </div>
                <div className={fr.cx("fr-fieldset__element")}>
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
                </div>
              </fieldset>

              {/* Connexion : login_siren réservé registry.admin, masqué (pas grisé) sinon. */}
              {canManageScopes && (
                <fieldset className={fr.cx("fr-fieldset")}>
                  <legend className={fr.cx("fr-fieldset__legend")}>Connexion</legend>
                  <div className={fr.cx("fr-fieldset__element")}>
                    <Input
                      label="SIREN de connexion (ProConnect)"
                      hintText="9 chiffres — distinct du SIRET du territoire"
                      state={modal.errors?.login_siren ? "error" : "default"}
                      stateRelatedMessage={modal.errors?.login_siren ?? ""}
                      nativeInputProps={{
                        inputMode: "numeric",
                        maxLength: 9,
                        value: (modal.currentRow.login_siren as string) ?? "",
                        onChange: (e) => modal.validateInputChange(formSchema, "login_siren", e.target.value),
                      }}
                    />
                  </div>
                </fieldset>
              )}

              {/* Périmètres : masqués pour territory.admin ; opérateur = Select unique, territoire = table éditable. */}
              {(canManageScopes || isOperatorTarget) && (
                <fieldset className={fr.cx("fr-fieldset")}>
                  <legend className={fr.cx("fr-fieldset__legend")}>Périmètres</legend>
                  {isOperatorTarget && (
                    <div className={fr.cx("fr-fieldset__element")}>
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
                    </div>
                  )}
                  {canManageScopes && isTerritoryTarget && !isOperatorTarget && (
                    <div className={fr.cx("fr-fieldset__element")}>
                      <UserScopesEditor
                        scopes={(modal.currentRow.scopes as UserScopeInput[]) ?? []}
                        territories={territoriesList()}
                        onChange={onChangeScopes}
                      />
                    </div>
                  )}
                </fieldset>
              )}
            </>
          )}
          {modal.typeModal === "delete" && (isScopedDelete ? scopedDeleteConfirmation() : defaultDeleteConfirmation())}
        </>
      </Modal>
    </>
  );
}
