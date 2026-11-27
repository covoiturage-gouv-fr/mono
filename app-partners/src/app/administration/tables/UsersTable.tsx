import UserScopesEditor from "@/components/administration/UserScopesEditor";
import AdminTable from "@/components/common/AdminTable";
import AlertMessage from "@/components/common/AlertMessage";
import { Modal } from "@/components/common/Modal";
import Pagination from "@/components/common/Pagination";
import TerritoryAutocomplete from "@/components/common/TerritoryAutocomplete";
import { features } from "@/config/features";
import { type TerritoryOption } from "@/helpers/api";
import { getRolesList, isEditableRole, isOperator, isTerritory, labelRole } from "@/helpers/auth";
import { operatorSiret, siretAfterRoleChange, siretAfterScopesChange, suggestSiret, userFormSchema } from "@/helpers/userForm";
import { roleColor, territoriesCell } from "@/helpers/usersTable";
import { useOperatorsList, useTerritoriesList, useUsersList } from "@/hooks/api";
import { FormValidationError, useActionsModal } from "@/hooks/useActionsModal";
import { useUrlSearch } from "@/hooks/useUrlSearch";
import {
  UsersInterface,
  type OperatorsInterface,
  type TerritoriesInterface,
  type UserScopeInput,
} from "@/interfaces/dataInterface";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import Alert from "@codegouvfr/react-dsfr/Alert";
import Button, { type ButtonProps } from "@codegouvfr/react-dsfr/Button";
import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";
import Input from "@codegouvfr/react-dsfr/Input";
import Select from "@codegouvfr/react-dsfr/Select";
import Tooltip from "@codegouvfr/react-dsfr/Tooltip";
import { useEffect, useState } from "react";

// Texte tronqué sur une ligne, complet au survol.
const ellipsis = (text?: string) =>
  text ? (
    <span className="cell-ellipsis" title={text}>
      {text}
    </span>
  ) : null;

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

  // registry.admin seul manipule login_siret / octroi de scope (miroir de la permission back).
  const canManageScopes = user?.role === "registry.admin";

  // Garde-fou : signale une édition en cours pour la confirmation de bascule de périmètre.
  useEffect(() => {
    setFormEditing(modal.openModal && (modal.typeModal === "create" || modal.typeModal === "update"));
    return () => setFormEditing(false);
  }, [modal.openModal, modal.typeModal, setFormEditing]);

  const {
    data,
    error: usersError,
    refetch: refetchUsers,
  } = useUsersList({
    territoryId: props.territoryId,
    operatorId: props.operatorId,
    page: currentPage,
    search: debouncedSearch || undefined,
  });
  const totalPages = data?.meta.totalPages ?? 1;
  const totalRecords = data?.meta.total ?? 0;

  const headers = ["Nom", "Adresse mail", "Rôle", "Entités", "Actions"];
  const { data: operatorsData, refetch: refetchOperators } = useOperatorsList({ limit: 100 });
  const operatorsList = () => {
    if (user?.operator_id) {
      return [operatorsData?.data.find((t) => t.id === user?.operator_id)] as OperatorsInterface["data"];
    }
    return operatorsData?.data ?? [];
  };
  // Hors registry.admin l'API masque scopes : seul le territoire de l'admin connecté reste à nommer
  // dans la colonne. La modale, elle, cherche les territoires côté serveur (TerritoryAutocomplete).
  const { data: territoriesData } = useTerritoriesList({ limit: 1, territory_id: props.territoryId });
  const territoriesList = (): TerritoriesInterface["data"] => territoriesData?.data ?? [];

  // Périmètres initiaux d'une ligne (fallback sur la colonne legacy si l'API ne renvoie pas encore scopes).
  const initialScopes = (row: Partial<UsersInterface["data"][0]>): UserScopeInput[] => {
    if (row.scopes?.length) return row.scopes;
    if (row.territory_id) return [{ territory_id: row.territory_id, is_default: true }];
    return [];
  };

  // Périmètre par défaut du formulaire, sous la forme attendue par l'autocomplétion.
  const defaultTerritory = (): TerritoryOption | null => {
    const scopes = (modal.currentRow.scopes as UserScopeInput[] | undefined) ?? [];
    const def = scopes.find((s) => s.is_default) ?? scopes[0];
    if (!def?.territory_id) return null;
    return { _id: def.territory_id, name: def.territory_name ?? `#${def.territory_id}`, siret: def.siret ?? null };
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
      ...(canManageScopes
        ? {
            scopes,
            login_siret:
              row.login_siret ??
              (isOperator(row.role) ? operatorSiret(operatorsList(), row.operator_id ?? null) : suggestSiret(scopes)),
          }
        : {}),
    });
    modal.setErrors({});
    modal.setOpenModal(true);
    modal.setTypeModal("update");
  };

  // Un compte au rôle non attribuable (demo, registry.user…) serait refusé par l'API : pas de bouton.
  const rowActions = (d: UsersInterface["data"][0]) => {
    const buttons: ButtonProps[] = [];
    if (isEditableRole(d.role)) {
      buttons.push({
        children: "modifier",
        iconId: "fr-icon-refresh-line" as const,
        priority: "secondary" as const,
        onClick: () => openUpdateModal(d),
      });
    }
    if (d.email !== user?.email) {
      buttons.push({
        children: "supprimer",
        iconId: "fr-icon-delete-bin-line" as const,
        onClick: () => {
          modal.setCurrentRow(d);
          modal.setOpenModal(true);
          setDeleteOutcome(undefined);
          modal.setTypeModal("delete");
        },
      });
    }
    if (buttons.length === 0) return null;
    return (
      <ButtonsGroup
        key={d.id}
        buttons={buttons as [ButtonProps, ...ButtonProps[]]}
        buttonsSize="small"
        inlineLayoutWhen="always"
      />
    );
  };

  // Une ligne par compte : le défaut, « (+N) » pour les autres, tous listés dans l'infobulle.
  // Hors registry.admin l'API masque scopes : le nom du territory_id legacy vient de la liste des territoires.
  const territoriesColumn = (d: UsersInterface["data"][0]) => {
    const nameOf = (id: number) => territoriesList().find((t) => t?._id === id)?.name;
    const { label, extra, all } = territoriesCell(d.scopes, d.territory_id, nameOf);
    if (extra === 0) return ellipsis(label);
    const cell = (
      <span>
        <span className="cell-ellipsis">{label}</span> (+{extra})
      </span>
    );
    return (
      <Tooltip
        kind="hover"
        title={all.map((name, i) => (
          <span key={i}>
            {name}
            <br />
          </span>
        ))}
      >
        {cell}
      </Tooltip>
    );
  };

  // Opérateur ou territoires selon la famille du rôle ; rien pour le registre.
  const entitiesColumn = (d: UsersInterface["data"][0]) => {
    if (isOperator(d.role)) return ellipsis(operatorsList().find((o) => o?.id === d.operator_id)?.name);
    if (isTerritory(d.role)) return territoriesColumn(d);
    return null;
  };

  const roleColumn = (role: string) => {
    const color = roleColor(role);
    return (
      <span className="role-pill">
        {color && <span className={`role-pill__dot role-pill__dot--${color}`} aria-hidden="true" />}
        {labelRole(role)}
      </span>
    );
  };

  const dataTable =
    data?.data?.map((d) => [
      ellipsis(`${d.firstname} ${d.lastname}`),
      ellipsis(d.email),
      roleColumn(d.role),
      entitiesColumn(d),
      rowActions(d),
    ]) ?? [];

  // À la bascule de rôle, les champs de l'autre famille sont purgés : sinon le corps envoyé porte
  // à la fois un opérateur et des périmètres.
  const onChangeRole = (role: string) => {
    modal.setCurrentRow((prev) => ({
      ...prev,
      role,
      ...(role.startsWith("operator.") ? {} : { operator_id: null }),
      ...(role.startsWith("territory.") || !canManageScopes ? {} : { scopes: [], territory_id: null }),
      ...(canManageScopes
        ? {
            login_siret: siretAfterRoleChange(
              prev.role as string,
              role,
              prev.login_siret as string | null | undefined,
              (prev.scopes as UserScopeInput[] | undefined) ?? [],
            ),
          }
        : {}),
    }));
    modal.setErrors({});
  };
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

  // Met à jour les périmètres, resynchronise territory_id (dual-write legacy) et suggère le SIRET si vide.
  // Un SIRET déjà rempli n'est jamais écrasé.
  const onChangeScopes = (scopes: UserScopeInput[]) => {
    const def = scopes.find((s) => s.is_default) ?? scopes[0];
    modal.setCurrentRow((prev) => ({
      ...prev,
      scopes,
      territory_id: def?.territory_id ?? null,
      login_siret: siretAfterScopesChange(prev.login_siret as string | null | undefined, scopes),
    }));
  };

  // Le select natif ne connaît que des chaînes : "" = aucun opérateur.
  // Choisir un opérateur est un geste explicite : son SIRET remplace celui déjà saisi.
  const onChangeOperator = (value: string) => {
    const operatorId = value === "" ? null : Number(value);
    const siret = operatorSiret(operatorsList(), operatorId);
    modal.validateInputsChange(userFormSchema, { operator_id: operatorId, ...(siret ? { login_siret: siret } : {}) });
  };

  const errorAlertMessage = () => {
    const fields = Object.values(modal.errors ?? {}).filter(Boolean);
    return fields.length > 0 ? fields.join(" | ") : (modal.submitError?.message ?? "");
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
          message={errorAlertMessage()}
          typeAlert={alert}
          onClose={() => setAlert(undefined)}
        />
      )}

      <h3 className={fr.cx("fr-callout__title")}>{props.title}</h3>
      {usersError && (
        <Alert
          severity="error"
          title="Une erreur s'est produite"
          description={usersError.message}
          className={fr.cx("fr-mb-2w")}
        />
      )}
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
                ...(canManageScopes ? { scopes, login_siret: suggestSiret(scopes) } : {}),
              });
              modal.setOpenModal(true);
              modal.setErrors({});
              modal.setTypeModal("create");
            }}
            title="Inviter un utilisateur"
            size="small"
          >
            Inviter
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
      <AdminTable data={dataTable} headers={headers} />
      <Pagination count={totalPages} defaultPage={currentPage} onChange={onChangePage} />
      <Modal
        open={modal.openModal}
        title={modal.typeModal === "create" ? "Inviter un utilisateur" : modal.modalTitle(modal.typeModal)}
        onOpen={async () => {
          if (modal.typeModal === "update" || modal.typeModal === "create") {
            await refetchOperators();
          }
        }}
        onClose={() => modal.setOpenModal(false)}
        onSubmit={async () => {
          try {
            const result = await modal.submitModal("dashboard/user", userFormSchema);
            setDeleteOutcome(result?.outcome === "scope_released" ? "scope_released" : "user_deleted");
            setAlert(modal.typeModal);
          } catch (e) {
            if (e instanceof FormValidationError) return false;
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
                      "aria-invalid": !!modal.errors?.firstname,
                      type: "text",
                      value: (modal.currentRow.firstname as string) ?? "",
                      onChange: (e) => modal.validateInputChange(userFormSchema, "firstname", e.target.value),
                    }}
                  />
                </div>
                <div className={fr.cx("fr-fieldset__element")}>
                  <Input
                    label="Nom"
                    state={modal.errors?.lastname ? "error" : "default"}
                    stateRelatedMessage={modal.errors?.lastname ?? ""}
                    nativeInputProps={{
                      "aria-invalid": !!modal.errors?.lastname,
                      type: "text",
                      value: (modal.currentRow.lastname as string) ?? "",
                      onChange: (e) => modal.validateInputChange(userFormSchema, "lastname", e.target.value),
                    }}
                  />
                </div>
                <div className={fr.cx("fr-fieldset__element")}>
                  <Input
                    label="Adresse mail"
                    state={modal.errors?.email ? "error" : "default"}
                    stateRelatedMessage={modal.errors?.email ?? ""}
                    nativeInputProps={{
                      "aria-invalid": !!modal.errors?.email,
                      type: "text",
                      value: (modal.currentRow.email as string) ?? "",
                      onChange: (e) => modal.validateInputChange(userFormSchema, "email", e.target.value),
                    }}
                  />
                </div>
                <div className={fr.cx("fr-fieldset__element")}>
                  <Select
                    label="Rôle"
                    state={modal.errors?.role ? "error" : "default"}
                    stateRelatedMessage={modal.errors?.role ?? ""}
                    nativeSelectProps={{
                      "aria-invalid": !!modal.errors?.role,
                      value: (modal.currentRow.role ?? "") as string,
                      onChange: (e) => onChangeRole(e.target.value),
                    }}
                  >
                    {roleList().map((r: string, i: number) => (
                      <option key={i} value={r}>
                        {labelRole(r)}
                      </option>
                    ))}
                  </Select>
                </div>
                {canManageScopes && isOperatorTarget && (
                  <div className={fr.cx("fr-fieldset__element")}>
                    <Select
                      label="Opérateur"
                      state={modal.errors?.operator_id ? "error" : "default"}
                      stateRelatedMessage={modal.errors?.operator_id ?? ""}
                      nativeSelectProps={{
                        "aria-invalid": !!modal.errors?.operator_id,
                        value: (modal.currentRow.operator_id as number | null) ?? "",
                        onChange: (e) => onChangeOperator(e.target.value),
                      }}
                    >
                      <option value="">aucun</option>
                      {operatorsList().map((o, i) => (
                        <option key={i} value={o?.id}>
                          {o?.name}
                        </option>
                      ))}
                    </Select>
                  </div>
                )}
              </fieldset>

              {/* Périmètres : territoires seulement, pour registry.admin. */}
              {canManageScopes && isTerritoryTarget && (
                <fieldset className={fr.cx("fr-fieldset")}>
                  <legend className={fr.cx("fr-fieldset__legend")}>Géographies</legend>
                  <div className={fr.cx("fr-fieldset__element")}>
                    {features.multiSiret ? (
                      <UserScopesEditor
                        scopes={(modal.currentRow.scopes as UserScopeInput[]) ?? []}
                        onChange={onChangeScopes}
                      />
                    ) : (
                      <TerritoryAutocomplete
                        label="Territoire"
                        value={defaultTerritory()}
                        onChange={(t) =>
                          onChangeScopes(
                            t
                              ? [{ territory_id: t._id, is_default: true, territory_name: t.name, siret: t.siret }]
                              : [],
                          )
                        }
                      />
                    )}
                    {modal.errors?.scopes && <p className={fr.cx("fr-error-text")}>{modal.errors.scopes}</p>}
                  </div>
                </fieldset>
              )}

              {/* Connexion : login_siret réservé registry.admin, masqué (pas grisé) sinon. */}
              {canManageScopes && (
                <fieldset className={fr.cx("fr-fieldset")}>
                  <legend className={fr.cx("fr-fieldset__legend")}>Connexion</legend>
                  <div className={fr.cx("fr-fieldset__element")}>
                    <Input
                      label="SIRET de connexion ProConnect"
                      hintText="14 chiffres, SIRET de la structure rattachée au compte"
                      state={modal.errors?.login_siret ? "error" : "default"}
                      stateRelatedMessage={modal.errors?.login_siret ?? ""}
                      nativeInputProps={{
                        "aria-invalid": !!modal.errors?.login_siret,
                        inputMode: "numeric",
                        maxLength: 14,
                        value: (modal.currentRow.login_siret as string | null) ?? "",
                        onChange: (e) =>
                          modal.validateInputChange(userFormSchema, "login_siret", e.target.value || null),
                      }}
                    />
                  </div>
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
