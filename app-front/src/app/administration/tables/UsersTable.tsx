import AlertMessage from "@/components/common/AlertMessage";
import { Modal } from "@/components/common/Modal";
import Pagination from "@/components/common/Pagination";
import { getApiUrl } from "@/helpers/api";
import { enumRoles, getRolesList, labelRole } from "@/helpers/auth";
import { useActionsModal } from "@/hooks/useActionsModal";
import { useApi } from "@/hooks/useApi";
import { type OperatorsInterface, type TerritoriesInterface, type UsersInterface } from "@/interfaces/dataInterface";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import Button from "@codegouvfr/react-dsfr/Button";
import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";
import Input from "@codegouvfr/react-dsfr/Input";
import Select from "@codegouvfr/react-dsfr/Select";
import Table from "@codegouvfr/react-dsfr/Table";
import { useMemo, useState } from "react";
import { z } from "zod";

export default function UsersTable(props: { title: string; territoryId: number | null; operatorId: number | null }) {
  const { user, simulatedRole } = useAuth();
  const [currentPage, setCurrentPage] = useState(1);
  const modal = useActionsModal<UsersInterface["data"][0]>();
  const [alert, setAlert] = useState<"create" | "update" | "delete" | "error">();
  const onChangePage = (id: number) => {
    setCurrentPage(id);
  };
  const url = useMemo(() => {
    const urlObj = new URL(getApiUrl("v3", "dashboard/users"));
    if (props.territoryId) {
      urlObj.searchParams.set("territory_id", props.territoryId.toString());
    } else if (props.operatorId) {
      urlObj.searchParams.set("operator_id", props.operatorId.toString());
    }
    if (currentPage !== 1) {
      urlObj.searchParams.set("page", currentPage.toString());
    }
    return urlObj.toString();
  }, [props.territoryId, props.operatorId, currentPage]);

  const { data, refetch: refetchUsers } = useApi<UsersInterface>(url);
  const totalPages = data?.meta.totalPages ?? 1;

  const headers = ["Prénom", "Nom", "Adresse mail", "Rôle", "Opérateur", "Territoire", "Actions"];
  const operatorsApiUrl = getApiUrl("v3", `dashboard/operators?limit=100`);
  const { data: operatorsData, refetch: refetchOperators } = useApi<OperatorsInterface>(operatorsApiUrl);
  const operatorsList = () => {
    if (user?.operator_id) {
      return [operatorsData?.data.find((t) => t.id === user?.operator_id)] as OperatorsInterface["data"];
    }
    return operatorsData?.data ?? [];
  };
  const territoriesApiUrl = getApiUrl("v3", `dashboard/territories?limit=200`);
  const territoriesWithPolicyApiUrl = getApiUrl("v3", `dashboard/territories?policy=true&limit=200`);
  const { data: territoriesData, refetch: refetchTerritories } = useApi<TerritoriesInterface>(territoriesApiUrl);
  const { data: territoriesWithPolicyData } = useApi<TerritoriesInterface>(territoriesWithPolicyApiUrl);
  const territoriesList = () => {
    if (user?.territory_id) {
      return [territoriesData?.data.find((t) => t._id === user?.territory_id)] as TerritoriesInterface["data"];
    }
    return territoriesData?.data ?? [];
  };
  const territoriesWithPolicyList = () => {
    if (user?.territory_id) {
      return [
        territoriesWithPolicyData?.data.find((t) => t._id === user?.territory_id),
      ] as TerritoriesInterface["data"];
    }
    return territoriesWithPolicyData?.data ?? [];
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
                    modal.setCurrentRow(d);
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
                    modal.setCurrentRow(d);
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
    operator_id: z.number({ message: "L'identifiant n'est pas un nombre" }).nullable(),
    territory_id: z.number({ message: "L'identifiant n'est pas un nombre" }).nullable(),
    role: z.enum(enumRoles, { message: "Le rôle n'est pas valide" }),
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
        <>
          <Button
            iconId="fr-icon-add-circle-line"
            onClick={() => {
              modal.setCurrentRow({
                firstname: "",
                lastname: "",
                email: "",
                operator_id: user?.operator_id ?? undefined,
                territory_id: user?.territory_id ?? undefined,
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
        </>
      )}
      <Table data={dataTable} headers={headers} colorVariant="blue-ecume" />
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
          await modal.submitModal("dashboard/user", formSchema);
          setAlert(Object.keys(modal.errors ?? {}).length > 0 ? "error" : modal.typeModal);
          await refetchUsers();
        }}
      >
        <>
          {(modal.typeModal === "update" || modal.typeModal === "create") && (
            <>
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
              {((modal.currentRow.role && (modal.currentRow.role as string).split(".")[0] === "operator") ??
                user?.operator_id) && (
                <Select
                  label="Opérateur"
                  nativeSelectProps={{
                    value: (modal.currentRow.operator_id as number) ?? undefined,
                    onChange: (e) => modal.validateInputChange(formSchema, "operator_id", e.target.value),
                  }}
                >
                  {user?.role === "registry.admin" && <option value={undefined}>aucun</option>}
                  {operatorsList().map((o, i) => (
                    <option key={i} value={o?.id}>
                      {o?.name}
                    </option>
                  ))}
                </Select>
              )}
              {((modal.currentRow.role && (modal.currentRow.role as string).split(".")[0] === "territory") ??
                user?.territory_id) && (
                <Select
                  label="Territoire"
                  nativeSelectProps={{
                    value: (modal.currentRow.territory_id as number) ?? undefined,
                    onChange: (e) => modal.validateInputChange(formSchema, "territory_id", e.target.value),
                  }}
                >
                  {user?.role === "registry.admin" && <option value={undefined}>aucun</option>}
                  {territoriesWithPolicyList().map((t, i) => (
                    <option key={i} value={t?._id}>
                      {t?.name}
                    </option>
                  ))}
                </Select>
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
