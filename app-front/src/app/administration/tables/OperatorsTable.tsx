 
import AlertMessage from "@/components/common/AlertMessage";
import { Modal } from "@/components/common/Modal";
import Pagination from "@/components/common/Pagination";
import { getApiUrl } from "@/helpers/api";
import { useActionsModal } from "@/hooks/useActionsModal";
import { useApi } from "@/hooks/useApi";
import { useDebounce } from "@/hooks/useDebounce";
import { type OperatorsInterface } from "@/interfaces/dataInterface";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import Button from "@codegouvfr/react-dsfr/Button";
import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";
import { Input } from "@codegouvfr/react-dsfr/Input";
import Table from "@codegouvfr/react-dsfr/Table";
import { useMemo, useState } from "react";
import { z } from "zod";

export default function OperatorsTable(props: { title: string; id: number | null }) {
  const { user } = useAuth();
  const [currentPage, setCurrentPage] = useState(1);
  const [search, setSearch] = useState("");
  const debouncedSearch = useDebounce(search, 500);
  const modal = useActionsModal<OperatorsInterface["data"][0]>();
  const [alert, setAlert] = useState<"create" | "update" | "delete" | "error">();
  const onChangePage = (id: number) => {
    setCurrentPage(id);
  };
  const onChangeSearch = (search: string) => {
    setSearch(search);
    setCurrentPage(1);
  };
  const url = useMemo(() => {
    const urlObj = new URL(getApiUrl("v3", "dashboard/operators"));
    if (props.id) {
      urlObj.searchParams.set("id", props.id.toString());
    }
    if (currentPage !== 1) {
      urlObj.searchParams.set("page", currentPage.toString());
    }
    if (debouncedSearch !== "") {
      urlObj.searchParams.set("search", debouncedSearch);
    }
    return urlObj.toString();
  }, [props.id, currentPage, debouncedSearch]);
  const { data, refetch: refetchOperators } = useApi<OperatorsInterface>(url);
  const totalPages = data?.meta.totalPages ?? 1;
  const totalRecords = data?.meta.total ?? 0;
  const headers = ["Identifiant", "Nom", "Siret", "Actions"];
  const dataTable =
    data?.data.map((d) => [
      d.id,
      d.name,
      d.siret,
      <ButtonsGroup
        key={d.id}
        buttons={[
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
        ]}
        buttonsSize="small"
        inlineLayoutWhen="lg and up"
      />,
    ]) ?? [];

  const formSchema = z.object({
    name: z.string().min(3, { message: "Le nom doit contenir au moins 3 caractères" }),
    siret: z.string().regex(/^\d{14}$/, { message: "Le SIRET doit contenir 14 chiffres" }),
  });

  return (
    <>
      {alert === "delete" && (
        <AlertMessage
          title="Suppression réussie"
          message="L'opérateur a été supprimé."
          typeAlert={alert}
          onClose={() => setAlert(undefined)}
        />
      )}
      {alert === "create" && (
        <AlertMessage
          title="Opérateur ajouté avec succès"
          message="L'opérateur a été enregistré dans la base de données."
          typeAlert={alert}
          onClose={() => setAlert(undefined)}
        />
      )}
      {alert === "update" && (
        <AlertMessage
          title="Opérateur modifié avec succès"
          message="L'opérateur a été enregistré dans la base de données."
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
        {user?.role === "registry.admin" && (
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", marginBottom: "1rem" }}>
              <Button
                iconId="fr-icon-add-circle-line"
                onClick={() => {
                  modal.setCurrentRow({ name: "", siret: "" });
                  modal.setOpenModal(true);
                  modal.setErrors({});
                  modal.setTypeModal("create");
                }}
                title="Ajouter un opérateur"
                size="small"
              >
                Ajouter
              </Button>
              <Input
                label="Rechercher"
                state={search !== "" ? (totalRecords <= 0 ? "error" : "success") : "default"}
                stateRelatedMessage={totalRecords + " résultats"}
                hintText="Nom / SIRET"
                nativeInputProps={{
                  type: "text",
                  value: search ?? "",
                  onChange: (e) =>
                    onChangeSearch(
                      e.target.value,
                    ),
                }}
              />
          </div>
        )}
      
      <Table data={dataTable} headers={headers} colorVariant="blue-ecume" fixed />
      <Pagination count={totalPages} defaultPage={currentPage} onChange={onChangePage} />
      <Modal
        open={modal.openModal}
        title={modal.modalTitle(modal.typeModal)}
        onClose={() => modal.setOpenModal(false)}
        onSubmit={async () => {
          await modal.submitModal("dashboard/operator", formSchema);
          setAlert(Object.keys(modal.errors ?? {}).length > 0 ? "error" : modal.typeModal);
          await refetchOperators();
        }}
      >
        <>
          {(modal.typeModal === "update" || modal.typeModal === "create") && (
            <>
              <Input
                label="Nom de l'opérateur"
                state={modal.errors?.name ? "error" : "default"}
                stateRelatedMessage={modal.errors?.name ?? ""}
                nativeInputProps={{
                  type: "text",
                  value: (modal.currentRow.name as string) ?? "",
                  onChange: (e) => modal.validateInputChange(formSchema, "name", e.target.value),
                }}
              />
              <Input
                label="Siret"
                state={modal.errors?.siret ? "error" : "default"}
                stateRelatedMessage={modal.errors?.siret ?? ""}
                nativeInputProps={{
                  type: "text",
                  value: (modal.currentRow.siret as string) ?? "",
                  onChange: (e) => modal.validateInputChange(formSchema, "siret", e.target.value),
                }}
              />
            </>
          )}
          {modal.typeModal === "delete" &&
            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
            `Êtes-vous sûr de vouloir supprimer l'opérateur ${modal.currentRow?.name} ?`}
        </>
      </Modal>
    </>
  );
}
