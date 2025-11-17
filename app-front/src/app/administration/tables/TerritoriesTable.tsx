import AlertMessage from "@/components/common/AlertMessage";
import { Modal } from "@/components/common/Modal";
import Pagination from "@/components/common/Pagination";
import { Config } from "@/config";
import { getApiUrl } from "@/helpers/api";
import { formatErrors, useActionsModal } from "@/hooks/useActionsModal";
import { useApi } from "@/hooks/useApi";
import { useDebounce } from "@/hooks/useDebounce";
import type { Company, TerritoriesInterface, TerritorySelectorsInterface } from "@/interfaces/dataInterface";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import Button from "@codegouvfr/react-dsfr/Button";
import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";
import Input from "@codegouvfr/react-dsfr/Input";
import Table from "@codegouvfr/react-dsfr/Table";
import { useEffect, useMemo, useState } from "react";
import { z } from "zod";

export default function TerritoriesTable(props: { title: string; id: number | null }) {
  const { user } = useAuth();
  const [currentPage, setCurrentPage] = useState(1);
  const [search, setSearch] = useState("");
  const debouncedSearch = useDebounce(search, 500);
  const [selector, setSelector] = useState<TerritorySelectorsInterface>();
  const modal = useActionsModal<TerritoriesInterface["data"][0]>();
  const [alert, setAlert] = useState<"create" | "update" | "delete" | "error">();
  const onChangePage = (page: number) => {
    setCurrentPage(page);
  };
  const onChangeSearch = (search: string) => {
    setSearch(search);
    setCurrentPage(1);
  };
  const url = useMemo(() => {
    const urlObj = new URL(getApiUrl("v3", "dashboard/territories?limit=25"));
    if (props.id) {
      urlObj.searchParams.set("id", props.id.toString());
    }
    if (currentPage !== 1) {
      urlObj.searchParams.set("offset", ((currentPage - 1) * 25).toString());
    }
    if (debouncedSearch !== "") {
      urlObj.searchParams.set("search", debouncedSearch);
    }
    return urlObj.toString();
  }, [props.id, currentPage, debouncedSearch]);
  const { data, refetch: refetchTerritories } = useApi<TerritoriesInterface>(url);
  const totalPages = () => {
    if (data) {
      return Math.ceil(data.meta.pagination.total / data.meta.pagination.limit);
    }
    return 1;
  };
  const totalRecords = data?.meta.pagination.total ?? 0;
  const headers = ["Identifiant", "Nom", "Siret", "Actions"];
  const dataTable =
    data?.data.map((d) => [
      d._id,
      d.name,
      d.siret,
      <ButtonsGroup
        key={d._id}
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
  const fetchCompany = async (siret: string): Promise<Response> => {
    return fetch(`${Config.get<string>("auth.domain")}/rpc?methods=company:fetch`, {
      credentials: "include",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        method: "company:fetch",
        params: siret,
        id: 1,
      }),
    });
  };

  const findGeoBySiren = async (siret: string): Promise<Response> => {
    return await fetch(`${Config.get<string>("auth.domain")}/rpc?methods=territory:findGeoBySiren`, {
      credentials: "include",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        method: "territory:findGeoBySiren",
        params: { siren: siret.substring(0, 9) },
        id: 1,
      }),
    });
  };

  const submitModal = async (url: string) => {
    if (modal.typeModal === "create") {
      const result = formSchema.safeParse(modal.currentRow);
      if (!result.success) {
        const errors = result.error.flatten().fieldErrors;
        modal.setErrors(formatErrors(errors));
      }
    }
    const request = {
      url: "",
      params: {
        method: "",
        credentials: "include",
        headers: {
          "Content-Type": "application/json",
        },
      } as RequestInit,
    };
    switch (modal.typeModal) {
      case "delete":
        request.url = getApiUrl("v3", `${url}/${modal.currentRow?._id as string}`);
        request.params.method = "DELETE";
        break;
      case "create": {
        const companyResponse: Response = await fetchCompany(modal.currentRow.siret as string);
        if (companyResponse.ok) {
          const companyBody = (await companyResponse.json()) as Company;
          request.url = getApiUrl("v3", url);
          request.params.method = "POST";
          request.params.body = JSON.stringify({
            ...modal.currentRow,
            company_id: companyBody.result.data._id,
            selector: selector,
          });
        } else {
          throw new Error("Aucune entreprise trouvée pour ce siret");
        }
        break;
      }
    }
    const response = await fetch(request.url, request.params);
    if (!response.ok) {
      const res = await response.json();
      throw new Error(res?.message ?? "Une erreur est survenue");
    }
    return;
  };

  useEffect(() => {
    const siretValidated = async () => {
      if (!modal.errors?.siret && modal.currentRow.siret) {
        const geoResponse = await findGeoBySiren(modal.currentRow.siret as string);
        if (geoResponse.ok) {
          const body = await geoResponse.json();
          if (body?.result?.data?.aom_siren) {
            modal.setCurrentRow({
              ...modal.currentRow,
              name: body.result.data.aom_name,
            });
            modal.validateInputChange(formSchema, "name", body.result.data.aom_name as string);
            setSelector({
              aom: [body.result.data.aom_siren],
            });
          }
        }
      }
    };
    void siretValidated();
  }, [modal.errors?.siret, modal.currentRow.siret]);

  return (
    <>
      {alert === "delete" && (
        <AlertMessage
          title="Suppression réussie"
          message="Le territoire a été supprimé."
          typeAlert={alert}
          onClose={() => setAlert(undefined)}
        />
      )}
      {alert === "create" && (
        <AlertMessage
          title="Territoire ajouté avec succès"
          message="Le territoire a été enregistré dans la base de données."
          typeAlert={alert}
          onClose={() => setAlert(undefined)}
        />
      )}
      {alert === "update" && (
        <AlertMessage
          title="Territoire modifié avec succès"
          message="Le territoire a été enregistré dans la base de données."
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
            title="Ajouter un territoire"
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
      <Pagination count={totalPages()} defaultPage={currentPage} onChange={onChangePage} />
      <Modal
        open={modal.openModal}
        title={modal.modalTitle(modal.typeModal)}
        onClose={() => modal.setOpenModal(false)}
        onSubmit={async () => {
          await submitModal("dashboard/territory");
          setAlert(Object.keys(modal.errors ?? {}).length > 0 ? "error" : modal.typeModal);
          await refetchTerritories();
        }}
      >
        <>
          {(modal.typeModal === "update" || modal.typeModal === "create") && (
            <>
              <Input
                label="Siret"
                state={modal.errors?.siret ? "error" : "default"}
                stateRelatedMessage={modal.errors?.siret ?? ""}
                nativeInputProps={{
                  type: "text",
                  value: (modal.currentRow.siret as string) ?? "",
                  onChange: (e) => {
                    modal.validateInputChange(formSchema, "siret", e.target.value);
                  },
                }}
              />
              <Input
                label="Nom du territoire"
                state={modal.errors?.name ? "error" : "default"}
                stateRelatedMessage={modal.errors?.name ?? ""}
                nativeInputProps={{
                  type: "text",
                  value: (modal.currentRow.name as string) ?? "",
                  onChange: (e) => {
                    modal.validateInputChange(formSchema, "name", e.target.value);
                  },
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
