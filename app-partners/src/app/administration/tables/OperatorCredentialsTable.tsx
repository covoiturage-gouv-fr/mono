import { Modal } from "@/components/common/Modal";
import { getApiUrl } from "@/helpers/api";
import { useActionsModal } from "@/hooks/useActionsModal";
import { useApi } from "@/hooks/useApi";
import {
  type CreateTokenResponseInterface as Credentials,
  type OperatorTokenInterface,
} from "@/interfaces/dataInterface";
import { fr } from "@codegouvfr/react-dsfr";
import Button from "@codegouvfr/react-dsfr/Button";
import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";
import Table from "@codegouvfr/react-dsfr/Table";
import Link from "next/link";
import { useMemo, useState } from "react";

export default function OperatorCredentialsTable(props: { title: string; operatorId: number; refresh: () => void }) {
  const [credentials, setCredentials] = useState<Credentials>();

  const url = useMemo(() => {
    const urlObj = new URL(getApiUrl("v3", "auth/credentials"));
    urlObj.searchParams.set("operator_id", props.operatorId.toString());
    return urlObj.toString();
  }, [props.operatorId]);

  const { data } = useApi<OperatorTokenInterface[]>(url);
  const modal = useActionsModal<OperatorTokenInterface>();
  const headers = ["Identifiant clé", "Actions"];
  const dataTable =
    data?.map((d) => [
      d.token_id,
      <ButtonsGroup
        key={d.token_id}
        buttons={[
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

  const handleCreateCredentials = async () => {
    const url = new URL(getApiUrl("v3", "auth/credentials"));
    const init: RequestInit = {
      method: "POST",
      credentials: "include",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
    };

    if (props.operatorId) {
      init.body = JSON.stringify({ operator_id: props.operatorId });
    }

    const response = await fetch(url, init);

    if (response.status == 201) {
      setCredentials((await response.json()) as Credentials);
    } else {
      console.error("Failed to generate credentials:", response.status);
    }
  };

  const handleDeleteCredentials = async (tokenId: string) => {
    const url = new URL(getApiUrl("v3", "auth/credentials"));
    url.searchParams.set("token_id", tokenId);
    if (props.operatorId) url.searchParams.set("operator_id", props.operatorId?.toString());

    const response = await fetch(url, {
      credentials: "include",
      method: "DELETE",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
    });

    if (response.status == 204) {
      setCredentials(undefined);
    } else {
      console.error("Failed to delete credentials:", response.status);
    }
  };

  return (
    <>
      <h3 className={fr.cx("fr-callout__title")}>{props.title}</h3>
      <div className={fr.cx("fr-text--md")}>
        <Link
          href="https://tech.covoiturage.beta.gouv.fr/#topic-connexion-a-l-api"
          target="_blank"
          aria-label={`Ouvrir une nouvelle fenêtre vers la documentation technique`}
        >
          Consulter la documentation technique
        </Link>
      </div>
      <>
        <Button
          iconId="fr-icon-add-circle-line"
          onClick={() => {
            modal.setOpenModal(true);
            modal.setErrors({});
            modal.setTypeModal("create");
          }}
          title="Générer une nouvelle clé d'API"
          size="small"
        >
          Générer
        </Button>
      </>
      <Table data={dataTable} headers={headers} colorVariant="blue-ecume" fixed />
      <Modal
        open={modal.openModal}
        title={`Clé d'API`}
        cancelButton={false}
        onOpen={async () => {
          if (modal.typeModal === "create") {
            await handleCreateCredentials();
          }
        }}
        onClose={() => {
          modal.setOpenModal(false);
          setCredentials(undefined);
          props.refresh();
        }}
        onSubmit={async () => {
          if (modal.typeModal === "delete") {
            await handleDeleteCredentials(modal.currentRow.token_id as string);
          }
          setCredentials(undefined);
          props.refresh();
        }}
      >
        {modal.typeModal === "create" && credentials && (
          <>
            <div className={fr.cx("fr-text--md")}>
              <p>access_key</p>
              <code className="codeblock">{credentials.access_key}</code>
            </div>
            <div className={fr.cx("fr-text--md")}>
              <p>secret_key</p>
              <code className="codeblock">{credentials.secret_key}</code>
            </div>
            <blockquote className={fr.cx("fr-callout", "fr-callout--green-tilleul-verveine")}>
              Attention, la clé n&apos;est affichée qu&apos;une seule fois.
            </blockquote>
          </>
        )}
        {modal.typeModal === "delete" && (
          <>
            <p>Êtes-vous sûr de vouloir supprimer la clé&nbsp;?</p>
            <code className="codeblock">{modal.currentRow.token_id as string}</code>
          </>
        )}
      </Modal>
    </>
  );
}
