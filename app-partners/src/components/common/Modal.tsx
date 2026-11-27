import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";
import { createModal } from "@codegouvfr/react-dsfr/Modal";
import { useIsModalOpen } from "@codegouvfr/react-dsfr/Modal/useIsModalOpen";
import { type ReactNode, useEffect, useId, useState } from "react";

export interface ModalProps {
  open: boolean;
  title: string;
  children?: ReactNode;
  cancelButton?: boolean;
  submitLabel?: string;
  // Boutons en fin de contenu plutôt qu'en pied fixe : il faut défiler tout le formulaire pour valider.
  inlineButtons?: boolean;
  onClose: () => void;
  onOpen?: () => Promise<void>;
  // Renvoyer `false` garde la modale ouverte (erreurs de champ à corriger).
  onSubmit: () => Promise<void | boolean>;
}

export interface ModalResponse {
  doProceed: boolean;
}

export function Modal(props: ModalProps) {
  const id = useId();
  const [modal] = useState(() =>
    createModal({
      id: `modal-${id}`,
      isOpenedByDefault: false,
    }),
  );

  useEffect(() => {
    if (props.open) {
      modal.open();
      if (props.onOpen) {
        void props.onOpen();
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- onOpen n'est pas stable : ne déclencher qu'à l'ouverture
  }, [props.open]);

  useIsModalOpen(modal, {
    onConceal: () => {
      props.onClose();
    },
  });

  // Verrou : un double clic sur OK envoyait deux requêtes.
  const [submitting, setSubmitting] = useState(false);
  const okButton = {
    children: props.submitLabel ?? "OK",
    disabled: submitting,
    onClick: async () => {
      if (submitting) return;
      setSubmitting(true);
      let keepOpen = false;
      try {
        keepOpen = (await props.onSubmit()) === false;
      } catch {
        // Un rejet inattendu ne doit pas laisser la modale figée.
      } finally {
        setSubmitting(false);
      }
      if (!keepOpen) modal.close();
    },
  };

  const cancel = { children: "Annuler", priority: "secondary" as const };
  const footerOk = { ...okButton, doClosesModal: false };
  const footerCancel = { ...cancel, onClick: () => props.onClose() };

  return (
    <modal.Component
      title={props.title}
      buttons={props.inlineButtons ? undefined : props.cancelButton !== false ? [footerCancel, footerOk] : [footerOk]}
      concealingBackdrop={false}
    >
      {/* Le DSFR n'écoute Échap que sur ses propres boutons : depuis un champ, la modale restait ouverte. */}
      <div
        onKeyDown={(e) => {
          if (e.key === "Escape") {
            e.stopPropagation();
            modal.close();
          }
        }}
      >
        {props.children}
        {props.inlineButtons && (
          <ButtonsGroup
            inlineLayoutWhen="always"
            alignment="right"
            buttonsEquisized
            buttons={
              props.cancelButton !== false ? [{ ...cancel, onClick: () => modal.close() }, okButton] : [okButton]
            }
          />
        )}
      </div>
    </modal.Component>
  );
}
