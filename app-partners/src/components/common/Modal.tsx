import { createModal } from "@codegouvfr/react-dsfr/Modal";
import { useIsModalOpen } from "@codegouvfr/react-dsfr/Modal/useIsModalOpen";
import { type ReactNode, useEffect, useId, useState } from "react";

export interface ModalProps {
  open: boolean;
  title: string;
  children?: ReactNode;
  cancelButton?: boolean;
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
  }, [props.open]);

  useIsModalOpen(modal, {
    onConceal: () => {
      props.onClose();
    },
  });

  // Verrou : un double clic sur OK envoyait deux requêtes.
  const [submitting, setSubmitting] = useState(false);
  const okButton = {
    children: "OK",
    doClosesModal: false,
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

  return (
    <modal.Component
      title={props.title}
      buttons={
        props.cancelButton !== false
          ? [
              {
                children: "Annuler",
                onClick: () => {
                  props.onClose();
                },
              },
              okButton,
            ]
          : [okButton]
      }
      concealingBackdrop={false}
    >
      {props.children}
    </modal.Component>
  );
}
