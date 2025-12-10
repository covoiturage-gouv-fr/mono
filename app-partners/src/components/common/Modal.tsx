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
  onSubmit: () => Promise<void>;
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

  const okButton = {
    children: "OK",
    onClick: () => {
      props.onClose();
      void props.onSubmit();
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
      concealingBackdrop={true}
    >
      {props.children}
    </modal.Component>
  );
}
