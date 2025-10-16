// components/common/AlertMessage.tsx
import Alert from "@codegouvfr/react-dsfr/Alert";
import { useEffect, useState } from "react";

export interface AlertProps {
  message?: string;
  title?: string;
  typeAlert: "create" | "update" | "delete" | "error";
  duration?: number;
  onClose?: () => void;
}

export default function AlertMessage(props: AlertProps) {
  const [visible, setVisible] = useState(true);
  const { message = "", title, typeAlert, duration = 5000, onClose } = props;

  const severityMap: Record<AlertProps["typeAlert"], "success" | "warning" | "error" | "info"> = {
    create: "success",
    update: "success",
    delete: "warning",
    error: "error",
  };

  useEffect(() => {
    const timer = setTimeout(() => {
      setVisible(false);
      onClose?.();
    }, duration);
    return () => clearTimeout(timer);
  }, [duration, onClose]);

  if (!visible) return null;

  return (
    <div
      className="fr-container"
      style={{
        position: "fixed",
        backgroundColor: "#fff",
        top: "1rem",
        left: 0,
        right: 0,
        zIndex: 1000,
      }}
    >
      <Alert
        severity={severityMap[typeAlert]}
        title={title ?? defaultTitle(typeAlert)}
        description={message}
        closable
        onClose={() => {
          setVisible(false);
          onClose?.();
        }}
      />
    </div>
  );
}

function defaultTitle(type: AlertProps["typeAlert"]): string {
  switch (type) {
    case "create":
      return "Création réussie";
    case "update":
      return "Mise à jour réussie";
    case "delete":
      return "Suppression réussie";
    case "error":
      return "Erreur";
    default:
      return "Notification";
  }
}
