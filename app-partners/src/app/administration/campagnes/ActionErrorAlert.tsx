import { fr } from "@codegouvfr/react-dsfr";
import Alert from "@codegouvfr/react-dsfr/Alert";

// Un message multi-lignes (campaignErrorMessage) s'affiche en liste, une ligne reste en texte.
export default function ActionErrorAlert({ message, onClose }: { message: string; onClose: () => void }) {
  const lines = message.split("\n");
  return (
    <Alert
      className={fr.cx("fr-mb-3w")}
      severity="error"
      small
      description={
        lines.length > 1 ? (
          <ul className={fr.cx("fr-mb-0")}>
            {lines.map((line) => (
              <li key={line}>{line}</li>
            ))}
          </ul>
        ) : (
          message
        )
      }
      closable
      onClose={onClose}
    />
  );
}
