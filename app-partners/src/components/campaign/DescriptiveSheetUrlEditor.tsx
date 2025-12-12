import AlertMessage from "@/components/common/AlertMessage";
import { getApiUrl } from "@/helpers/api";
import { isRegistry, isTerritory } from "@/helpers/auth";
import { AuthContextProps } from "@/interfaces/auth";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import Button from "@codegouvfr/react-dsfr/Button";
import Input from "@codegouvfr/react-dsfr/Input";
import { useState } from "react";
import isURL from "validator/lib/isURL";

interface DescriptiveSheetUrlEditorProps {
  campaignId: number;
  initialValue?: string;
  onSuccess?: () => void;
}

export default function DescriptiveSheetUrlEditor({ campaignId, initialValue }: DescriptiveSheetUrlEditorProps) {
  const [descriptiveSheetUrl, setDescriptiveSheetUrl] = useState<string>(initialValue ?? "");
  const [error, setError] = useState<string | null>(null);
  const [alert, setAlert] = useState<"success" | "error">();
  const { user, simulatedRole }: AuthContextProps = useAuth();

  const handleUpdateDescriptiveSheetUrl = async () => {
    if (!campaignId) return;
    if (descriptiveSheetUrl != "" && !isURL(descriptiveSheetUrl)) {
      setError("Merci de renseigner une URL valide.");
      return;
    }
    try {
      const response = await fetch(getApiUrl("v3", "policies/update/descriptive-sheet-url"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        credentials: "include",
        body: JSON.stringify({
          _id: campaignId,
          descriptive_sheet_url: descriptiveSheetUrl,
        }),
      });
      if (!response.ok) {
        setAlert("error");
        return;
      }

      setError(null);
      setAlert("success");
    } catch (e) {
      console.error(e);
      setAlert("error");
    }
  };

  return (
    <div className={fr.cx("fr-mt-4w")}>
      {alert === "success" && (
        <AlertMessage
          title="URL enregistrée avec succès"
          message="L'URL de la fiche descriptive a été mise à jour."
          typeAlert="update"
          onClose={() => setAlert(undefined)}
        />
      )}
      {alert === "error" && (
        <AlertMessage
          title="Une erreur s'est produite"
          message="Impossible d'enregistrer l'URL. Veuillez réessayer."
          typeAlert="error"
          onClose={() => setAlert(undefined)}
        />
      )}
      {user?.role && (isRegistry(user.role) || isTerritory(user.role)) && !simulatedRole ? (
        <>
          <div className={fr.cx("fr-grid-row", "fr-grid-row--gutters")}>
            <div className={fr.cx("fr-col", "fr-col-md", "fr-text--bold")}>
              <Input
                label="Description complète du paramètrage de la campagne :"
                state={error ? "error" : "default"}
                stateRelatedMessage={error ?? ""}
                hintText="URL de la fiche descriptive"
                nativeInputProps={{
                  value: descriptiveSheetUrl,
                  onChange: (e) => setDescriptiveSheetUrl(e.target.value),
                  placeholder: "https://...",
                  type: "url",
                }}
              />
            </div>
            <div className={fr.cx("fr-col", "fr-col-md")} style={{ display: "flex", alignItems: "flex-end" }}>
              <Button onClick={() => void handleUpdateDescriptiveSheetUrl()}>Enregistrer ou modifier l'URL</Button>
            </div>
          </div>
        </>
      ) : (
        <div className={fr.cx("fr-col", "fr-col-md")}>
          <p className={"fr-text--bold"}>Description complète du paramètrage de la campagne :</p>
          <Button
            onClick={() => {
              if (initialValue && isURL(initialValue)) {
                window.open(initialValue, "_blank", "noopener,noreferrer");
              }
            }}
            disabled={!initialValue || !isURL(initialValue)}
          >
            Voir la fiche descriptive
          </Button>
        </div>
      )}
    </div>
  );
}
