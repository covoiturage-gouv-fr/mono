"use client";
import Alert from "@codegouvfr/react-dsfr/Alert";
import { useAuth } from "../../providers/AuthProvider";

export default function Anonymous() {
  const { user } = useAuth();
  if (user?.role === "anonymous") {
    return (
      <div className="fr-mt-4w">
        <Alert
          title={"Important"}
          severity="warning"
          description={
            <p>
              Vous êtes identifé en tant qu&apos;utilisateur anonyme. Veuillez
              contacter votre administrateur afin qu&apos;il associe votre
              compte ProConnect avec votre compte RPC
            </p>
          }
        />
      </div>
    );
  }
  return null;
}
