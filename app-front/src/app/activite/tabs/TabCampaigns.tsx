"use client";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import Alert from "@codegouvfr/react-dsfr/Alert";
import CampaignsTable from "../tables/CampaignsTable";

export default function TabCampaigns() {
  const { user } = useAuth();
  return (
    <>
      <Alert
        title={"Important"}
        severity="info"
        description={
          <>
            <p>
              Les graphiques présentés regroupent les trajets conformes aux conditions générales d’utilisation de
              covoiturage.beta.gouv, dont l’origine <strong>ou</strong> la destination se situe sur le territoire
              sélectionné. Ces données peuvent être soumises à un léger redressement statistique à posteriori
              (annulation /modification de trajets) qui peut engendrer un léger différentiel de données avec les appels
              de fonds.
            </p>
            <p>
              Les appels de fonds correspondent à une extraction effectuée chaque mois à une date précise (généralement
              le 6 du mois suivant), portant sur:
            </p>
            <ul>
              <li>
                le nombre de trajets mensuels respectant les conditions générales d’utilisation de
                covoiturage.beta.gouv, avec une origine <strong>ou</strong> une destination sur le territoire
                sélectionné
              </li>
              <li>
                le nombre de trajets mensuels incités, c’est-à-dire les trajets identifiés comme éligibles aux critères
                de la campagne d’incitation, pour lesquels covoiturage.beta.gouv a calculé le montant d’incitation à
                verser.
              </li>
            </ul>
          </>
        }
      />
      {user && (
        <div className={fr.cx("fr-mt-4w")}>
          <CampaignsTable
            title={`Campagnes d'incitation`}
            territoryId={user?.territory_id ?? null}
            operatorId={user?.operator_id ?? null}
          />
        </div>
      )}
    </>
  );
}
