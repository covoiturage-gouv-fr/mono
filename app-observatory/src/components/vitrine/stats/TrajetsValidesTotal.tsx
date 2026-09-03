"use client";

import { TRAJETS_VALIDES_INDICATEUR } from "@/app/startup-etat/stats/data";
import Rows from "@/components/observatoire/indicators/Rows";
import { OBSERVATORY_API_URL } from "@/helpers/api";
import { useApi } from "@/hooks/useApi";

type EvolFluxRow = { year: number; journeys: number | string };

// Total des trajets « passager » validés = somme de la série annuelle nationale
// d'evol-flux (2019 → année en cours). Rien n'est rendu tant que l'API n'a pas
// répondu : pas de valeur de repli figée.
export default function TrajetsValidesTotal() {
  const url = `${OBSERVATORY_API_URL}/evol-flux?code=XXXXX&type=country&indic=journeys`;
  const { data } = useApi<EvolFluxRow[]>(url);
  if (!data || data.length === 0) return null;

  const total = data.reduce((sum, r) => sum + Number(r.journeys), 0);

  return (
    <Rows
      data={[
        { ...TRAJETS_VALIDES_INDICATEUR, value: total.toLocaleString("fr-FR") },
      ]}
    />
  );
}
