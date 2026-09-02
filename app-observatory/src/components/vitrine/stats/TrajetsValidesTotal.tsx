"use client";

import { TRAJETS_VALIDES_TOTAL } from "@/app/startup-etat/stats/data";
import { OBSERVATORY_API_URL } from "@/helpers/api";
import { useApi } from "@/hooks/useApi";
import StatFigure from "./StatFigure";

type EvolFluxRow = { year: number; journeys: number | string };

// Total des trajets « passager » validés = somme de la série annuelle nationale
// (API observatoire), avec repli sur la valeur figée (src/app/stats/data.ts).
export default function TrajetsValidesTotal() {
  const url = `${OBSERVATORY_API_URL}/evol-flux?code=XXXXX&type=country&indic=journeys&past=7`;
  const { data } = useApi<EvolFluxRow[]>(url);
  const sum = (data ?? []).reduce((total, r) => total + Number(r.journeys), 0);
  const total = sum > 0 ? sum : TRAJETS_VALIDES_TOTAL;

  return (
    <StatFigure
      value={total.toLocaleString("fr-FR")}
      label={'trajets « passager » validés depuis 2019'}
      note="Trajets courte distance transmis par les plateformes de covoiturage partenaires et validés par les services de normalisation et de contrôle de covoiturage.beta.gouv.fr."
    />
  );
}
