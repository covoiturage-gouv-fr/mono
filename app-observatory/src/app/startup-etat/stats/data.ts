// Données figées de la page /stats (mesures d'impact de la Startup d'État).
// Relevées à la main depuis le dashboard Metabase 111 — public :
// https://stats.covoiturage.beta.gouv.fr/public/dashboard/2084d346-8e3b-495e-9b10-b4870a35632a
// Voir src/app/stats/README.md pour la procédure de rafraîchissement.
// Dernier relevé : 2026-09-02.

export interface SeriePoint {
  x: string;
  y: number;
}

export interface Indicateur {
  valeur: string;
  note: string;
}

// Objectif national : 3 millions de trajets quotidiens en covoiturage d'ici 2027,
// soit ~3 M de « trajets passager » validés par mois (repère du graphe mensuel).
export const TRAJETS_GOAL_2027 = 3_000_000;

// Total des trajets « passager » validés depuis 2019. Sert de repli quand la somme
// de la série annuelle de l'API n'est pas disponible.
export const TRAJETS_VALIDES_TOTAL = 49_933_442;

export const INDICATEURS = {
  plateformes_actives: { valeur: "10", note: "vs 23 actives fin 2024" },
  collectivites_accompagnees: { valeur: "150+", note: "" },
  pct_collectivites_reduiraient_incitations: {
    valeur: "48 %",
    note: "vs 43 % qui déclaraient réduire ou arrêter à la même question en 2024 (répondants de l'étude d'impact 2025 auprès des collectivités)",
  },
  note_satisfaction_observatoire: {
    valeur: "8,31/10",
    note: "vs 8,49/10 en 2024",
  },
  telechargements_datagouv: {
    valeur: "8 480",
    note: "téléchargements comptabilisés en 2025",
  },
  attestations_honneur_fmd_total: { valeur: "337 014", note: "" },
  attestations_honneur_fmd_2025: {
    valeur: "110 231",
    note: "vs 93 587 en 2024",
  },
  campagnes_incitation_2024: {
    valeur: "137",
    note: "+55 % par rapport à 2023",
  },
  lignes_covoiturage_2024: { valeur: "77", note: "pour les périphéries" },
  aires_covoiturage_2024: { valeur: "123", note: "" },
} satisfies Record<string, Indicateur>;

// Repli du graphe mensuel des trajets « passager » validés (carte Metabase 413).
// L'API observatoire ne remonte que ~7 ans glissants : ce repli garde l'historique
// complet depuis 2019 et prend le relais si l'API est indisponible.
export const TRAJETS_PAR_MOIS: SeriePoint[] = [
  { x: "2019-01", y: 9 },
  { x: "2019-02", y: 51 },
  { x: "2019-03", y: 58 },
  { x: "2019-04", y: 622 },
  { x: "2019-05", y: 9795 },
  { x: "2019-06", y: 43621 },
  { x: "2019-07", y: 70158 },
  { x: "2019-08", y: 57218 },
  { x: "2019-09", y: 99688 },
  { x: "2019-10", y: 123984 },
  { x: "2019-11", y: 171367 },
  { x: "2019-12", y: 444168 },
  { x: "2020-01", y: 569839 },
  { x: "2020-02", y: 408546 },
  { x: "2020-03", y: 246829 },
  { x: "2020-04", y: 34374 },
  { x: "2020-05", y: 71718 },
  { x: "2020-06", y: 135581 },
  { x: "2020-07", y: 105119 },
  { x: "2020-08", y: 32993 },
  { x: "2020-09", y: 65756 },
  { x: "2020-10", y: 66122 },
  { x: "2020-11", y: 35110 },
  { x: "2020-12", y: 32607 },
  { x: "2021-01", y: 41770 },
  { x: "2021-02", y: 59304 },
  { x: "2021-03", y: 110195 },
  { x: "2021-04", y: 93478 },
  { x: "2021-05", y: 103034 },
  { x: "2021-06", y: 135018 },
  { x: "2021-07", y: 115746 },
  { x: "2021-08", y: 95438 },
  { x: "2021-09", y: 166940 },
  { x: "2021-10", y: 219645 },
  { x: "2021-11", y: 246413 },
  { x: "2021-12", y: 225830 },
  { x: "2022-01", y: 232713 },
  { x: "2022-02", y: 259819 },
  { x: "2022-03", y: 399608 },
  { x: "2022-04", y: 382184 },
  { x: "2022-05", y: 429763 },
  { x: "2022-06", y: 437726 },
  { x: "2022-07", y: 381658 },
  { x: "2022-08", y: 340524 },
  { x: "2022-09", y: 542810 },
  { x: "2022-10", y: 626534 },
  { x: "2022-11", y: 667159 },
  { x: "2022-12", y: 569899 },
  { x: "2023-01", y: 808879 },
  { x: "2023-02", y: 847540 },
  { x: "2023-03", y: 1068333 },
  { x: "2023-04", y: 812926 },
  { x: "2023-05", y: 775906 },
  { x: "2023-06", y: 801744 },
  { x: "2023-07", y: 618672 },
  { x: "2023-08", y: 527341 },
  { x: "2023-09", y: 737599 },
  { x: "2023-10", y: 851815 },
  { x: "2023-11", y: 927463 },
  { x: "2023-12", y: 853530 },
  { x: "2024-01", y: 968299 },
  { x: "2024-02", y: 997027 },
  { x: "2024-03", y: 1089858 },
  { x: "2024-04", y: 1028655 },
  { x: "2024-05", y: 949372 },
  { x: "2024-06", y: 1003902 },
  { x: "2024-07", y: 911614 },
  { x: "2024-08", y: 672253 },
  { x: "2024-09", y: 1010550 },
  { x: "2024-10", y: 1225169 },
  { x: "2024-11", y: 1356181 },
  { x: "2024-12", y: 1581109 },
  { x: "2025-01", y: 1271547 },
  { x: "2025-02", y: 1077002 },
  { x: "2025-03", y: 1104820 },
  { x: "2025-04", y: 940997 },
  { x: "2025-05", y: 883773 },
  { x: "2025-06", y: 896873 },
  { x: "2025-07", y: 829504 },
  { x: "2025-08", y: 625566 },
  { x: "2025-09", y: 958938 },
  { x: "2025-10", y: 1035107 },
  { x: "2025-11", y: 1012018 },
  { x: "2025-12", y: 942025 },
  { x: "2026-01", y: 951104 },
  { x: "2026-02", y: 930494 },
  { x: "2026-03", y: 1108479 },
  { x: "2026-04", y: 999025 },
  { x: "2026-05", y: 870706 },
  { x: "2026-06", y: 922617 },
];

// Repli du graphe annuel des trajets « passager » validés (carte Metabase 396).
export const TRAJETS_PAR_AN: SeriePoint[] = [
  { x: "2019", y: 1020739 },
  { x: "2020", y: 1804594 },
  { x: "2021", y: 1612811 },
  { x: "2022", y: 5270397 },
  { x: "2023", y: 9631748 },
  { x: "2024", y: 12793989 },
  { x: "2025", y: 11578170 },
  { x: "2026", y: 6163481 },
];

// Coût unitaire d'un trajet validé, en € (carte Metabase 445) : coût de
// fonctionnement annuel total / nombre de trajets validés dans l'année.
export const COUT_UNITAIRE_TRAJET: SeriePoint[] = [
  { x: "2019", y: 0.5137 },
  { x: "2020", y: 0.2319 },
  { x: "2021", y: 0.3348 },
  { x: "2022", y: 0.1117 },
  { x: "2023", y: 0.0783 },
  { x: "2024", y: 0.0498 },
  { x: "2025", y: 0.0542 },
];

// Demandes de CEE enregistrées mensuellement par le RPC — courte distance
// (carte Metabase 409).
export const CEE_COURTE_DISTANCE: SeriePoint[] = [
  { x: "2023-01", y: 24488 },
  { x: "2023-02", y: 19101 },
  { x: "2023-03", y: 22137 },
  { x: "2023-04", y: 17363 },
  { x: "2023-05", y: 14617 },
  { x: "2023-06", y: 12898 },
  { x: "2023-07", y: 12174 },
  { x: "2023-08", y: 12690 },
  { x: "2023-09", y: 23443 },
  { x: "2023-10", y: 23229 },
  { x: "2023-11", y: 26830 },
  { x: "2023-12", y: 29806 },
  { x: "2024-01", y: 32685 },
  { x: "2024-02", y: 32793 },
  { x: "2024-03", y: 40329 },
  { x: "2024-04", y: 26894 },
  { x: "2024-05", y: 23396 },
  { x: "2024-06", y: 28834 },
  { x: "2024-07", y: 25616 },
  { x: "2024-08", y: 18727 },
  { x: "2024-09", y: 35409 },
  { x: "2024-10", y: 40520 },
  { x: "2024-11", y: 70528 },
  { x: "2024-12", y: 187064 },
  { x: "2025-01", y: 1304 },
  { x: "2025-02", y: 2236 },
  { x: "2025-03", y: 1078 },
  { x: "2025-04", y: 452 },
  { x: "2025-05", y: 13 },
];

// Demandes de CEE enregistrées mensuellement par le RPC — longue distance
// (carte Metabase 414 ; le mois 2023-01 est présent 2× dans la source, agrégé ici).
export const CEE_LONGUE_DISTANCE: SeriePoint[] = [
  { x: "2023-01", y: 44470 },
  { x: "2023-02", y: 30717 },
  { x: "2023-03", y: 37402 },
  { x: "2023-04", y: 42451 },
  { x: "2023-05", y: 39477 },
  { x: "2023-06", y: 30062 },
  { x: "2023-07", y: 44496 },
  { x: "2023-08", y: 45013 },
  { x: "2023-09", y: 32035 },
  { x: "2023-10", y: 28762 },
  { x: "2023-11", y: 22609 },
  { x: "2023-12", y: 21194 },
  { x: "2024-01", y: 748 },
  { x: "2024-02", y: 1738 },
  { x: "2024-03", y: 1670 },
  { x: "2024-04", y: 1478 },
  { x: "2024-05", y: 1431 },
  { x: "2024-06", y: 78 },
];

const MONTHS_FR = [
  "janv.", "févr.", "mars", "avr.", "mai", "juin",
  "juil.", "août", "sept.", "oct.", "nov.", "déc.",
];

// "2023-01" -> "janv. 2023" ; toute autre valeur est renvoyée telle quelle.
export function frMonthLabel(value: string): string {
  const match = /^(\d{4})-(\d{2})$/.exec(value);
  if (!match) return value;
  return `${MONTHS_FR[Number(match[2]) - 1]} ${match[1]}`;
}
