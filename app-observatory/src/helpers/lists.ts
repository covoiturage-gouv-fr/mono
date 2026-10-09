export const yearList = [
  { id: 2026, name: "2026" },
  { id: 2025, name: "2025" },
  { id: 2024, name: "2024" },
  { id: 2023, name: "2023" },
  { id: 2022, name: "2022" },
  { id: 2021, name: "2021" },
  { id: 2020, name: "2020" },
];

// Dernier millésime du référentiel géo. Au-delà, l'API territoires sert `is_latest`
// (le millésime courant peut être en avance sur les données réellement chargées).
export const latestMillesime = Math.max(...yearList.map((y) => y.id));

// Millésime à demander à l'API territoires pour une année de dashboard :
// l'année elle-même si elle est antérieure au dernier millésime, sinon `undefined`
// (l'API sert alors `is_latest`).
export const targetMillesime = (year: number) => year < latestMillesime ? year : undefined;

export const monthList = [
  { id: 1, name: "Janvier" },
  { id: 2, name: "Février" },
  { id: 3, name: "Mars" },
  { id: 4, name: "Avril" },
  { id: 5, name: "Mai" },
  { id: 6, name: "Juin" },
  { id: 7, name: "Juillet" },
  { id: 8, name: "Août" },
  { id: 9, name: "Septembre" },
  { id: 10, name: "Octobre" },
  { id: 11, name: "Novembre" },
  { id: 12, name: "Décembre" },
];

export const trimesterList = [
  { id: 1, name: "Janvier - Mars" },
  { id: 2, name: "Avril - Juin" },
  { id: 3, name: "Juillet - Septembre" },
  { id: 4, name: "Octobre - Décembre" },
];

export const semesterList = [
  { id: 1, name: "Janvier - Juin" },
  { id: 2, name: "Juillet - Décembre" },
];

export const periodList = [
  { id: "month", name: "Mois" },
  { id: "trimester", name: "Trimestre" },
  { id: "semester", name: "Semestre" },
  { id: "year", name: "Année" },
];

export const territoryList = [
  { id: "com", name: "Communes" },
  { id: "epci", name: "EPCI" },
  { id: "aom", name: "AOM" },
  { id: "dep", name: "Départements" },
  { id: "reg", name: "Régions" },
  { id: "country", name: "Pays" },
];

// Niveaux observables dans un territoire : ceux plus fins que lui. Un territoire
// custom n'a pas de rang : il peut chevaucher plusieurs départements.
export const observeList = (type: string) => {
  if (type === "custom") return territoryList.filter((d) => ["com", "epci", "aom", "dep"].includes(d.id));
  const index = territoryList.findIndex((d) => d.id === type);
  return index === -1 ? [] : territoryList.slice(0, index);
};

export const mapList = [
  { id: 1, name: "Flux de trajets" },
  { id: 2, name: "Densité de départs et arrivées dans une zone" },
  { id: 3, name: "Taux d'occupation et nombre de véhicules partagés" },
  { id: 4, name: "Aires de covoiturage" },
  //{ id: 5, name: "Voies réservées de covoiturage" },
];

export const graphList = [
  { id: 1, name: "Evolution du nombre de trajets" },
  { id: 2, name: "Evolution de la distance moyenne des trajets (en km)" },
  { id: 3, name: "Evolution du taux d'occupation" },
  //{ id: 4, name: "Evolution du nombre de véhicules partagés" },
];
