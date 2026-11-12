// Contrat de /observatory/barometer : mêmes colonnes que les fichiers
// barometre_points.csv et barometre_mesures.csv.

export interface BarometerPoint {
  id: number;
  nom: string;
  fournisseur_id: string;
  fournisseur_nom: string;
  gestionnaire: string | null;
  departement: string | null;
  axe: string | null;
  sens: string | null;
  pk: number | null;
  lat: number;
  lon: number;
  voie_droite: number | null;
  voie_milieu: number | null;
  voie_gauche: number | null;
  infrarouge: number | null;
  comptage_arriere: number | null;
  technologie: string | null;
}

/**
 * `heure` remplie : `nb_vl` est un comptage sur l'heure.
 * `heure` vide : `nb_vl` et `tx_covoiturage` sont des moyennes sur la période,
 * pour le jour de la semaine indiqué si `jour` est rempli (1 = lundi … 7 = dimanche).
 */
export interface BarometerMeasure {
  id: number;
  debut: string;
  fin: string;
  jour: number | null;
  heure: number | null;
  nb_vl: number;
  tx_covoiturage: number;
}

export interface BarometerProvider {
  id: string;
  name: string;
}

export interface BarometerChart {
  key: "heure" | "jour" | "periode";
  title: string;
  kind: "bar" | "line";
  labels: string[];
  series: { label: string; data: number[] }[];
}
