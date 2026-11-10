import { Geometry } from "geojson";
import {
  Direction,
  Distance,
  Hour,
  INSEECode,
  PerimeterLabel,
  PerimeterType,
} from "./Perimeter";

export interface KeyFiguresDataInterface {
  territory: INSEECode;
  l_territory: PerimeterLabel;
  passengers: number;
  distance: number;
  duration: number;
  journeys: number;
  intra_journeys: number;
  occupation_rate: number;
  new_drivers: number;
  new_passengers: number;
  direction: Direction;
}

export interface LastFluxDataInterface {
  year: number;
  month: number;
}

export interface FluxDataInterface {
  ter_1: INSEECode;
  lng_1: number;
  lat_1: number;
  ter_2: INSEECode;
  lng_2: number;
  lat_2: number;
  passengers: number;
  distance: number;
  duration: number;
}

export interface DensiteDataInterface {
  hex: string;
  count: number;
}

export interface OccupationDataInterface {
  code: INSEECode;
  libelle: PerimeterLabel;
  journeys: number;
  has_incentive?: number;
  occupation_rate: number;
  geom: Geometry;
}

export interface AiresCovoiturageDataInterface {
  id_lieu: string;
  nom_lieu: string;
  com_lieu: string;
  type: string;
  date_maj: Date;
  nbre_pl: number;
  nbre_pmr: number;
  duree: number;
  horaires: string;
  proprio: string;
  lumiere: boolean;
  geom: Geometry;
}

export interface TerritoryListInterface {
  territory: INSEECode;
  l_territory: PerimeterLabel;
  type: PerimeterType;
}

export interface EvolDataInterface {
  year: number;
}

export interface EvolFluxDataInterface extends EvolDataInterface {
  journeys: number;
  has_incentive: number;
  passengers: number;
  distance: number;
}
export interface EvolDistanceDataInterface extends EvolDataInterface {
  distance: number;
  journeys: number;
}

export interface EvolOccupationDataInterface extends EvolDataInterface {
  journeys: number;
  trips: number;
  has_incentive: number;
  occupation_rate: number;
}

export interface DistributionDistanceDataInterface {
  territory: INSEECode;
  l_territory: PerimeterLabel;
  direction: Direction;
  distances: Distance[];
}
export interface DistributionHoraireDataInterface {
  territory: INSEECode;
  l_territory: PerimeterLabel;
  direction: Direction;
  hours: Hour[];
}

export interface BestFluxDataInterface {
  territory_1: INSEECode;
  l_territory_1: PerimeterLabel;
  territory_2: INSEECode;
  l_territory_2: string;
  journeys: number;
}

export interface BestTerritoriesDataInterface {
  code: INSEECode;
  libelle: PerimeterLabel;
  journeys: number;
}
export interface IncentiveDataInterface {
  code: INSEECode;
  libelle: PerimeterLabel;
  direction: Direction;
  collectivite: number;
  operateur: number;
  autres: number;
  no_incentive: number;
}

// Route /observatory/campaigns (vue zone_exposed.campaigns).
export interface CampaignDataInterface {
  type: PerimeterType;
  code: INSEECode;
  // libellé du périmètre ; null si le territoire n'est pas au dernier millésime
  libelle: string | null;
  premiere_campagne: string | null;
  budget_incitations: string | null;
  date_debut: string | null;
  date_fin: string | null;
  conducteur_montant_max_par_passager: string | null;
  conducteur_montant_max_par_mois: number | null;
  conducteur_montant_min_par_passager: string | null;
  conducteur_trajets_max_par_mois: number | null;
  passager_trajets_max_par_mois: number | null;
  passager_gratuite: string | null;
  passager_eligible_gratuite: string | null;
  passager_reduction_ticket: string | null;
  passager_eligibilite_reduction: string | null;
  passager_montant_ticket: string | null;
  zone_sens_des_trajets: string | null;
  zone_exclusion: string | null;
  si_zone_exclue_liste: string | null;
  autre_exclusion: string | null;
  trajet_longueur_min: number | null;
  trajet_longueur_max: number | null;
  trajet_classe_de_preuve: string | null;
  operateurs: string | null;
  autres_informations: string | null;
  lien: string | null;
  geom: Geometry;
}

// Propriétés GeoJSON d'une campagne (noms courts pour la carte et le détail).
export interface CampaignPropertiesInterface {
  name?: string;
  type: PerimeterType;
  code: INSEECode;
  debut: string | null;
  fin: string | null;
  budget: string | null;
  operateurs: string | null;
  p_gratuit: string | null;
  p_eligible: string | null;
  p_t_max: number | null;
  p_tk: string | null;
  p_e_tk: string | null;
  p_m_tk: string | null;
  p_max: number | null;
  c_m_max_p: string | null;
  c_m_max: number | null;
  c_m_min_p: string | null;
  c_t_max: number | null;
  sens: string | null;
  exclusion: string | null;
  liste_exclusion: string | null;
  autre_exclusion: string | null;
  l_min: number | null;
  l_max: number | null;
  classe: string | null;
  lien: string | null;
}

// Fichier statique voies_covoit_*.json (voies réservées au covoiturage).
export interface VoieReserveePropertiesInterface {
  name: string;
  type: string;
  gestionnaire: string;
  localisation: string;
  mise_en_service: string;
  distance: string;
  link: string;
}
