import { Geometry } from "geojson";
import { Direction, Distance, Hour, INSEECode, PerimeterLabel, PerimeterType } from "./Perimeter";

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
  territory: INSEECode;
  l_territory: PerimeterLabel;
  journeys: number;
  has_incentive: number;
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
  distances: Array<Distance>;
}
export interface DistributionHoraireDataInterface {
  territory: INSEECode;
  l_territory: PerimeterLabel;
  direction: Direction;
  hours: Array<Hour>;
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
}
