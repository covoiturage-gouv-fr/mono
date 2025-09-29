import { FrCxArg } from "@codegouvfr/react-dsfr";
import type { LayersList, PickingInfo } from "@deck.gl/core";
import { LngLatBoundsLike, MapLayerMouseEvent } from "maplibre-gl";
import { ReactNode } from "react";
import { INSEECode, PerimeterType } from "./Perimeter";

export interface ViewInterface {
  latitude: number;
  longitude: number;
  zoom: number;
}

export interface LegendInterface {
  title: string;
  type: string;
  classes: ClasseInterface[];
  order?: number;
}

export interface ClasseInterface {
  color: number[];
  val: string;
  width: number;
}

export interface MapInterface {
  title?: string;
  height?: string | number;
  width?: string | number;
  initialView?: ViewInterface;
  mapStyle?: string;
  scrollZoom?: boolean;
  bounds?: LngLatBoundsLike;
  download?: ReactNode;
  children?: ReactNode;
  legend?: LegendInterface[];
  sidebar?: ReactNode;
  sidebarPosition?: "left" | "right";
  sidebarWidth?: number;
  cursor?: string;
  onMouseEnter?: (e: MapLayerMouseEvent) => void;
  onMouseLeave?: (e: MapLayerMouseEvent) => void;
  onMouseMove?: (e: MapLayerMouseEvent) => void;
  onClick?: (e: MapLayerMouseEvent) => void;
  interactiveLayerIds?: string[];
}

export interface DeckMapInterface extends MapInterface {
  layers?: LayersList;
  tooltip?: ((info: PickingInfo) => DeckTooltipContent) | null;
}

export interface DeckTooltipContent {
  html: string;
  className: string;
  style: object;
}

export interface SearchParamsInterface {
  code: INSEECode;
  type: PerimeterType;
  observe: PerimeterType;
  year: number;
  month?: number;
  trimester?: number;
  semester?: number;
  period: PeriodType;
  map?: number;
  graph?: number;
}

export interface RowsProps {
  data: any[];
}

export interface IndicatorProps {
  __component?: string;
  value: string;
  unit: string;
  text: string;
  info?: string;
  icon?: FrCxArg;
  link?: string;
}

export interface AnalyseProps {
  title?: string;
  content: string;
  link?: {
    title: string;
    url: string;
  };
}

export interface SingleMapProps {
  title: string;
  params: SearchParamsInterface;
}

export type SingleGraphProps = SingleMapProps;

export type FluxIndicators =
  | "journeys"
  | "passengers"
  | "has_incentive"
  | "distance";
export type OccupationIndicators =
  | "journeys"
  | "trips"
  | "has_incentive"
  | "occupation_rate";
export type PeriodType = "month" | "trimester" | "semester" | "year";
