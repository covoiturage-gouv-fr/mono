// Réponses de l'API REST Strapi 4 pour les types de contenu de cms/src.
// Les attributs non demandés (`fields`) ou relations non peuplées (`populate`) sont absents à l'exécution.
import { IndicatorProps, SearchParamsInterface } from "@/interfaces/observatoire/componentsInterfaces";

export interface StrapiEntity<A> {
  id: number;
  attributes: A;
}

export interface StrapiPagination {
  page: number;
  pageSize: number;
  pageCount: number;
  total: number;
}

export interface StrapiResponse<A> {
  data: StrapiEntity<A>[];
  meta: { pagination: StrapiPagination };
}

export interface StrapiImageFormat {
  url: string;
  width: number;
  height: number;
}

// Attributs du plugin upload : width/height/formats sont null hors images.
export interface StrapiFile {
  name: string;
  url: string;
  alternativeText?: string | null;
  ext: string;
  size: number;
  width: number | null;
  height: number | null;
  formats: Partial<Record<"thumbnail" | "small" | "medium" | "large", StrapiImageFormat>> | null;
}

export interface StrapiMedia {
  data: StrapiEntity<StrapiFile> | null;
}

export interface StrapiRelation<A> {
  data: StrapiEntity<A>[];
}

export interface Button {
  title: string;
  url: string;
  icon?: string;
  color?: "primary" | "secondary";
}

export interface Hero {
  title: string;
  subtitle?: string;
  content: string;
  img: StrapiMedia;
  buttons: Button[];
}

export type Block = Omit<Hero, "subtitle">;

export interface HighlightComponent extends Block {
  __component: "page.highlight";
}

export interface ListTitleComponent {
  __component: "page.list-title";
  title: string;
}

export interface TitleRow {
  __component: "row.title";
  title: string;
}

export type IndicatorRow = Omit<IndicatorProps, "__component"> & { __component: "row.indicator" };

export interface AnalyseRow {
  __component: "row.analyse";
  title?: string;
  content: string;
  link?: { title: string; url: string };
}

export interface MapRow {
  __component: "row.map";
  title: string;
  params: SearchParamsInterface;
}

export interface GraphRow {
  __component: "row.graph";
  title: string;
  params: SearchParamsInterface;
}

export type Row = TitleRow | IndicatorRow | AnalyseRow | MapRow | GraphRow;

export interface CardRow {
  __component: "row.card";
  title: string;
  content: string;
  img: StrapiMedia;
  tags: StrapiRelation<ServiceTag>;
  buttons: Button[];
}

export type VitrineRow = TitleRow | CardRow;

export interface Categorie {
  label: string;
  slug: string;
}

export interface ServiceTag {
  label: string;
  slug: string;
}

export interface Tag {
  label: string;
  slug: string;
}

export interface Article {
  title: string;
  description: string;
  content?: string;
  img: StrapiMedia;
  legend?: string;
  categories: StrapiRelation<Categorie>;
  slug: string;
  buttons: Button[];
  createdAt: string;
  updatedAt: string;
  publishedAt: string;
}

export interface Resource {
  title: string;
  content: string;
  public_date: string;
  slug: string;
  categories: StrapiRelation<Categorie>;
  img: StrapiMedia;
  link?: string;
  file: StrapiMedia;
  buttons: Button[];
  createdAt: string;
  updatedAt: string;
}

export interface Page {
  title: string;
  tags: StrapiRelation<Tag>;
  hero?: Hero | null;
  block?: Block | null;
  content?: string;
  slug: string;
  rows: Row[];
  list: (HighlightComponent | ListTitleComponent)[];
  resources: StrapiRelation<Resource>;
  complement?: string;
}

export interface PageVitrine {
  Hero?: Hero | null;
  Rows: VitrineRow[];
}
