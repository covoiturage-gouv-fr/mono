export const perimeterTypes = [
  "com",
  "epci",
  "aom",
  "dep",
  "reg",
  "country",
] as const;
export type PerimeterType = (typeof perimeterTypes)[number];

export interface TerritoryListInterface {
  territory: string;
  l_territory: string;
  type: PerimeterType;
}
