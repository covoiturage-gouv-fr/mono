export const perimeterTypes = [
  "com",
  "epci",
  "aom",
  "dep",
  "reg",
  "country",
  "custom",
] as const;
export type PerimeterType = (typeof perimeterTypes)[number];
