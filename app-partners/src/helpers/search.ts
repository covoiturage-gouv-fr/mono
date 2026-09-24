import type { PerimeterType } from "@/interfaces/searchInterface";

export const castPerimeterType = (value: PerimeterType) => {
  switch (value) {
    case "com":
      return "Commune";
    case "epci":
      return "Communauté de commune";
    case "aom":
      return "Autorité organisatrice des mobilités";
    case "dep":
      return "Département";
    case "reg":
      return "Région";
    case "country":
      return "Pays";
  }
};
