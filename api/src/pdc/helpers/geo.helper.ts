import { Timezone } from "@/pdc/providers/validator/types.ts";

export function getTzFromLon(lon: number): Timezone {
  switch (true) {
    // France métropolitaine
    case lon >= -5.5 && lon <= 9.8:
      return "Europe/Paris";
    // Guadeloupe & Martinique (UTC-4)
    case lon >= -62 && lon <= -60:
      return "America/Guadeloupe";
    // Guyane (UTC-3)
    case lon >= -54 && lon <= -51:
      return "America/Cayenne";
    // Saint-Pierre-et-Miquelon (UTC-3)
    case lon >= -56.5 && lon <= -56.1:
      return "America/Miquelon";
    // Réunion (UTC+4)
    case lon >= 55 && lon <= 56:
      return "Indian/Reunion";
    // Mayotte (UTC+3)
    case lon >= 44 && lon <= 46:
      return "Indian/Mayotte";
    // Nouvelle-Calédonie (UTC+11)
    case lon >= 158 && lon <= 172:
      return "Pacific/Noumea";
    // Polynésie française (UTC-10 à UTC-9)
    case lon >= -155 && lon <= -135:
      return "Pacific/Tahiti";
    // Wallis-et-Futuna (UTC+12)
    case lon >= -178.3 && lon <= -176.1:
      return "Pacific/Wallis";
    default:
      return "UTC";
  }
}
