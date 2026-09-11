import { Config } from "@/config";
import type { PerimeterType } from "@/interfaces/searchInterface";

export const getApiUrl = (version: string, path: string) => {
  const host = Config.get<string>("next.public_api_url", "");
  return `${host}/${version}/${path}`;
};

// Base de l'API datalake ; le path /observatory est fixé ici, pas dans la var d'env.
export const DATALAKE_API_URL = `${Config.get<string>("next.public_datalake_base_url", "")}/observatory`;

// Borne de `q` côté route (api-datalake, `Query(max_length=64)`) : au-delà elle répond 422.
const SEARCH_MAX_QUERY = 64;

export interface TerritorySearchResult {
  id: string;
  territory: string;
  l_territory: string;
  type: PerimeterType;
  year: number;
}

// Autocomplete de sélection de territoire via la route datalake `/observatory/territories/search`
// (remplace l'index Meilisearch `geo`). Sans `year`, l'API sert le dernier millésime (`is_latest`).
export async function searchTerritories(q: string | null, limit = 20, signal?: AbortSignal): Promise<TerritorySearchResult[]> {
  const query = (q ?? "").trim().slice(0, SEARCH_MAX_QUERY);
  if (!query) return [];
  const params = new URLSearchParams({ q: query, limit: String(limit) });
  const url = `${DATALAKE_API_URL}/territories/search?${params}`;
  // L'autocomplete ne sait rien afficher d'autre qu'une liste vide : on journalise la
  // panne pour ne pas la confondre avec une recherche sans résultat.
  try {
    const response = await fetch(url, { signal });
    if (!response.ok) {
      console.error(`Recherche de territoires : HTTP ${response.status}`);
      return [];
    }
    const data: unknown = await response.json();
    if (!Array.isArray(data)) {
      console.error("Recherche de territoires : réponse inattendue");
      return [];
    }
    return data as TerritorySearchResult[];
  } catch (e) {
    // Requête remplacée par une frappe plus récente : ce n'est pas une panne.
    if (e instanceof Error && e.name === "AbortError") return [];
    console.error("Recherche de territoires injoignable", e);
    return [];
  }
}
