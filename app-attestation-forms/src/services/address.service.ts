import { z } from "zod";

const searchResponseSchema = z.object({
  features: z.array(z.object({ properties: z.object({ label: z.string() }) })),
});

const endpoint = "https://api-adresse.data.gouv.fr/search/";

export async function searchAddress(term: string): Promise<string[]> {
  if (!term.trim()) return [];

  const q = encodeURIComponent(term.trim());

  try {
    const res = await fetch(`${endpoint}?q=${q}&limit=20`);
    const data = searchResponseSchema.parse(await res.json());
    return data.features.map((f) => f.properties.label);
  } catch (err) {
    console.error(err);
    return [];
  }
}
