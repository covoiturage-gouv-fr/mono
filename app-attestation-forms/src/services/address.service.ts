interface Feature {
  properties: {
    label: string;
  };
}

const endpoint = "https://api-adresse.data.gouv.fr/search/";

export async function searchAddress(term: string): Promise<string[]> {
  if (!term.trim()) return [];

  const q = encodeURIComponent(term.trim());

  try {
    const res = await fetch(`${endpoint}?q=${q}&limit=20`);
    const data: { features: Feature[] } = await res.json();
    return data.features.map((f) => f.properties.label);
  } catch (err) {
    console.error(err);
    return [];
  }
}
