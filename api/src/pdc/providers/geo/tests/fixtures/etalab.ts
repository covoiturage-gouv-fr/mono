// Réponses réelles (tronquées aux champs lus) de geo.api.gouv.fr et api-adresse.data.gouv.fr.

export const geoApi = {
  // GET /communes?lon=2.26188&lat=48.721505&fields=code&format=json
  positionToInsee: [{ code: "91377", nom: "Massy" }],
  // GET /communes?code=91377&fields=centre&format=json
  inseeToPosition: [
    { centre: { type: "Point", coordinates: [2.2696, 48.7263] }, nom: "Massy", code: "91377" },
  ],
  // position en plein Atlantique, code inconnu
  empty: [],
};

export const ban = {
  // GET /search?q=30+avenue+carnot+massy&limit=1&autocomplete=0
  search: {
    type: "FeatureCollection",
    features: [{
      type: "Feature",
      geometry: { type: "Point", coordinates: [2.264861, 48.728599] },
      properties: {
        label: "30 Avenue Carnot 91300 Massy",
        score: 0.970421818181818,
        postcode: "91300",
        citycode: "91377",
        city: "Massy",
      },
    }],
    query: "30 avenue carnot massy",
  },
  // GET /reverse?lat=48.721505&lon=2.26188 (1er résultat sur 10)
  reverse: {
    type: "FeatureCollection",
    features: [{
      type: "Feature",
      geometry: { type: "Point", coordinates: [2.26247, 48.721571] },
      properties: {
        label: "4a Rue Ampère 91300 Massy",
        postcode: "91300",
        citycode: "91377",
        city: "Massy",
        distance: 44,
      },
    }],
  },
  // /search sans résultat et /reverse hors France
  empty: { type: "FeatureCollection", features: [] },
};
