import { describe, expect, test } from "vitest";
import type { BarometerMeasure, BarometerPoint } from "@/interfaces/observatoire/barometer";
import {
  chartsOf,
  filterPoints,
  periodLabel,
  pointsToGeoJSON,
  providerColors,
  providersOf,
} from "./barometer";

const point = (id: number, fournisseur_id: string, fournisseur_nom = fournisseur_id.toUpperCase()): BarometerPoint => ({
  id,
  nom: `Point ${id}`,
  fournisseur_id,
  fournisseur_nom,
  gestionnaire: null,
  departement: null,
  axe: null,
  sens: null,
  pk: null,
  lat: 46,
  lon: 2,
  voie_droite: null,
  voie_milieu: null,
  voie_gauche: null,
  infrarouge: null,
  comptage_arriere: null,
  technologie: null,
});

const points = [point(1, "vinci"), point(2, "cerema"), point(3, "vinci")];

const measure = (m: Partial<BarometerMeasure>): BarometerMeasure => ({
  id: 1,
  debut: "2024-06-03",
  fin: "2024-06-03",
  jour: null,
  heure: null,
  nb_vl: 100,
  tx_covoiturage: 10,
  ...m,
});

describe("providersOf", () => {
  test("liste chaque fournisseur une fois, triés par nom", () => {
    expect(providersOf(points)).toEqual([
      { id: "cerema", name: "CEREMA" },
      { id: "vinci", name: "VINCI" },
    ]);
  });
});

describe("filterPoints", () => {
  test("garde les points du fournisseur", () => {
    expect(filterPoints(points, "vinci").map((p) => p.id)).toEqual([1, 3]);
  });

  test("sans fournisseur, garde tout", () => {
    expect(filterPoints(points, null)).toHaveLength(3);
  });
});

describe("providerColors", () => {
  test("attribue une couleur stable par fournisseur", () => {
    const colors = providerColors(providersOf(points));
    expect(Object.keys(colors)).toEqual(["cerema", "vinci"]);
    expect(colors.cerema).not.toEqual(colors.vinci);
  });
});

describe("pointsToGeoJSON", () => {
  test("place les points en [lon, lat] avec leur id et leur fournisseur", () => {
    const fc = pointsToGeoJSON([{ ...point(1, "vinci"), lat: 45.5, lon: 4.5 }]);
    expect(fc.features[0].geometry).toEqual({ type: "Point", coordinates: [4.5, 45.5] });
    expect(fc.features[0].properties).toMatchObject({ id: 1, fournisseur_id: "vinci" });
  });
});

describe("periodLabel", () => {
  test("un seul jour", () => {
    expect(periodLabel("2024-06-03", "2024-06-03")).toBe("3 juin 2024");
  });

  test("un mois entier", () => {
    expect(periodLabel("2025-02-01", "2025-02-28")).toBe("février 2025");
  });

  test("une plage quelconque", () => {
    expect(periodLabel("2025-06-10", "2025-06-27")).toBe("du 10/06/2025 au 27/06/2025");
  });
});

describe("chartsOf", () => {
  test("heure remplie : taux par heure, une série par période, heures triées", () => {
    const charts = chartsOf([
      measure({ heure: 8, tx_covoiturage: 14 }),
      measure({ heure: 7, tx_covoiturage: 12 }),
      measure({ debut: "2025-06-10", fin: "2025-06-10", heure: 7, tx_covoiturage: 15 }),
    ]);
    expect(charts).toEqual([
      {
        key: "heure",
        title: "Taux de covoiturage par heure",
        kind: "bar",
        labels: ["7h", "8h"],
        series: [
          { label: "3 juin 2024", data: [12, 14] },
          { label: "10 juin 2025", data: [15, NaN] },
        ],
      },
    ]);
  });

  test("jour et heure remplis : le jour complète le libellé de la série", () => {
    const [chart] = chartsOf([measure({ jour: 1, heure: 7 })]);
    expect(chart.series[0].label).toBe("3 juin 2024 (lundi)");
  });

  test("jour rempli, heure vide : moyenne par jour de la semaine", () => {
    const charts = chartsOf([
      measure({ debut: "2025-06-01", fin: "2025-06-30", jour: 2, tx_covoiturage: 11 }),
      measure({ debut: "2025-06-01", fin: "2025-06-30", jour: 1, tx_covoiturage: 10 }),
    ]);
    expect(charts).toEqual([
      {
        key: "jour",
        title: "Taux de covoiturage moyen par jour de la semaine",
        kind: "bar",
        labels: ["lundi", "mardi"],
        series: [{ label: "juin 2025", data: [10, 11] }],
      },
    ]);
  });

  test("jour et heure vides : évolution de la moyenne par période, en courbe à partir de 3 périodes", () => {
    const month = (m: string, tx: number) =>
      measure({ debut: `2025-${m}-01`, fin: `2025-${m}-${m === "02" ? "28" : "30"}`, tx_covoiturage: tx });
    const [chart] = chartsOf([month("06", 12), month("04", 10), month("02", 9)]);
    expect(chart).toEqual({
      key: "periode",
      title: "Évolution du taux de covoiturage moyen",
      kind: "line",
      labels: ["février 2025", "avril 2025", "juin 2025"],
      series: [{ label: "Taux moyen", data: [9, 10, 12] }],
    });
  });

  test("n'affiche que les graphiques pour lesquels il y a des mesures, dans un ordre fixe", () => {
    const keys = chartsOf([
      measure({ debut: "2025-06-01", fin: "2025-06-30" }),
      measure({ heure: 7 }),
    ]).map((c) => c.key);
    expect(keys).toEqual(["heure", "periode"]);
    expect(chartsOf([])).toEqual([]);
  });
});
