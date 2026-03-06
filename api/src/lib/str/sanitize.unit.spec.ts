import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { sanitize } from "./sanitize.ts";

// sanitize campaign names
describe("sanitize helper", () => {
  const list = [
    ["#1_LAVALAGGLO_2022_01", "1_lavalagglo_2022_01"],
    ["#1_MetrRouenNormandie_2022_04", "1_metrrouennormandie_2022_04"],
    ["#1_SMT_2022_02", "1_smt_2022_02"],
    ["Campagne 1", "campagne_1"],
    [
      "Campagne avril_juin 2022 CCPE Karos",
      "campagne_avril_juin_2022_ccpe_karos",
    ],
    ["Covoiturage multiopérateur 2022", "covoiturage_multioperateur_2022"],
    [
      "Encourager financièrement le covoiturage",
      "encourager_financierement_le_covoiturage",
    ],
    [
      "Encourager financièrement le covoiturage - PMGV",
      "encourager_financierement_le_covoiturage-pmgv",
    ],
    [
      "Encourager financièrement le covoiturage - Pays de la Loire",
      "encourager_financierement_le_covoiturage-pays_de_la_loire",
    ],
    ["Expérimentation covoit'tan", "experimentation_covoittan"],
    ["IDFM", "idfm"],
    ["IDFM - Période normale", "idfm-periode_normale"],
    ["JECOVOIT.2021", "jecovoit_2021"],
    ["KLIMA - 2€ par trajet puis 0,1€/km", "klima-2e_par_trajet_puis_01e_km"],
    [
      "KLIMA - Echelonné de 20 à 10 centimes/km",
      "klima-echelonne_de_20_a_10_centimes_km",
    ],
    ["Test", "test"],
    ["Test de campagne", "test_de_campagne"],
    ["covoiturage Arlysère", "covoiturage_arlysere"],
    ["test 1", "test_1"],
  ];

  for (const [src, trg] of list) {
    it(`Sanitize ${src}`, (t) => {
      assertEquals(sanitize(src, 128), trg);
    });
  }
});
