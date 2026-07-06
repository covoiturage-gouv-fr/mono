import { assertEquals, assertStringIncludes } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import excel from "dep:excel";
import { SliceStatInterface } from "../../contracts/interfaces/PolicySliceStatInterface.ts";
import { SlicesWorksheetWriter } from "./SlicesWorksheetWriter.ts";

describe("SlicesWorksheetWriter — layout déclaré (GEN-643)", () => {
  const slices: SliceStatInterface[] = [
    { count: 10, subsidized: 8, sum: 1000, slice: { start: 0, end: 2000 } },
    { count: 5, subsidized: 4, sum: 500, slice: { start: 2000, end: null } },
  ];

  // Génère la feuille en écrivant un vrai classeur puis en le relisant.
  async function render(withDeclared: boolean): Promise<excel.Worksheet> {
    const path = await Deno.makeTempFile({ suffix: ".xlsx" });
    const wbWriter = new excel.stream.xlsx.WorkbookWriter({ filename: path, useStyles: true });
    await new SlicesWorksheetWriter().call(wbWriter, slices, withDeclared);
    await wbWriter.commit();

    const wb = new excel.Workbook();
    await wb.xlsx.readFile(path);
    await Deno.remove(path);
    return wb.getWorksheet("Synthèse par tranche")!;
  }

  it("affiche les bandeaux calculé / déclaré", async () => {
    const ws = await render(true);
    assertStringIncludes(String(ws.getCell("B1").value), "calculées par covoiturage.beta");
    assertStringIncludes(String(ws.getCell("E1").value), "déclarées par les opérateurs");
  });

  it("agrège le déclaré depuis la colonne U de l'onglet Trajets", async () => {
    const ws = await render(true);
    // ligne bandeau=1, en-têtes=2, 1re tranche=3
    assertStringIncludes(ws.getCell("B3").formula, "SUMIFS(Trajets!R:R"); // calculé (existant)
    assertStringIncludes(ws.getCell("E3").formula, "SUMIFS(Trajets!U:U"); // montant déclaré
    assertStringIncludes(ws.getCell("F3").formula, 'COUNTIFS(Trajets!U:U,">0"'); // trajets incités déclarés
    assertStringIncludes(ws.getCell("G3").formula, "SUMIFS(Trajets!T:T"); // contribution passagers (E→G)
  });

  it("calcule le delta = calculé − déclaré sur la période normale (montant et trajets incités)", async () => {
    const ws = await render(true);
    // total normale=ligne5 (layout déterministe : 2 tranches). Le delta ne porte que sur la normale.
    assertStringIncludes(String(ws.getCell("A15").value), "Delta");
    assertEquals(ws.getCell("B16").formula, "B5-E5"); // delta montant
    assertEquals(ws.getCell("B17").formula, "D5-F5"); // delta trajets incités
  });

  it("masque le déclaré (E/F) sur le tableau booster mais garde la contribution (G)", async () => {
    const ws = await render(true);
    // booster : bandeau=8, en-têtes=9, 1re tranche=10
    assertEquals(ws.getCell("E9").value, null); // pas d'en-tête montant déclaré
    assertEquals(ws.getCell("F9").value, null); // pas d'en-tête trajets déclarés
    assertStringIncludes(String(ws.getCell("G9").value), "Contribution passagers");
    assertEquals(ws.getCell("E10").value, null); // pas de formule déclarée sur le booster
    assertStringIncludes(ws.getCell("G10").formula, "SUMIFS(Trajets!T:T"); // contribution conservée
  });

  it("laisse le layout générique inchangé quand le déclaré est absent (pas de colonne U/G déclaré)", async () => {
    const ws = await render(false);
    // en-têtes en ligne 1, 1re tranche en ligne 2, colonne E = contribution passagers
    assertStringIncludes(ws.getCell("E2").formula, "SUMIFS(Trajets!T:T");
    assertEquals(ws.getCell("F2").value, null); // pas de colonne déclarée F
  });
});
