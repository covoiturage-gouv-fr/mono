import { assertEquals, assertStringIncludes } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import excel from "dep:excel";
import { SliceStatInterface } from "../../contracts/interfaces/PolicySliceStatInterface.ts";
import { SlicesWorksheetWriter } from "./SlicesWorksheetWriter.ts";

describe("SlicesWorksheetWriter — campagne avec déclaré (GEN-643)", () => {
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

  function hasCellValue(ws: excel.Worksheet, predicate: (v: string) => boolean): boolean {
    let found = false;
    ws.eachRow((row) =>
      row.eachCell((c) => {
        if (predicate(String(c.value ?? ""))) found = true;
      })
    );
    return found;
  }

  it("résume le calculé + la contribution passagers, sans bandeau ni colonne déclarée", async () => {
    const ws = await render(true);
    // Pas de bandeau : en-têtes en ligne 1, 1re tranche en ligne 2.
    assertStringIncludes(String(ws.getCell("A1").value), "période normale");
    assertStringIncludes(String(ws.getCell("B1").value), "Montant d'incitation");
    assertStringIncludes(String(ws.getCell("E1").value), "Contribution passagers");

    // Données : B calculé (R), C tous, D incités, E contribution passagers (T).
    assertStringIncludes(ws.getCell("B2").formula, "SUMIFS(Trajets!R:R");
    assertStringIncludes(ws.getCell("C2").formula, "COUNTIFS(Trajets!S:S");
    assertStringIncludes(ws.getCell("D2").formula, 'COUNTIFS(Trajets!R:R,">0"');
    assertStringIncludes(ws.getCell("E2").formula, "SUMIFS(Trajets!T:T");

    // Aucune colonne déclarée (montant U / trajets incités déclarés) dans la synthèse.
    assertEquals(ws.getCell("F2").value, null);
  });

  it("n'affiche plus les bandeaux calculé / déclaré", async () => {
    const ws = await render(true);
    assertEquals(hasCellValue(ws, (v) => v.includes("déclarées par les opérateurs")), false);
    assertEquals(hasCellValue(ws, (v) => v.includes("calculées par covoiturage.beta")), false);
  });

  it("ne produit plus de bloc Delta", async () => {
    const ws = await render(true);
    assertEquals(hasCellValue(ws, (v) => v.includes("Delta")), false);
  });

  it("conserve la définition operator_declared_incentive dans la documentation", async () => {
    const ws = await render(true);
    assertEquals(hasCellValue(ws, (v) => v === "operator_declared_incentive"), true);
  });

  it("laisse le layout générique inchangé quand le déclaré est absent", async () => {
    const ws = await render(false);
    // En-têtes en ligne 1, 1re tranche en ligne 2, colonne E = contribution passagers.
    assertStringIncludes(ws.getCell("E2").formula, "SUMIFS(Trajets!T:T");
    assertEquals(ws.getCell("F2").value, null);
  });
});
