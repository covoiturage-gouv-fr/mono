import { assertEquals, assertRejects } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { InvalidRequestException } from "@/ilos/common/index.ts";
import { resolveSimulatedTerritory, toCampaignTerritory } from "./campaignConditionTerritory.ts";

describe("toCampaignTerritory", () => {
  const group = (has_versions: boolean, selectors: { type: string; value: string }[]) => ({
    name: "Territoire",
    has_versions,
    selectors,
  });

  it("un territoire à périmètre versionné est un territoire custom du datalake", () => {
    assertEquals(toCampaignTerritory(36709, group(true, [{ type: "epci", value: "200067999" }])), {
      territory_type: "custom",
      territory_code: "36709",
      territory_label: "Territoire",
    });
  });

  it("un sélecteur unique de type géographique donne son couple type/code", () => {
    assertEquals(toCampaignTerritory(12, group(false, [{ type: "aom", value: "200067999" }])), {
      territory_type: "aom",
      territory_code: "200067999",
      territory_label: "Territoire",
    });
  });

  it("refuse un territoire inconnu du datalake (plusieurs sélecteurs, arrondissement, aucun)", () => {
    const two = [{ type: "epci", value: "200067999" }, { type: "epci", value: "200069730" }];
    assertEquals(toCampaignTerritory(12, group(false, two)), null);
    assertEquals(toCampaignTerritory(12, group(false, [{ type: "arr", value: "69381" }])), null);
    assertEquals(toCampaignTerritory(12, group(false, [])), null);
  });
});

describe("resolveSimulatedTerritory", () => {
  const resolved = { territory_type: "custom" as const, territory_code: "7", territory_label: "T" };
  const caller = (registry: boolean) => ({ registry, operator_id: null, territory_id: null, territory: null });
  const repo = (r: typeof resolved | null) => ({ resolveTerritory: () => Promise.resolve(r) });

  it("l'équipe RPC avec territory_id obtient le territoire résolu", async () => {
    assertEquals(await resolveSimulatedTerritory(repo(resolved), caller(true), 7), resolved);
  });

  it("sans territory_id ou hors équipe RPC, rien", async () => {
    assertEquals(await resolveSimulatedTerritory(repo(resolved), caller(true), undefined), null);
    assertEquals(await resolveSimulatedTerritory(repo(resolved), caller(false), 7), null);
  });

  it("refuse un territoire sans équivalent", async () => {
    await assertRejects(() => resolveSimulatedTerritory(repo(null), caller(true), 7), InvalidRequestException);
  });
});
