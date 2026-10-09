import { assertEquals, assertRejects } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { NotFoundException } from "@/ilos/common/index.ts";
import { TerritoryRepositoryInterfaceResolver } from "@/pdc/services/export/repositories/TerritoryRepository.ts";
import { TerritorySelectorsInterface } from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";
import { TerritoryService } from "./TerritoryService.ts";

const period = { start_at: new Date("2026-01-01"), end_at: new Date("2026-02-01") };

class FakeTerritoryRepository extends TerritoryRepositoryInterfaceResolver {
  public arrCalls: [number, Date, Date][] = [];

  constructor(
    protected selectors: Record<number, TerritorySelectorsInterface>,
    protected perimeters: Record<number, string[]>,
    protected groupNames: Record<number, string> = {},
  ) {
    super();
  }

  public override getTerritoryNamesBatch(type: string, codes: string[]): Promise<string[]> {
    return Promise.resolve(codes.map((c) => `${type}:${c}`));
  }

  public override getTerritoryGroupNames(ids: number[]): Promise<string[]> {
    return Promise.resolve(ids.flatMap((id) => this.groupNames[id] ? [this.groupNames[id]] : []));
  }

  public override getTerritorySelectors(id: number): Promise<TerritorySelectorsInterface> {
    return Promise.resolve(this.selectors[id] ?? {});
  }

  public override getTerritoryPerimeterArr(id: number, from: Date, to: Date): Promise<string[] | null> {
    this.arrCalls.push([id, from, to]);
    return Promise.resolve(this.perimeters[id] ?? null);
  }
}

function serviceWith(
  selectors: Record<number, TerritorySelectorsInterface> = {},
  perimeters: Record<number, string[]> = {},
  groupNames: Record<number, string> = {},
) {
  const repository = new FakeTerritoryRepository(selectors, perimeters, groupNames);
  return { repository, service: new TerritoryService(repository) };
}

describe("TerritoryService: resolve", () => {
  it("exports the whole country without territory nor geo_selector", async () => {
    const { service } = serviceWith();
    assertEquals(await service.resolve({ ...period }), null);
  });

  it("keeps an administrative geo_selector as is", async () => {
    const { service } = serviceWith();
    assertEquals(await service.resolve({ ...period, geo_selector: { aom: ["217500016"] } }), { aom: ["217500016"] });
  });

  it("resolves a territory_id without perimeter version through its selectors", async () => {
    const { service } = serviceWith({ 1: { aom: ["217500016"] } });
    assertEquals(await service.resolve({ ...period, territory_id: [1] }), { aom: ["217500016"] });
  });

  it("resolves a territory_id with perimeter versions to its arr over the period", async () => {
    const { service, repository } = serviceWith({}, { 2: ["91471", "91477"] });
    assertEquals(await service.resolve({ ...period, territory_id: [2] }), { arr: ["91471", "91477"] });
    assertEquals(repository.arrCalls, [[2, period.start_at, period.end_at]]);
  });

  it("prefers the perimeter versions over the selectors", async () => {
    const { service } = serviceWith({ 3: { aom: ["217500016"] } }, { 3: ["91471"] });
    assertEquals(await service.resolve({ ...period, territory_id: [3] }), { arr: ["91471"] });
  });

  it("refuses a territory_id that resolves to nothing instead of exporting the whole country", async () => {
    const { service } = serviceWith();
    await assertRejects(() => service.resolve({ ...period, territory_id: [4] }), NotFoundException);
  });

  it("resolves custom territories of the geo_selector to their arr", async () => {
    const { service } = serviceWith({}, { 5: ["91471"], 6: ["91477", "91471"] });
    assertEquals(
      await service.resolve({ ...period, geo_selector: { custom: ["5", "6"] } }),
      { arr: ["91471", "91477"] },
    );
  });

  it("merges custom territories with the other geo_selector keys", async () => {
    const { service } = serviceWith({}, { 5: ["91471"] });
    assertEquals(
      await service.resolve({ ...period, geo_selector: { com: ["75056"], custom: ["5"] } }),
      { com: ["75056"], arr: ["91471"] },
    );
  });

  it("refuses an unknown custom territory", async () => {
    const { service } = serviceWith();
    await assertRejects(
      () => service.resolve({ ...period, geo_selector: { custom: ["7"] } }),
      NotFoundException,
    );
  });

  it("gives priority to the geo_selector over territory_id", async () => {
    const { service } = serviceWith({ 1: { aom: ["217500016"] } });
    assertEquals(
      await service.resolve({ ...period, territory_id: [1], geo_selector: { com: ["75056"] } }),
      { com: ["75056"] },
    );
  });
});

describe("TerritoryService: displaySelector", () => {
  const { service } = serviceWith();

  it("keeps the geo_selector as typed, custom territories included", () => {
    assertEquals(
      service.displaySelector({ ...period, geo_selector: { com: ["75056"], custom: ["5"] } }),
      { com: ["75056"], custom: ["5"] },
    );
  });

  it("names the territory_id as territory groups", () => {
    assertEquals(service.displaySelector({ ...period, territory_id: [1, 2] }), { custom: ["1", "2"] });
  });

  it("is null for the whole country", () => {
    assertEquals(service.displaySelector({ ...period }), null);
    assertEquals(service.displaySelector({ ...period, territory_id: [] }), null);
  });
});

describe("TerritoryService: getTerritoryNames", () => {
  it("names custom territories by their territory group", async () => {
    const { service } = serviceWith({}, {}, { 5: "SCoT de test" });
    assertEquals(
      await service.getTerritoryNames({ com: ["75056"], custom: ["5"] }),
      ["com:75056", "SCoT de test"],
    );
  });
});
