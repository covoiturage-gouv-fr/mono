import { assertEquals, assertRejects } from "dep:assert";
import { beforeEach, describe, it } from "dep:testing-bdd";
import { TerritorySelectorsInterface } from "../contracts/common/interfaces/TerritoryCodeInterface.ts";
import { TerritoryPerimeterInterface } from "../contracts/common/interfaces/TerritoryPerimeterInterface.ts";
import { findVersionAt } from "../helpers/perimeters.ts";
import { PerimeterRepositoryProviderInterfaceResolver } from "../interfaces/PerimeterRepositoryProviderInterface.ts";
import { PerimeterCommand } from "./PerimeterCommand.ts";

const RESOLVED: Record<string, string[]> = {
  "epci:200056232": ["91377", "91471", "91477"],
  "com:91471": ["91471"],
  "arr:69381": ["69381"],
};

const STANDARD = 1;
const CUSTOM = 57;

class FakePerimeterRepository extends PerimeterRepositoryProviderInterfaceResolver {
  territories = new Map<number, { name: string; company?: string; selectors: string[] }>([
    [STANDARD, { name: "standard", selectors: ["91377", "91477"] }],
    [CUSTOM, { name: "custom", selectors: [] }],
  ]);
  versions = new Map<number, TerritoryPerimeterInterface[]>();
  evolutions = [{ old_com: "91471", new_com: "91999" }];

  of(territory_id: number): TerritoryPerimeterInterface[] {
    return this.versions.get(territory_id) ?? [];
  }

  async findTerritory(territory_id: number) {
    const t = this.territories.get(territory_id);
    return t ? { _id: territory_id, name: t.name } : undefined;
  }

  async findTerritoryByName(name: string) {
    const found = [...this.territories].find(([, t]) => t.name.toLowerCase() === name.trim().toLowerCase());
    return found ? { _id: found[0], name: found[1].name } : undefined;
  }

  async createTerritory(name: string, siret: string | undefined, data: Omit<TerritoryPerimeterInterface, "version">) {
    const _id = Math.max(...this.territories.keys()) + 1;
    this.territories.set(_id, { name, company: siret, selectors: [] });
    await this.create(_id, data);
    return _id;
  }

  async findTerritoriesWithVersions() {
    return [...this.versions.keys()];
  }

  async findByTerritory(territory_id: number) {
    return this.of(territory_id);
  }

  async create(territory_id: number, data: Omit<TerritoryPerimeterInterface, "version">) {
    const versions = this.of(territory_id);
    const v = { ...data, version: versions.length + 1 };
    this.versions.set(territory_id, [...versions, v]);
    return v;
  }

  async getArr(territory_id: number, at: Date) {
    return findVersionAt(this.of(territory_id), at)?.arr ?? this.territories.get(territory_id)!.selectors;
  }

  async resolve(selectors: TerritorySelectorsInterface) {
    const keys = Object.entries(selectors).flatMap(([t, codes]) => codes!.map((c: string) => `${t}:${c}`));
    return {
      arr: [...new Set(keys.flatMap((k) => RESOLVED[k] ?? []))].sort(),
      unknown: keys.filter((k) => !RESOLVED[k]),
    };
  }

  async findEvolutions() {
    return this.evolutions;
  }

  async describe(arr: string[]) {
    return arr.map((a) => ({ arr: a, label: `label ${a}`, pop: 1 }));
  }

  async findPolicies() {
    return [];
  }
}

describe("PerimeterCommand", () => {
  let repository: FakePerimeterRepository;
  let command: PerimeterCommand;

  beforeEach(() => {
    repository = new FakePerimeterRepository();
    command = new PerimeterCommand(repository);
  });

  it("create adds a territory and its first version from 2019-01-01", async () => {
    await command.call("create", ["epci:200056232"], { name: "SCoT", siret: "12345678900011", yes: true });

    const [_id, territory] = [...repository.territories].at(-1)!;
    assertEquals(territory, { name: "SCoT", company: "12345678900011", selectors: [] });
    assertEquals(repository.of(_id), [{
      version: 1,
      arr: ["91377", "91471", "91477"],
      valid_from: new Date("2018-12-31T23:00:00Z"),
      valid_to: null,
    }]);
  });

  it("create requires a name", async () => {
    await assertRejects(() => command.call("create", ["com:91471"], { yes: true }), Error, "--name");
  });

  it("create refuses a name already used, ignoring case and spaces", async () => {
    await assertRejects(
      () => command.call("create", ["com:91471"], { name: " Custom ", yes: true }),
      Error,
      `${CUSTOM}`,
    );
    assertEquals(repository.territories.size, 2);
  });

  it("create does not write in dry-run", async () => {
    await command.call("create", ["com:91471"], { name: "SCoT", dryRun: true });
    assertEquals(repository.territories.size, 2);
  });

  it("other actions require an existing territory", async () => {
    await assertRejects(() => command.call("show", [], {}), Error, "--territory");
    await assertRejects(() => command.call("show", [], { territory: 999 }), Error, "999");
  });

  it("add on a standard territory builds on its selectors", async () => {
    await command.call("add", ["com:91471"], { territory: STANDARD, yes: true, from: "2026-01-01" });

    assertEquals(repository.of(STANDARD), [{
      version: 1,
      arr: ["91377", "91471", "91477"],
      valid_from: new Date("2025-12-31T23:00:00Z"),
      valid_to: null,
    }]);
  });

  it("add and remove build on the version valid at --from", async () => {
    await command.call("add", ["epci:200056232"], { territory: CUSTOM, yes: true, from: "2026-01-01" });
    await command.call("remove", ["com:91471"], { territory: CUSTOM, yes: true, from: "2026-07-01" });
    await command.call("add", ["arr:69381"], { territory: CUSTOM, yes: true, from: "2026-03-01", to: "2026-04-01" });

    assertEquals(repository.of(CUSTOM).map((v) => [v.arr, v.valid_from, v.valid_to]), [
      [["91377", "91471", "91477"], new Date("2025-12-31T23:00:00Z"), null],
      [["91377", "91477"], new Date("2026-06-30T22:00:00Z"), null],
      [["69381", "91377", "91471", "91477"], new Date("2026-02-28T23:00:00Z"), new Date("2026-03-31T22:00:00Z")],
    ]);
  });

  it("rollback copies a previous version", async () => {
    await command.call("add", ["epci:200056232"], { territory: CUSTOM, yes: true, from: "2026-01-01" });
    await command.call("set", ["arr:69381"], { territory: CUSTOM, yes: true, from: "2026-01-01" });
    await command.call("rollback", [], { territory: CUSTOM, yes: true, from: "2026-01-01", toVersion: 1 });

    assertEquals(repository.of(CUSTOM).map((v) => v.arr), [
      ["91377", "91471", "91477"],
      ["69381"],
      ["91377", "91471", "91477"],
    ]);
  });

  it("refuses unknown codes", async () => {
    await assertRejects(
      () => command.call("add", ["epci:200056232", "aom:123456789"], { territory: CUSTOM, yes: true }),
      Error,
      "aom:123456789",
    );
    assertEquals(repository.of(CUSTOM).length, 0);
  });

  it("refuses an empty perimeter", async () => {
    await command.call("add", ["com:91471"], { territory: CUSTOM, yes: true, from: "2026-01-01" });
    await assertRejects(
      () => command.call("remove", ["com:91471"], { territory: CUSTOM, yes: true, from: "2026-01-01" }),
      Error,
      "vide",
    );
    assertEquals(repository.of(CUSTOM).length, 1);
  });

  it("refuses a change without effect", async () => {
    await command.call("add", ["com:91471"], { territory: CUSTOM, yes: true, from: "2026-01-01" });
    await assertRejects(
      () => command.call("add", ["com:91471"], { territory: CUSTOM, yes: true, from: "2026-01-01" }),
      Error,
      "Aucun changement",
    );
  });

  it("dry-run does not write", async () => {
    await command.call("add", ["com:91471"], { territory: CUSTOM, dryRun: true });
    assertEquals(repository.of(CUSTOM).length, 0);
  });

  it("remap adds successor codes from --from, keeping the base range", async () => {
    await command.call("add", ["epci:200056232"], {
      territory: CUSTOM,
      yes: true,
      from: "2026-01-01",
      to: "2027-01-01",
    });
    await command.call("remap", [], { territory: CUSTOM, yes: true, from: "2026-07-01" });

    assertEquals(repository.of(CUSTOM)[1], {
      version: 2,
      arr: ["91377", "91471", "91477", "91999"],
      valid_from: new Date("2026-06-30T22:00:00Z"),
      valid_to: new Date("2026-12-31T23:00:00Z"),
    });
  });

  it("remap --all only touches territories with a version", async () => {
    await command.call("add", ["com:91471"], { territory: CUSTOM, yes: true, from: "2026-01-01" });
    await command.call("remap", [], { all: true, yes: true, from: "2026-07-01" });

    assertEquals(repository.of(CUSTOM).map((v) => v.arr), [["91471"], ["91471", "91999"]]);
    assertEquals(repository.of(STANDARD).length, 0);
  });

  it("remap does nothing without evolution", async () => {
    await command.call("add", ["arr:69381"], { territory: CUSTOM, yes: true, from: "2026-01-01" });
    await command.call("remap", [], { territory: CUSTOM, yes: true, from: "2026-07-01" });
    assertEquals(repository.of(CUSTOM).length, 1);
  });

  it("refuses unknown action", async () => {
    await assertRejects(() => command.call("drop", [], { territory: CUSTOM }), Error, "drop");
  });
});
