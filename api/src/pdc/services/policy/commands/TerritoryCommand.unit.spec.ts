import { assertEquals, assertRejects } from "dep:assert";
import { beforeEach, describe, it } from "dep:testing-bdd";
import {
  PolicyRepositoryProviderInterfaceResolver,
  PolicyTerritoryInterface,
  PolicyTerritoryRepositoryProviderInterfaceResolver,
  SerializedPolicyInterface,
  TerritoryCode,
} from "../interfaces/index.ts";
import { TerritoryCommand } from "./TerritoryCommand.ts";

const RESOLVED: Record<string, string[]> = {
  "epci:200056232": ["91377", "91471", "91477"],
  "com:91471": ["91471"],
  "arr:69381": ["69381"],
};

class FakeTerritoryRepository extends PolicyTerritoryRepositoryProviderInterfaceResolver {
  versions: PolicyTerritoryInterface[] = [];

  async findByPolicy(): Promise<PolicyTerritoryInterface[]> {
    return this.versions;
  }

  async create(_policy_id: number, data: Omit<PolicyTerritoryInterface, "version">) {
    const v = { ...data, version: this.versions.length + 1 };
    this.versions.push(v);
    return v;
  }

  async resolve(codes: TerritoryCode[]) {
    const keys = codes.map((c) => `${c.type}:${c.code}`);
    return {
      arr: [...new Set(keys.flatMap((k) => RESOLVED[k] ?? []))].sort(),
      unknown: codes.filter((_, i) => !RESOLVED[keys[i]]),
    };
  }

  async describe(arr: string[]) {
    return arr.map((a) => ({ arr: a, label: `label ${a}`, pop: 1 }));
  }
}

describe("TerritoryCommand", () => {
  const start_date = new Date("2026-01-01T00:00:00+0100");
  let territories: FakeTerritoryRepository;
  let command: TerritoryCommand;

  beforeEach(() => {
    territories = new FakeTerritoryRepository();
    const policies = {
      find: async () => ({ _id: 42, start_date, tz: "Europe/Paris" }) as SerializedPolicyInterface,
    } as unknown as PolicyRepositoryProviderInterfaceResolver;
    command = new TerritoryCommand(policies, territories);
  });

  it("add creates a first version starting at the campaign start date", async () => {
    await command.call("add", ["epci:200056232"], { campaign: 42, yes: true });

    assertEquals(territories.versions, [{
      version: 1,
      arr: ["91377", "91471", "91477"],
      valid_from: start_date,
      valid_to: null,
    }]);
  });

  it("add and remove build on the version valid at --from", async () => {
    await command.call("add", ["epci:200056232"], { campaign: 42, yes: true });
    await command.call("remove", ["com:91471"], { campaign: 42, yes: true, from: "2026-07-01" });
    await command.call("add", ["arr:69381"], { campaign: 42, yes: true, from: "2026-03-01", to: "2026-04-01" });

    assertEquals(territories.versions.map((v) => [v.arr, v.valid_from, v.valid_to]), [
      [["91377", "91471", "91477"], start_date, null],
      [["91377", "91477"], new Date("2026-06-30T22:00:00Z"), null],
      [["69381", "91377", "91471", "91477"], new Date("2026-02-28T23:00:00Z"), new Date("2026-03-31T22:00:00Z")],
    ]);
  });

  it("rollback copies a previous version", async () => {
    await command.call("add", ["epci:200056232"], { campaign: 42, yes: true });
    await command.call("set", ["arr:69381"], { campaign: 42, yes: true });
    await command.call("rollback", [], { campaign: 42, yes: true, toVersion: 1 });

    assertEquals(territories.versions.map((v) => v.arr), [
      ["91377", "91471", "91477"],
      ["69381"],
      ["91377", "91471", "91477"],
    ]);
  });

  it("refuses unknown codes", async () => {
    await assertRejects(
      () => command.call("add", ["epci:200056232", "aom:123456789"], { campaign: 42, yes: true }),
      Error,
      "aom:123456789",
    );
    assertEquals(territories.versions.length, 0);
  });

  it("refuses an empty perimeter", async () => {
    await command.call("add", ["com:91471"], { campaign: 42, yes: true });
    await assertRejects(() => command.call("remove", ["com:91471"], { campaign: 42, yes: true }), Error, "vide");
    assertEquals(territories.versions.length, 1);
  });

  it("refuses a change without effect", async () => {
    await command.call("add", ["com:91471"], { campaign: 42, yes: true });
    await assertRejects(
      () => command.call("add", ["com:91471"], { campaign: 42, yes: true }),
      Error,
      "Aucun changement",
    );
  });

  it("dry-run does not write", async () => {
    await command.call("add", ["com:91471"], { campaign: 42, dryRun: true });
    assertEquals(territories.versions.length, 0);
  });

  it("refuses unknown action", async () => {
    await assertRejects(() => command.call("drop", [], { campaign: 42 }), Error, "drop");
  });
});
