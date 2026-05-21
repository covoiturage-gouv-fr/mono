import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import { KernelInterfaceResolver } from "@/ilos/common/index.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { GeoRepositoryProvider } from "./GeoRepositoryProvider.ts";

class TestKernel extends KernelInterfaceResolver {}

describe("GeoRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: GeoRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new GeoRepositoryProvider(db.connection, new TestKernel());
  });

  afterAll(async () => {
    await after(db);
  });

  it("list with a search that matches nothing returns an empty page and total 0", async () => {
    const result = await repository.list({ search: "__no_match__" });
    assertEquals(result.data, [] as never);
    assertEquals(result.meta.pagination.total, 0);
    assertEquals(result.meta.pagination.offset, 0);
    assertEquals(result.meta.pagination.limit, 100);
  });

  it("list honours custom limit / offset and the exclude_coms branch", async () => {
    const result = await repository.list({
      search: "__no_match__",
      exclude_coms: true,
      limit: 5,
      offset: 10,
    });
    assertEquals(result.data, [] as never);
    assertEquals(result.meta.pagination.limit, 5);
    assertEquals(result.meta.pagination.offset, 10);
  });

  it("list with an INSEE allowlist filter compiles without crashing", async () => {
    const result = await repository.list({
      search: "__no_match__",
      where: { insee: ["00000", "99999"] },
    });
    assertEquals(result.data, [] as never);
    assertEquals(result.meta.pagination.total, 0);
  });

  it("findBySiren returns the empty-shape result when no row matches", async () => {
    const result = await repository.findBySiren({ siren: "000000000" });
    assertEquals(result.reg_name, null);
    assertEquals(result.aom_name, null);
    assertEquals(result.dep_name, null);
    assertEquals(result.epci_name, null);
    assertEquals(result.coms, []);
  });

  it("findBySiren takes the reg/dep branch on 2 or 3 char siren", async () => {
    const result2 = await repository.findBySiren({ siren: "00" });
    const result3 = await repository.findBySiren({ siren: "000" });
    assertEquals(result2.reg_name, null);
    assertEquals(result3.reg_name, null);
  });
});
