import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { TerritoryRepository } from "@/pdc/services/export/repositories/TerritoryRepository.ts";
import { TerritoryCodeEnum } from "@/pdc/services/policy/interfaces/index.ts";
import { TerritoryService } from "./TerritoryService.ts";
// import { DbContext, KernelContext, makeLegacyDbBeforeAfter, makeKernelBeforeAfter } from "@/pdc/providers/test/index.ts";
// import { ExportServiceProvider as ExportSP } from "@/pdc/services/export/ExportServiceProvider.ts";
// import { UserServiceProvider as UserSP } from "@/pdc/services/user/UserServiceProvider.ts";

// const { before: kernelBefore, after: kernelAfter } = makeKernelBeforeAfter(
//   UserSP,
//   ExportSP,
// );
// const { before: dbBefore, after: dbAfter } = makeLegacyDbBeforeAfter();

// describe("TerritoryService", () => {
//   // ---------------------------------------------------------------------------
//   // SETUP
//   // ---------------------------------------------------------------------------

//   let db: DbContext;
//   let kc: KernelContext;
//   let repository: TerritoryRepository;
//   let service: TerritoryService;

//   /**
//    * - boot up postgresql connection
//    * - create the kernel
//    * - stop the existing kernel connection to replace it with the test one
//    * - setup the db macro with the connection
//    */
//   beforeAll(async () => {
//     db = await dbBefore();
//     kc = await kernelBefore();
//     await kc.kernel.getContainer().get(DenoPostgresConnection).down();
//     kc.kernel
//       .getContainer()
//       .rebind(DenoPostgresConnection)
//       .toConstantValue(db.connection);

//     repository = new TerritoryRepository(db.connection);
//     service = new TerritoryService(repository);
//   });

//   afterAll(async () => {
//     await kernelAfter(kc);
//     await dbAfter(db);
//   });

//   // ---------------------------------------------------------------------------
//   // TESTS : resolve
//   // ---------------------------------------------------------------------------

//   // TODO
// });

describe("TerritoryService: geoStringToObject", () => {
  const connection = new DenoPostgresConnection();
  const repository = new TerritoryRepository(connection);
  const service = new TerritoryService(repository);

  it("should return the correct result when no params are provided", () => {
    const result = service.geoStringToObject([]);
    assertEquals(result, null);
  });

  it("should return the correct result when geo param is empty", () => {
    const result = service.geoStringToObject([]);
    assertEquals(result, null);
  });

  it("should return the correct result when code is not in the TerritoryCodeEnum list", () => {
    const result = service.geoStringToObject(["invalid_code"]);
    assertEquals(result, null);
  });

  it("should return the correct result when geo param is an AOM", () => {
    const result = service.geoStringToObject(["aom:code"]);
    assertEquals(result, { [TerritoryCodeEnum.Mobility]: ["CODE"] });
  });

  it("should return the correct result when geo param is many AOM", () => {
    const result = service.geoStringToObject(["aom:codeA", "aom:codeB"]);
    assertEquals(result, { [TerritoryCodeEnum.Mobility]: ["CODEA", "CODEB"] });
  });

  it("should return the correct result when geo param is an AOM and a COM", () => {
    const result = service.geoStringToObject(["aom:code", "com:code"]);
    assertEquals(result, {
      [TerritoryCodeEnum.Mobility]: ["CODE"],
      [TerritoryCodeEnum.City]: ["CODE"],
    });
  });
});

describe("TerritoryService: mergeSelectors", () => {
  const connection = new DenoPostgresConnection();
  const repository = new TerritoryRepository(connection);
  const service = new TerritoryService(repository);

  it("should return the correct result when no params are provided", () => {
    const result = service.mergeSelectors([]);
    assertEquals(result, {});
  });

  it("should return the correct result when only one param is provided", () => {
    const result = service.mergeSelectors([
      { [TerritoryCodeEnum.City]: ["CODE"] },
    ]);
    assertEquals(result, { [TerritoryCodeEnum.City]: ["CODE"] });
  });

  it("should return the correct result when two params are provided", () => {
    const result = service.mergeSelectors([
      { [TerritoryCodeEnum.City]: ["CODEA"] },
      { [TerritoryCodeEnum.City]: ["CODEB"] },
    ]);
    assertEquals(result, { [TerritoryCodeEnum.City]: ["CODEA", "CODEB"] });
  });

  it("should return the correct result when two params are provided with the same code", () => {
    const result = service.mergeSelectors([
      { [TerritoryCodeEnum.City]: ["CODE"] },
      { [TerritoryCodeEnum.City]: ["CODE"] },
    ]);
    assertEquals(result, { [TerritoryCodeEnum.City]: ["CODE"] });
  });

  it("should return the correct result when two params are provided with the same code and different type", () => {
    const result = service.mergeSelectors([
      { [TerritoryCodeEnum.City]: ["CODEA"] },
      { [TerritoryCodeEnum.Mobility]: ["CODEB"] },
    ]);
    assertEquals(result, {
      [TerritoryCodeEnum.City]: ["CODEA"],
      [TerritoryCodeEnum.Mobility]: ["CODEB"],
    });
  });
});
