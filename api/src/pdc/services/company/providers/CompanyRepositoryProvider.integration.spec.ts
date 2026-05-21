import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";
import { CompanyInterface } from "@/shared/common/interfaces/CompanyInterface2.ts";

import { CompanyRepositoryProvider } from "./CompanyRepositoryProvider.ts";

const sample = (overrides: Partial<CompanyInterface> = {}): CompanyInterface => ({
  siret: "99999999999999",
  siren: "999999999",
  nic: "99999",
  legal_name: "ACME GOV",
  company_naf_code: "8411Z",
  establishment_naf_code: "8411Z",
  legal_nature_code: "7120",
  legal_nature_label: "ADMINISTRATION",
  intra_vat: "FR99999999999",
  headquarter: true,
  nonprofit_code: null,
  address: "1 rue des Lilas 75001 PARIS",
  address_street: "rue des Lilas",
  address_postcode: "75001",
  address_cedex: null,
  address_city: "PARIS",
  ...overrides,
});

describe("CompanyRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: CompanyRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new CompanyRepositoryProvider(db.connection);
    // Seed inserts company rows with explicit _id but does not bump the
    // company.companies__id_seq sequence, so the first sequence-driven
    // INSERT collides on the primary key. Align the sequence with the
    // current max before exercising updateOrCreate.
    await db.connection.query(sql`
      SELECT setval(
        'company.companies__id_seq',
        GREATEST(1000, COALESCE((SELECT MAX(_id) FROM company.companies), 0))
      )
    `);
  });

  afterAll(async () => {
    await after(db);
  });

  it("findBySiret returns undefined when no company matches", async () => {
    const result = await repository.findBySiret("00000000000000");
    assertEquals(result, undefined);
  });

  it("findById returns undefined when no company matches", async () => {
    const result = await repository.findById(987654321);
    assertEquals(result, undefined);
  });

  it("updateOrCreate inserts a new company and findBySiret round-trips every column", async () => {
    const data = sample({ siret: "11111111111111", siren: "111111111" });
    await repository.updateOrCreate(data);

    const found = await repository.findBySiret("11111111111111");
    assertEquals(found.siret, data.siret);
    assertEquals(found.siren, data.siren);
    assertEquals(found.nic, data.nic);
    assertEquals(found.legal_name, data.legal_name);
    assertEquals(found.company_naf_code, data.company_naf_code);
    assertEquals(found.establishment_naf_code, data.establishment_naf_code);
    assertEquals(found.legal_nature_code, data.legal_nature_code);
    assertEquals(found.legal_nature_label, data.legal_nature_label);
    assertEquals(found.intra_vat, data.intra_vat);
    assertEquals(found.headquarter, data.headquarter);
    assertEquals(found.nonprofit_code, data.nonprofit_code);
    assertEquals(found.address, data.address);
    assertEquals(found.address_street, data.address_street);
    assertEquals(found.address_postcode, data.address_postcode);
    assertEquals(found.address_cedex, data.address_cedex);
    assertEquals(found.address_city, data.address_city);
    assertEquals(typeof found._id, "number");
  });

  it("updateOrCreate upserts the same siret in place (no unique-key crash)", async () => {
    const siret = "22222222222222";
    await repository.updateOrCreate(sample({ siret, siren: "222222222", legal_name: "ORIGINAL" }));
    const before = await repository.findBySiret(siret);

    await repository.updateOrCreate(sample({ siret, siren: "222222222", legal_name: "RENAMED" }));
    const after = await repository.findBySiret(siret);

    assertEquals(after._id, before._id);
    assertEquals(after.legal_name, "RENAMED");
  });

  it("updateOrCreate stores POINT(lon lat) and findBySiret reads lon/lat back via ST_X/ST_Y", async () => {
    const data = sample({
      siret: "33333333333333",
      siren: "333333333",
      lon: 2.320884,
      lat: 48.854634,
    });
    await repository.updateOrCreate(data);

    const found = await repository.findBySiret("33333333333333");
    assertEquals(typeof found.lon, "number");
    assertEquals(typeof found.lat, "number");
    if (typeof found.lon !== "number" || typeof found.lat !== "number") return;
    // ST_Point round-trip is lossy past 6 decimals; pin to ~1m precision (5dp).
    assertEquals(Math.round(found.lon * 1e5) / 1e5, 2.32088);
    assertEquals(Math.round(found.lat * 1e5) / 1e5, 48.85463);
  });

  it("updateOrCreate stores geo as NULL when lon is undefined", async () => {
    const data = sample({ siret: "44444444444444", siren: "444444444" });
    delete data.lon;
    delete data.lat;
    await repository.updateOrCreate(data);

    const found = await repository.findBySiret("44444444444444");
    assertEquals(found.lon, null);
    assertEquals(found.lat, null);
  });

  it("findById returns the row inserted by updateOrCreate", async () => {
    const siret = "55555555555555";
    await repository.updateOrCreate(sample({ siret, siren: "555555555" }));
    const bySiret = await repository.findBySiret(siret);

    const byId = await repository.findById(bySiret._id);
    assertEquals(byId.siret, siret);
    assertEquals(byId._id, bySiret._id);
  });

  it("updateOrCreate followed by an upsert with a different siret does not collide", async () => {
    await repository.updateOrCreate(sample({ siret: "66666666666666", siren: "666666666" }));
    await repository.updateOrCreate(sample({ siret: "77777777777777", siren: "777777777" }));

    const a = await repository.findBySiret("66666666666666");
    const b = await repository.findBySiret("77777777777777");
    assertEquals(a.siret, "66666666666666");
    assertEquals(b.siret, "77777777777777");
    assertEquals(a._id !== b._id, true);
  });

});
