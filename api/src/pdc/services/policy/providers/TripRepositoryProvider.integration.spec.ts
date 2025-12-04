import { afterAll, assert, assertEquals, beforeAll, describe, it } from "../../../../dev_deps.ts";
import { LegacyDbContext, makeLegacyDbBeforeAfter } from "../../../providers/test/index.ts";
import { PolicyStatusEnum } from "../contracts/common/interfaces/PolicyInterface.ts";
import { Policy } from "../engine/entities/Policy.ts";
import { IDFMPeriodeNormale2021 } from "../engine/policies/20210520_IDFM.ts";
import { TripRepositoryProvider } from "./TripRepositoryProvider.ts";

// FIXME
// Leak on cursor
describe.skip("TripRepositoryProvider", () => {
  let db: LegacyDbContext;
  let repository: TripRepositoryProvider;
  const { before, after } = makeLegacyDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new TripRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("Should find carpools even with fraudcheck_error", async () => {
    const start_date = new Date("2024-03-01");
    const end_date = new Date("2024-03-30");

    const policy = await Policy.import({
      _id: 1,
      territory_id: 1,
      territory_selector: { aom: ["217500016"] },
      start_date,
      end_date,
      tz: "Europe/Paris",
      name: "Policy",
      handler: IDFMPeriodeNormale2021.id,
      status: PolicyStatusEnum.ACTIVE,
      incentive_sum: 5000n,
      max_amount: 10_000_000_00n,
    });

    const cursor = repository.findTripByPolicy(
      policy,
      start_date,
      end_date,
    );
    const { value } = await cursor.next(); // ok carpool
    await cursor.next(); // ok carpool
    await cursor.next(); // fraudcheck_error carpool
    assertEquals((value || []).length, 3);
    assert(Array.isArray(value));
    assertEquals(
      (value || [])
        .map((c) => ({
          start: {
            aom: c.start.aom,
            arr: c.start.arr,
            com: c.start.com,
            country: c.start.country,
            dep: c.start.dep,
            epci: c.start.epci,
            reg: c.start.reg,
            reseau: c.start.reseau,
          },
          end: {
            aom: c.end.aom,
            arr: c.end.arr,
            com: c.end.com,
            country: c.end.country,
            dep: c.end.dep,
            epci: c.end.epci,
            reg: c.end.reg,
            reseau: c.end.reseau,
          },
          operator_uuid: c.operator_uuid,
          datetime: c.datetime,
          distance: c.distance,
        }))
        .shift(),
      {
        start: {
          aom: "217500016",
          arr: "91471",
          com: "91471",
          country: "XXXXX",
          dep: "91",
          epci: "200056232",
          reg: "11",
          reseau: 232,
        },
        end: {
          aom: "217500016",
          arr: "91377",
          com: "91377",
          country: "XXXXX",
          dep: "91",
          epci: "200056232",
          reg: "11",
          reseau: 232,
        },
        operator_uuid: "25a8774f-8708-4cf7-8527-446106b64a35",
        datetime: new Date("2024-03-15"),
        distance: 10000,
      },
    );
  });
});
