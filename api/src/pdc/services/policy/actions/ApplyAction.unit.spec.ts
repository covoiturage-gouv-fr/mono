import { assertEquals, assertRejects } from "dep:assert";
import { afterEach, beforeEach, describe, it } from "dep:testing-bdd";
import sinon from "dep:sinon";
import { Policy } from "../engine/entities/Policy.ts";
import { generateCarpool } from "../engine/tests/helpers.ts";
import {
  CarpoolInterface,
  IncentiveRepositoryProviderInterfaceResolver,
  PolicyRepositoryProviderInterfaceResolver,
  TerritoryRepositoryProviderInterfaceResolver,
  TripRepositoryProviderInterfaceResolver,
} from "../interfaces/index.ts";
import { ApplyAction } from "./ApplyAction.ts";

describe("ApplyAction", () => {
  const from = new Date("2026-10-01T00:00:00Z");
  const to = new Date("2026-10-08T00:00:00Z");

  let stored: string[];
  let released: boolean;
  let storeError: Error | undefined;

  function setup(carpools: CarpoolInterface[]): ApplyAction {
    const policyRepository = {
      find: async () => ({ _id: 1, territory_id: 1 }),
    } as unknown as PolicyRepositoryProviderInterfaceResolver;

    const territoryRepository = {
      findPerimeters: async () => [],
    } as unknown as TerritoryRepositoryProviderInterfaceResolver;

    const incentiveRepository = {
      createOrUpdateMany: async (data: Array<{ operator_journey_id: string }>) => {
        if (storeError) throw storeError;
        stored.push(...data.map((i) => i.operator_journey_id));
      },
    } as unknown as IncentiveRepositoryProviderInterfaceResolver;

    const tripRepository = {
      async *findTripByPolicy() {
        try {
          yield carpools;
        } finally {
          released = true;
        }
      },
    } as unknown as TripRepositoryProviderInterfaceResolver;

    return new ApplyAction(policyRepository, incentiveRepository, tripRepository, territoryRepository);
  }

  beforeEach(() => {
    stored = [];
    released = false;
    storeError = undefined;
    sinon.stub(Policy, "import").resolves({
      _id: 1,
      start_date: new Date("2026-01-01T00:00:00Z"),
      end_date: new Date("2027-01-01T00:00:00Z"),
      processStateless: async (carpool: CarpoolInterface) => {
        if (!carpool.end) throw new TypeError("Cannot read properties of null (reading 'aom')");
        return { export: () => ({ operator_journey_id: carpool.operator_journey_id }) };
      },
    } as unknown as Policy);
  });

  afterEach(() => {
    sinon.restore();
  });

  it("should skip a failing trip and store the others", async () => {
    const action = setup([
      generateCarpool({ operator_journey_id: "ok-1" }),
      generateCarpool({ operator_journey_id: "ko", end: null as unknown as CarpoolInterface["end"] }),
      generateCarpool({ operator_journey_id: "ok-2" }),
    ]);

    await action.handle({ policy_id: 1, from, to });

    assertEquals(stored, ["ok-1", "ok-2"]);
    assertEquals(released, true);
  });

  it("should release the cursor when processing fails", async () => {
    storeError = new Error("store failed");
    const action = setup([generateCarpool({ operator_journey_id: "ok-1" })]);

    await assertRejects(() => action.handle({ policy_id: 1, from, to }), Error, "store failed");

    assertEquals(released, true);
  });
});
