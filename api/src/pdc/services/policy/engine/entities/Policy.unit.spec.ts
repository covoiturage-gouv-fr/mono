import { assertRejects } from "dep:assert";
import { it } from "dep:testing-bdd";
import { PolicyStatusEnum } from "../../contracts/common/interfaces/PolicyInterface.ts";
import {
  CarpoolInterface,
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  StatefulContextInterface,
  StatelessContextInterface,
} from "../../interfaces/index.ts";
import { applyLimitOnStatefulStage, isOperatorClassOrThrow, perKm, watchForGlobalMaxAmount } from "../helpers/index.ts";
import { UnknownTerritoryException } from "../exceptions/UnknownTerritoryException.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { process } from "../tests/macro.ts";
import { Policy } from "./Policy.ts";

class TestHandler implements PolicyHandlerInterface {
  async load(): Promise<void> {
    return;
  }

  processStateless(ctx: StatelessContextInterface): void {
    isOperatorClassOrThrow(ctx, ["C"]);
    ctx.incentive.set(perKm(ctx, { amount: 10 }));
    watchForGlobalMaxAmount(ctx, "max");
  }

  processStateful(ctx: StatefulContextInterface): void {
    applyLimitOnStatefulStage(ctx, "max", 2000, watchForGlobalMaxAmount);
  }

  describe() {
    return "";
  }

  params(): PolicyHandlerParamsInterface {
    return {};
  }
}

it(
  "should work if class C",
  async () =>
    await process(
      {
        handler: new TestHandler(),
        carpool: [{ distance: 1000 }, { distance: 2000 }],
        meta: [],
      },
      {
        incentive: [10, 20],
        meta: [{
          key: "max_amount_restriction.global.campaign.global",
          value: 30,
        }],
      },
    ),
);

it(
  "should work if not class C",
  async () =>
    await process(
      {
        handler: new TestHandler(),
        carpool: [{ distance: 1000, operator_class: "B" }],
        meta: [],
      },
      { incentive: [0], meta: [] },
    ),
);

it(
  "should work with initial meta",
  async () =>
    await process(
      {
        handler: new TestHandler(),
        carpool: [{ distance: 10000 }],
        meta: [{
          key: "max_amount_restriction.global.campaign.global",
          value: 1950,
        }],
      },
      {
        incentive: [50],
        meta: [{
          key: "max_amount_restriction.global.campaign.global",
          value: 2000,
        }],
      },
    ),
);

it(
  "should work with dates",
  async () =>
    await process(
      {
        handler: new TestHandler(),
        carpool: [
          { distance: 10000, datetime: new Date("2022-01-01") },
          { distance: 10000, datetime: new Date("2022-12-01") },
        ],
        meta: [],
      },
      {
        incentive: [100, 100],
        meta: [{
          key: "max_amount_restriction.global.campaign.global",
          value: 200,
        }],
      },
    ),
);

class MaxAmountPolicyHandler implements PolicyHandlerInterface {
  max_amount: number;

  constructor(max_amount: number) {
    this.max_amount = max_amount;
  }

  async load(): Promise<void> {
    return;
  }

  processStateless(ctx: StatelessContextInterface): void {
    isOperatorClassOrThrow(ctx, ["C"]);
    ctx.incentive.set(perKm(ctx, { amount: 10 }));
    watchForGlobalMaxAmount(ctx, "max");
  }

  processStateful(ctx: StatefulContextInterface): void {
    applyLimitOnStatefulStage(
      ctx,
      "max",
      this.max_amount,
      watchForGlobalMaxAmount,
    );
  }

  describe() {
    return "";
  }

  params(): PolicyHandlerParamsInterface {
    return {};
  }
}

it(
  "should use constructor max amount",
  async () =>
    await process(
      {
        handler: new MaxAmountPolicyHandler(60_000),
        carpool: [
          { distance: 10000, datetime: new Date("2022-01-01") },
          { distance: 10000, datetime: new Date("2022-12-01") },
        ],
        meta: [],
      },
      {
        incentive: [100, 100],
        meta: [{
          key: "max_amount_restriction.global.campaign.global",
          value: 200,
        }],
      },
    ),
);

it(
  "should restrict to the perimeter version valid at the carpool date",
  async () =>
    await process(
      {
        handler: new TestHandler(),
        policy: {
          perimeters: [
            { version: 1, arr: ["69381"], valid_from: new Date("2022-01-01"), valid_to: null },
            { version: 2, arr: ["91377"], valid_from: new Date("2022-06-01"), valid_to: null },
          ],
        },
        carpool: [
          { distance: 10000, datetime: new Date("2022-02-01") },
          { distance: 10000, datetime: new Date("2022-07-01") },
        ],
        meta: [],
      },
      {
        incentive: [0, 100],
        meta: [{
          key: "max_amount_restriction.global.campaign.global",
          value: 100,
        }],
      },
    ),
);

it(
  "should match perimeter versions on start or end arr",
  async () =>
    await process(
      {
        handler: new TestHandler(),
        policy: {
          perimeters: [{ version: 1, arr: ["69381"], valid_from: new Date("2022-01-01"), valid_to: null }],
        },
        carpool: [
          { distance: 10000, datetime: new Date("2022-02-01"), end: { arr: "69381" } },
          { distance: 10000, datetime: new Date("2022-02-01"), start: { arr: "69381" } },
          { distance: 10000, datetime: new Date("2022-02-01") },
        ],
        meta: [],
      },
      {
        incentive: [100, 100, 0],
        meta: [{
          key: "max_amount_restriction.global.campaign.global",
          value: 200,
        }],
      },
    ),
);

it(
  "should fall back to territory_selector when no perimeter version covers the date",
  async () =>
    await process(
      {
        handler: new TestHandler(),
        policy: {
          perimeters: [{ version: 1, arr: ["69381"], valid_from: new Date("2022-06-01"), valid_to: null }],
        },
        carpool: [{ distance: 10000, datetime: new Date("2022-02-01") }],
        meta: [],
      },
      {
        incentive: [100],
        meta: [{
          key: "max_amount_restriction.global.campaign.global",
          value: 100,
        }],
      },
    ),
);

it(
  "should refuse carpools outside the perimeter versions of a territory without selector",
  async () =>
    await process(
      {
        handler: new TestHandler(),
        policy: {
          territory_selector: {},
          perimeters: [{ version: 1, arr: ["69381"], valid_from: new Date("2022-06-01"), valid_to: null }],
        },
        carpool: [
          { distance: 10000, datetime: new Date("2022-02-01"), start: { arr: "69381" } },
          { distance: 10000, datetime: new Date("2022-07-01"), start: { arr: "69381" } },
        ],
        meta: [],
      },
      {
        incentive: [0, 100],
        meta: [{
          key: "max_amount_restriction.global.campaign.global",
          value: 100,
        }],
      },
    ),
);

it("should refuse a carpool with an unknown territory", async () => {
  const policy = new Policy(
    1,
    1,
    { aom: ["217500016"] },
    "test",
    new Date("2019-01-01"),
    new Date("2019-12-31"),
    "Europe/Paris",
    new TestHandler(),
    PolicyStatusEnum.ACTIVE,
    0,
  );
  const carpool = generateCarpool({ end: null as unknown as CarpoolInterface["end"] });
  await assertRejects(() => policy.processStateless(carpool), UnknownTerritoryException);
});
