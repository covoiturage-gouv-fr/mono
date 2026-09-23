import { it } from "dep:testing-bdd";
import {
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  StatefulContextInterface,
  StatelessContextInterface,
} from "../../interfaces/index.ts";
import { applyLimitOnStatefulStage, isOperatorClassOrThrow, perKm, watchForGlobalMaxAmount } from "../helpers/index.ts";
import { process } from "../tests/macro.ts";

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
  "should restrict to the policy_territories version valid at the carpool date",
  async () =>
    await process(
      {
        handler: new TestHandler(),
        policy: {
          territories: [
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
  "should match policy_territories on start or end arr",
  async () =>
    await process(
      {
        handler: new TestHandler(),
        policy: {
          territories: [{ version: 1, arr: ["69381"], valid_from: new Date("2022-01-01"), valid_to: null }],
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
  "should fall back to territory_selector when no policy_territories version covers the date",
  async () =>
    await process(
      {
        handler: new TestHandler(),
        policy: {
          territories: [{ version: 1, arr: ["69381"], valid_from: new Date("2022-06-01"), valid_to: null }],
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
