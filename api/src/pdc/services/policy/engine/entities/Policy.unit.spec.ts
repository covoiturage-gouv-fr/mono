import { it } from "../../../../../dev_deps.ts";
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
    applyLimitOnStatefulStage(ctx, "max", 2000n, watchForGlobalMaxAmount);
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
          value: 30n,
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
          value: 1950n,
        }],
      },
      {
        incentive: [50],
        meta: [{
          key: "max_amount_restriction.global.campaign.global",
          value: 2000n,
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
          value: 200n,
        }],
      },
    ),
);

class MaxAmountPolicyHandler implements PolicyHandlerInterface {
  max_amount: bigint;

  constructor(max_amount: bigint) {
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
        handler: new MaxAmountPolicyHandler(60_000n),
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
          value: 200n,
        }],
      },
    ),
);
