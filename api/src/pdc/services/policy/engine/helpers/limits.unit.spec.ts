import { assertEquals, assertObjectMatch } from "dep:assert";
import { it } from "dep:testing-bdd";

import {
  CarpoolInterface,
  IncentiveStateEnum,
  IncentiveStatusEnum,
  MetadataLifetime,
  MetadataStoreInterface,
  SerializedIncentiveInterface,
  StatelessContextInterface,
} from "../../interfaces/index.ts";
import { StatefulContext, StatelessContext } from "../entities/Context.ts";
import { MetadataStore } from "../entities/MetadataStore.ts";
import { generateCarpool, generateIncentive } from "../tests/helpers.ts";
import {
  applyLimitOnStatefulStage,
  applyLimitsOnStatefulStage,
  applyLimitsOnStatelessStage,
  ConfiguredLimitInterface,
  LimitTargetEnum,
  watchForGlobalMaxAmount,
  watchForPassengerMaxByTripByDay,
  watchForPersonMaxAmountByMonth,
  watchForPersonMaxAmountByYear,
  watchForPersonMaxTripByDay,
  watchForPersonMaxTripByMonth,
} from "./limits.ts";

function setupStateless(): [StatelessContextInterface, CarpoolInterface] {
  const carpool = generateCarpool();
  return [StatelessContext.fromCarpool(1, carpool), carpool];
}

async function setupStateful(
  data: Partial<SerializedIncentiveInterface> = {},
): Promise<[StatefulContext, MetadataStoreInterface]> {
  const store = new MetadataStore();
  return [
    await StatefulContext.fromIncentive(store, generateIncentive(data)),
    store,
  ];
}

it("should watchForGlobalMaxAmount", async () => {
  const [ctx] = setupStateless();
  watchForGlobalMaxAmount(ctx, "1");
  assertEquals(ctx.meta.export(), [
    {
      uuid: "1",
      key: "max_amount_restriction.global.campaign.global",
      initialValue: undefined,
      lifetime: MetadataLifetime.Always,
    },
  ]);
});

it("should watchForPersonMaxAmountByMonth", async () => {
  const [ctx] = setupStateless();
  watchForPersonMaxAmountByMonth(ctx, "1", LimitTargetEnum.Passenger);
  assertEquals(ctx.meta.export(), [
    {
      uuid: "1",
      key: `max_amount_restriction.${LimitTargetEnum.Passenger}-${ctx.carpool.passenger_identity_key}.month.0-2019`,
      initialValue: undefined,
      lifetime: MetadataLifetime.Month,
    },
  ]);
});

it("should watchForPersonMaxAmountByYear", async () => {
  const [ctx] = setupStateless();
  watchForPersonMaxAmountByYear(ctx, "1", LimitTargetEnum.Passenger);
  assertEquals(ctx.meta.export(), [
    {
      uuid: "1",
      key: `max_amount_restriction.${LimitTargetEnum.Passenger}-${ctx.carpool.passenger_identity_key}.year.2019`,
      initialValue: undefined,
      lifetime: MetadataLifetime.Year,
    },
  ]);
});

it("should watchForPersonMaxTripByMonth", async () => {
  const [ctx] = setupStateless();
  watchForPersonMaxTripByMonth(ctx, "1", LimitTargetEnum.Passenger);
  assertEquals(ctx.meta.export(), [
    {
      uuid: "1",
      key: `max_trip_restriction.${LimitTargetEnum.Passenger}-${ctx.carpool.passenger_identity_key}.month.0-2019`,
      initialValue: undefined,
      lifetime: MetadataLifetime.Month,
    },
  ]);
});

it("should watchForPersonMaxTripByDay", async () => {
  const [ctx] = setupStateless();
  watchForPersonMaxTripByDay(ctx, "1", LimitTargetEnum.Driver);
  assertEquals(ctx.meta.export(), [
    {
      uuid: "1",
      key: `max_trip_restriction.${LimitTargetEnum.Driver}-${ctx.carpool.driver_identity_key}.day.15-0-2019`,
      initialValue: undefined,
      lifetime: MetadataLifetime.Day,
    },
  ]);
});

it("should drop incentive if max is reached", async () => {
  const [ctx] = await setupStateful({
    meta: [
      {
        uuid: "uuid",
        key: "max_amount_restriction.global.campaign.global",
      },
    ],
  });
  ctx.meta.set("uuid", 10);
  applyLimitOnStatefulStage(ctx, "uuid", 10, watchForGlobalMaxAmount);
  assertEquals(ctx.incentive.get(), 0);
  assertEquals(ctx.meta.export(), [{
    policy_id: 1,
    key: "max_amount_restriction.global.campaign.global",
    value: 10,
  }]);
});

it("should partially drop incentive if max will be reached", async () => {
  const [ctx] = await setupStateful({
    meta: [
      {
        uuid: "uuid",
        key: "max_amount_restriction.global.campaign.global",
      },
    ],
  });
  ctx.meta.set("uuid", 30);
  applyLimitOnStatefulStage(ctx, "uuid", 100, watchForGlobalMaxAmount);
  assertEquals(ctx.incentive.get(), 70);
  assertEquals(ctx.meta.export(), [{
    policy_id: 1,
    key: "max_amount_restriction.global.campaign.global",
    value: 100,
  }]);
});

it("should increase meta if incentive is not null", async () => {
  const [ctx] = await setupStateful({
    meta: [
      {
        uuid: "uuid",
        key: "max_amount_restriction.global.campaign.global",
      },
    ],
  });
  ctx.meta.set("uuid", 30);
  applyLimitOnStatefulStage(ctx, "uuid", 200, watchForGlobalMaxAmount);
  assertEquals(ctx.incentive.get(), 100);
  assertEquals(ctx.meta.export(), [{
    policy_id: 1,
    key: "max_amount_restriction.global.campaign.global",
    value: 130,
  }]);
});

it("should watch and apply", async () => {
  const limit: ConfiguredLimitInterface = [
    "uuid",
    150,
    watchForGlobalMaxAmount,
  ];
  const [ctxStateless, carpool] = setupStateless();
  applyLimitsOnStatelessStage([limit], ctxStateless);
  assertEquals(ctxStateless.meta.export(), [
    {
      uuid: "uuid",
      key: "max_amount_restriction.global.campaign.global",
      initialValue: undefined,
      lifetime: MetadataLifetime.Always,
    },
  ]);
  ctxStateless.incentive.set(100);
  assertObjectMatch(ctxStateless.incentive.export(), {
    _id: undefined,
    operator_id: ctxStateless.carpool.operator_id,
    operator_journey_id: ctxStateless.carpool.operator_journey_id,
    datetime: ctxStateless.carpool.datetime,
    meta: ctxStateless.meta.export(),
    policy_id: ctxStateless.meta.policy_id,
    state: IncentiveStateEnum.Regular,
    status: IncentiveStatusEnum.Draft,
    statefulAmount: 100,
    statelessAmount: 100,
  });

  const [ctxStateful, store] = await setupStateful({
    ...ctxStateless.incentive.export(),
    _id: 1,
  });
  assertEquals(ctxStateful.meta.export(), [
    {
      policy_id: 1,
      key: "max_amount_restriction.global.campaign.global",
      value: 0,
    },
  ]);
  applyLimitsOnStatefulStage([limit], ctxStateful);
  assertEquals(ctxStateful.incentive.get(), 100);
  assertEquals(ctxStateful.meta.export(), [
    {
      policy_id: 1,
      key: "max_amount_restriction.global.campaign.global",
      value: 100,
    },
  ]);
  await store.save(ctxStateful.meta);
  assertEquals(await store.store(MetadataLifetime.Day), [
    {
      key: "max_amount_restriction.global.campaign.global",
      datetime: carpool.datetime,
      policy_id: 1,
      value: 100,
    },
  ]);
  applyLimitsOnStatefulStage([limit], ctxStateful);
  assertEquals(ctxStateful.incentive.get(), 50);
  assertEquals(ctxStateful.meta.export(), [
    {
      policy_id: 1,
      key: "max_amount_restriction.global.campaign.global",
      value: 150,
    },
  ]);
  await store.save(ctxStateful.meta);
  assertEquals(await store.store(MetadataLifetime.Day), [
    {
      key: "max_amount_restriction.global.campaign.global",
      datetime: carpool.datetime,
      policy_id: 1,
      value: 150,
    },
  ]);
});

it("should watch and apply for custom data", async () => {
  const limit: ConfiguredLimitInterface = [
    "uuid",
    3,
    watchForPassengerMaxByTripByDay,
  ];
  const [ctxStateless, carpool] = setupStateless();
  applyLimitsOnStatelessStage([limit], ctxStateless);
  assertEquals(ctxStateless.meta.export(), [
    {
      uuid: "uuid",
      key: `max_passenger_restriction.${carpool.operator_id}.${carpool.operator_trip_id}.day.15-0-2019`,
      initialValue: undefined,
      lifetime: MetadataLifetime.Day,
      carpoolValue: 1,
    },
  ]);
  ctxStateless.incentive.set(100);
  assertObjectMatch(ctxStateless.incentive.export(), {
    _id: undefined,
    operator_id: ctxStateless.carpool.operator_id,
    operator_journey_id: ctxStateless.carpool.operator_journey_id,
    datetime: ctxStateless.carpool.datetime,
    meta: ctxStateless.meta.export(),
    policy_id: ctxStateless.meta.policy_id,
    state: IncentiveStateEnum.Regular,
    status: IncentiveStatusEnum.Draft,
    statefulAmount: 100,
    statelessAmount: 100,
  });

  const [ctxStateful, store] = await setupStateful({
    ...ctxStateless.incentive.export(),
    _id: 1,
  });
  assertEquals(ctxStateful.meta.export(), [
    {
      policy_id: 1,
      key: `max_passenger_restriction.${carpool.operator_id}.${carpool.operator_trip_id}.day.15-0-2019`,
      value: 0,
      carpoolValue: 1,
    },
  ]);

  applyLimitsOnStatefulStage([limit], ctxStateful);
  assertEquals(ctxStateful.incentive.get(), 100);
  assertEquals(ctxStateful.meta.export(), [
    {
      policy_id: 1,
      key: `max_passenger_restriction.${carpool.operator_id}.${carpool.operator_trip_id}.day.15-0-2019`,
      value: 1,
      carpoolValue: 1,
    },
  ]);
  await store.save(ctxStateful.meta);
  assertEquals(await store.store(MetadataLifetime.Day), []);

  applyLimitsOnStatefulStage([limit], ctxStateful);
  assertEquals(ctxStateful.incentive.get(), 100);
  assertEquals(ctxStateful.meta.export(), [
    {
      policy_id: 1,
      key: `max_passenger_restriction.${carpool.operator_id}.${carpool.operator_trip_id}.day.15-0-2019`,
      value: 2,
      carpoolValue: 1,
    },
  ]);
  await store.save(ctxStateful.meta);
  assertEquals(await store.store(MetadataLifetime.Day), []);
});
