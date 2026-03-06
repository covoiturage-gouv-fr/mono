import { Timezone } from "../../../../providers/validator/types.ts";
import { RunnableSlices } from "../../interfaces/engine/PolicyInterface.ts";
import {
  OperatorsEnum,
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  PolicyHandlerStaticInterface,
  StatelessContextInterface,
} from "../../interfaces/index.ts";
import { getOperatorsAt, TimestampedOperators } from "../helpers/getOperatorsAt.ts";
import { isAdultOrThrow } from "../helpers/isAdultOrThrow.ts";
import { isAfter, IsAfterParams } from "../helpers/isAfter.ts";
import { isOperatorClassOrThrow } from "../helpers/isOperatorClassOrThrow.ts";
import { isOperatorOrThrow } from "../helpers/isOperatorOrThrow.ts";
import { LimitTargetEnum, watchForGlobalMaxAmount, watchForPersonMaxTripByDay } from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { perSeat } from "../helpers/per.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

/* eslint-disable-next-line */
export const SiouleLimagne: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "SiouleLimagne_2024_09";
  static readonly tz: Timezone = "Europe/Paris";

  protected operators: TimestampedOperators = [
    {
      date: new Date("2024-09-13T00:00:00+0100"),
      operators: [OperatorsEnum.BLABLACAR_DAILY],
    },
  ];

  protected regularSlices: RunnableSlices = [
    {
      start: 2_000,
      end: 80_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 200),
    },
  ];

  protected first_period: IsAfterParams = { date: new Date("2024-09-15") };
  protected second_period: IsAfterParams = { date: new Date("2024-10-15") };

  constructor(public policy_max_amount: bigint) {
    super();
    this.limits = [
      [
        "458F51C4-6DBE-4EA9-844F-BC2DCF37588B",
        this.policy_max_amount,
        watchForGlobalMaxAmount,
      ],
      [
        "6CDECFBD-9A98-4916-BDF1-B852213D2DE8",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
    ];
  }

  protected processExclusion(
    ctx: StatelessContextInterface,
  ) {
    isOperatorOrThrow(
      ctx,
      getOperatorsAt(this.operators, ctx.carpool.datetime),
    );
    onDistanceRangeOrThrow(ctx, { min: 2_000, max: 80_000 });
    isOperatorClassOrThrow(ctx, ["B", "C"]);
    isAdultOrThrow(ctx);
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
    super.processStateless(ctx);

    for (const { start, end } of this.regularSlices) {
      if (
        isAfter(ctx, this.second_period) &&
        onDistanceRange(ctx, { min: start, max: end })
      ) {
        ctx.incentive.set(perSeat(ctx, 150));
      } else if (
        isAfter(ctx, this.first_period) &&
        onDistanceRange(ctx, { min: start, max: end })
      ) {
        ctx.incentive.set(perSeat(ctx, 200));
      }
    }
  }

  params(): PolicyHandlerParamsInterface {
    return {
      tz: SiouleLimagne.tz,
      slices: this.regularSlices,
      operators: getOperatorsAt(this.operators),
      limits: {
        glob: this.policy_max_amount,
      },
    };
  }

};
