import { RunnableSlices } from "../../interfaces/engine/PolicyInterface.ts";
import {
  OperatorsEnum,
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  PolicyHandlerStaticInterface,
  StatelessContextInterface,
} from "../../interfaces/index.ts";
import { getOperatorsAt, TimestampedOperators } from "../helpers/getOperatorsAt.ts";
import { isOperatorClassOrThrow } from "../helpers/isOperatorClassOrThrow.ts";
import { isOperatorOrThrow } from "../helpers/isOperatorOrThrow.ts";
import { LimitTargetEnum, watchForGlobalMaxAmount, watchForPersonMaxTripByDay } from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { perKm, perSeat } from "../helpers/per.ts";
import { startsOrEndsAtOrThrow } from "../helpers/startsOrEndsAtOrThrow.ts";
import { description } from "./20240108_PetrLunevillois.html.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

/* eslint-disable-next-line */
export const PetrLunevillois092024032025: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "petr_lunevillois_092024_032025";

  protected operators: TimestampedOperators = [
    {
      date: new Date("2024-09-02T00:00:00+0200"),
      operators: [OperatorsEnum.MOBICOOP],
    },
  ];

  // 7 cts per km per passenger
  protected slices: RunnableSlices = [
    {
      start: 2_000,
      end: 60_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 7, offset: 2_000, limit: 60_000 })),
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "d5895fd5-05db-4680-85e1-ef0d95e01441",
        max_amount,
        watchForGlobalMaxAmount,
      ],
      [
        "95ae2e57-6e3d-4de3-aef2-96610be9e1da",
        2,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
    ];
  }

  protected processExclusion(ctx: StatelessContextInterface) {
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));
    onDistanceRangeOrThrow(ctx, { min: 2_000, max: 60_000 });
    isOperatorClassOrThrow(ctx, ["C"]);
    startsOrEndsAtOrThrow(ctx, { aom: ["200051134"] });
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
    super.processStateless(ctx);

    let amount = 0;
    for (const { start, fn } of this.slices) {
      if (onDistanceRange(ctx, { min: start })) {
        amount += fn(ctx);
      }
    }

    ctx.incentive.set(amount);
  }

  params(): PolicyHandlerParamsInterface {
    return {
      tz: "Europe/Paris",
      slices: this.slices,
      operators: getOperatorsAt(this.operators),
      limits: {
        glob: this.max_amount,
      },
    };
  }

  describe(): string {
    return description;
  }
};
