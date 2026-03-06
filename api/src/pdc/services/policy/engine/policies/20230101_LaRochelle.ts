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
import {
  LimitTargetEnum,
  watchForGlobalMaxAmount,
  watchForPersonMaxAmountByMonth,
  watchForPersonMaxTripByDay,
} from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { perKm, perSeat } from "../helpers/per.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

/* eslint-disable-next-line */
export const LaRochelle20232024: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "larochelle_2023";

  protected operators: TimestampedOperators = [
    {
      date: new Date("2023-01-01T00:00:00+0100"),
      operators: [OperatorsEnum.KLAXIT],
    },
  ];
  protected slices_before_may: RunnableSlices = [
    {
      start: 2_000,
      end: 15_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 150),
    },
    {
      start: 15_000,
      end: 30_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 10, offset: 15_000, limit: 30_000 })),
    },
  ];

  protected slices: RunnableSlices = [
    {
      start: 5_000,
      end: 10_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 100),
    },
    {
      start: 10_000,
      end: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 10, offset: 10_000, limit: 20_000 })),
    },
    {
      start: 20_000,
      fn: (ctx: StatelessContextInterface) => 0,
    },
  ];

  private updated_policy_date: Date = new Date("2023-05-01");

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "99911EAF-89AB-C346-DDD5-BD2C7704F935",
        max_amount,
        watchForGlobalMaxAmount,
      ],
      [
        "70CE7566-6FD5-F850-C039-D76AF6F8CEB5",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      /** /!\ Only apply after first of may 2023 /!\ **/
      [
        "ECDE3CD4-96FF-C9D2-BA88-45754205A798",
        80_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
    ];
  }

  protected processExclusion(ctx: StatelessContextInterface) {
    isOperatorOrThrow(
      ctx,
      getOperatorsAt(this.operators, ctx.carpool.datetime),
    );

    if (this.isBeforeFirstOfMay(ctx)) {
      onDistanceRangeOrThrow(ctx, { min: 2_000 });
    } else {
      onDistanceRangeOrThrow(ctx, { min: 5_000, max: 70_000 });
    }
    isOperatorClassOrThrow(ctx, ["B", "C"]);
  }

  private isBeforeFirstOfMay(ctx: StatelessContextInterface) {
    return ctx.carpool.datetime < this.updated_policy_date;
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
    super.processStateless(ctx);

    // Calcul des incitations par tranche
    let amount = 0;
    for (
      const { start, fn } of this.isBeforeFirstOfMay(ctx) ? this.slices_before_may : this.slices
    ) {
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

};
