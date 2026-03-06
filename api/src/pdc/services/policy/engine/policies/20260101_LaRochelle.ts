import { RunnableSlices } from "../../interfaces/engine/PolicyInterface.ts";
import {
  OperatorsEnum,
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  PolicyHandlerStaticInterface,
  StatelessContextInterface,
  TerritoryCodeEnum,
  TerritorySelectorsInterface,
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
import { startsOrEndsAtOrThrow } from "../helpers/startsOrEndsAtOrThrow.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// INSERT INTO policy.policies ( territory_id, start_date, end_date, name, unit, status, handler, max_amount )
// VALUES (178, '2026-01-01T00:00:00+0100', '2027-01-01T00:00:00+0100', 'CA La Rochelle 2026', 'euro', 'draft', 'la_rochelle_2026', 21000000);

export const LaRochelle2026: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "la_rochelle_2026";

  protected operators: TimestampedOperators = [
    {
      date: new Date("2026-01-01T00:00:00+0100"),
      operators: [OperatorsEnum.BLABLACAR_DAILY],
    },
  ];

  protected readonly territorySelector: TerritorySelectorsInterface = {
    [TerritoryCodeEnum.Mobility]: ["241700434"],
  };

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
      end: 70_000,
      fn: () => 0,
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "ba5fb7f9-ad6e-4323-89c5-07a4110790d5",
        6n,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "24c25b2a-6d62-489f-88a0-caef643b93cf",
        80_00n,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "fd992bd4-24c7-4bc1-a482-794624e36b00",
        max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected processExclusion(ctx: StatelessContextInterface) {
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));
    onDistanceRangeOrThrow(ctx, { min: 5_000, max: 70_000 });
    isOperatorClassOrThrow(ctx, ["B", "C"]);
    startsOrEndsAtOrThrow(ctx, this.territorySelector);
  }

  public override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
    super.processStateless(ctx);

    // Calcul des incitations par tranche
    let amount = 0;
    for (const { start, fn } of this.slices) {
      if (onDistanceRange(ctx, { min: start })) {
        amount += fn(ctx);
      }
    }

    ctx.incentive.set(amount);
  }

  public params(): PolicyHandlerParamsInterface {
    return {
      tz: "Europe/Paris",
      slices: this.slices,
      operators: getOperatorsAt(this.operators),
      limits: { glob: this.max_amount },
    };
  }

};
