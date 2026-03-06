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

// Pays Basque Adour
export const PaysBasque20232024: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "pays_basque_2023";

  protected operators: TimestampedOperators = [
    {
      date: new Date("2023-04-01T00:00:00+0200"),
      operators: [
        OperatorsEnum.KLAXIT,
        OperatorsEnum.MOBICOOP,
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
      ],
    },
    {
      date: new Date("2024-01-01T00:00:00+0100"),
      operators: [
        OperatorsEnum.KLAXIT,
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
      ],
    },
  ];

  protected slices: RunnableSlices = [
    {
      start: 5_000,
      end: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 200),
    },
    {
      start: 20_000,
      end: 30_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 10, offset: 20_000, limit: 30_000 })),
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "A34719E4-DCA0-78E6-38E4-701631B106C2",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "ECDE3CD4-96FF-C9D2-BA88-45754205A798",
        100_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "B15AD9E9-BF92-70FA-E8F1-B526D1BB6D4F",
        this.max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected processExclusion(ctx: StatelessContextInterface) {
    isOperatorOrThrow(
      ctx,
      getOperatorsAt(this.operators, ctx.carpool.datetime),
    );
    onDistanceRangeOrThrow(ctx, {
      min: 5_000,
      max: this.applyRuleChangeFor2024(ctx) ? 80_000 : 100_000,
    });
    isOperatorClassOrThrow(ctx, ["C"]);
    isAdultOrThrow(ctx);
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
    super.processStateless(ctx);

    // Par kilomètre
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


  private applyRuleChangeFor2024(ctx: StatelessContextInterface) {
    return ctx.carpool.datetime >= this.operators[1].date;
  }
};
