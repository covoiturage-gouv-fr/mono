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
import { perSeat } from "../helpers/per.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// Politique Grands poitiers
// territory_id: 323
// aom: 200069854
export const GrandPoitiers2027: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "grand_poitiers_2027";

  protected operator_class = ["C"];
  protected readonly operators: TimestampedOperators = [
    {
      date: new Date("2027-01-01T00:00:00+0100"),
      operators: [
        OperatorsEnum.KAROS,
        OperatorsEnum.BLABLACAR_DAILY,
      ],
    },
  ];

  constructor(public max_amount: number) {
    super();
    this.limits = [
      [
        "b3c1f0a2-6d4e-4a57-9a1c-2e7f8d5c4b10",
        120_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "c7d2e9b4-1f38-4c6a-8b05-9a4e3d6f7c21",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "d4e8a1c6-2b59-4f73-a6d0-5c1b9e8f3a32",
        2,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Passenger,
      ],
      [
        "e5f9b2d7-3c6a-4084-b7e1-6d2c0f9a4b43",
        this.max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected slices: RunnableSlices = [
    {
      start: 5_000,
      end: 80_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 150),
    },
  ];

  protected processExclusion(ctx: StatelessContextInterface) {
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));
    onDistanceRangeOrThrow(ctx, { min: 4_999, max: 80_001 });
    isOperatorClassOrThrow(ctx, this.operator_class);
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
      allTimeOperators: Array.from(new Set(this.operators.flatMap((entry) => entry.operators))),
      limits: { glob: this.max_amount },
    };
  }
};
