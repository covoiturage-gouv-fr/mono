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
import { perSeat } from "../helpers/per.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// Politique Grands poitiers
// territory_id: 323
// aom: 200069854
export const GrandPoitiers2026: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "grand_poitiers_2026";

  protected operator_class = ["C"];
  protected readonly operators: TimestampedOperators = [
    {
      date: new Date("2026-01-01T00:00:00+0100"),
      operators: [
        OperatorsEnum.KAROS,
        OperatorsEnum.BLABLACAR_DAILY,
      ],
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "51554928-b185-4ac4-a3b5-c567ada6c0fe",
        120_00n,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "5d165b48-92e1-4d79-acc0-315e520a0ae9",
        6n,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "8addc22a-bbfa-49c5-bed7-bf9d0b8a86b7",
        2n,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Passenger,
      ],
      [
        "34c079a2-be69-4210-8fc2-a20e761f3f12",
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

  describe(): string {
    return "";
  }
};
