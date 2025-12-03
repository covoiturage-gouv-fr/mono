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
import { description } from "./20250101_GrandPoitiers2025.html.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// Politique Grands poitiers
// territory_id: 323
// aom: 200069854
export const GrandPoitiers2025: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "grand_poitiers_2025";

  protected operator_class = ["C"];
  protected readonly operators: TimestampedOperators = [
    {
      date: new Date("2025-01-01T00:00:00+0100"),
      operators: [
        OperatorsEnum.KAROS,
        OperatorsEnum.MOBICOOP,
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.COCCINELLE,
      ],
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "a86039c8-c1d1-4021-ab8b-e61755a56816",
        120_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "fc103430-8a8b-4ad5-97ef-dc1f1ad8e8b2",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "e1f8dca2-d056-46cb-a86a-a66a4c3f3ba1",
        2,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Passenger,
      ],
      [
        "769001ad-b165-4328-b110-3e11b0a69307",
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
    onDistanceRangeOrThrow(ctx, { min: 4_999, max: 80_000 });
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
      allTimeOperators: Array.from(
        new Set(this.operators.flatMap((entry) => entry.operators)),
      ),
      limits: {
        glob: this.max_amount,
      },
    };
  }

  describe(): string {
    return description;
  }
};
