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
  watchForPersonMaxAmountByYear,
  watchForPersonMaxTripByDay,
} from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { perSeat } from "../helpers/per.ts";
import { description } from "./20240101_SMTCDriver.html.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// Politique Syndicat Mixte des Transports en Commun de l’Agglomération Clermontoise (SMTC)
// aom = 256300120
export const SMTC2024Driver: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "smtc_2024_driver";
  protected operators: TimestampedOperators = [
    {
      date: new Date("2024-01-01T00:00:00+0100"),
      operators: [OperatorsEnum.MOV_ICI],
    },
  ];
  protected operator_class = ["B", "C"];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "AFE1C47D-BF05-4FA9-9133-853D29797D09",
        90_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "AFE1C47D-BF05-4FA9-9133-853D2987GF56",
        540_00,
        watchForPersonMaxAmountByYear,
        LimitTargetEnum.Driver,
      ],
      [
        "AFE1C47D-BF05-4FA9-9133-853D297AZEPD",
        4,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "98B26189-C6FC-4DB1-AC1C-41F779C5B3C7",
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
    isOperatorOrThrow(
      ctx,
      getOperatorsAt(this.operators, ctx.carpool.datetime),
    );
    onDistanceRangeOrThrow(ctx, { min: 5_000, max: 80_000 });
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
      limits: {
        glob: this.max_amount,
      },
    };
  }

  describe(): string {
    return description;
  }
};
