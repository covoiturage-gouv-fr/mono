import { Timezone } from "../../../../providers/validator/types.ts";
import { toTzString } from "../../helpers/index.ts";
import { RunnableSlices } from "../../interfaces/engine/PolicyInterface.ts";
import {
  OperatorsEnum,
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  PolicyHandlerStaticInterface,
  StatelessContextInterface,
} from "../../interfaces/index.ts";
import { NotEligibleTargetException } from "../exceptions/NotEligibleTargetException.ts";
import { getOperatorsAt, TimestampedOperators } from "../helpers/getOperatorsAt.ts";
import { isAdultOrThrow } from "../helpers/isAdultOrThrow.ts";
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
import { perKm, perSeat } from "../helpers/per.ts";
import { endsAt, startsAndEndsAt, startsAt } from "../helpers/position.ts";
import { description } from "./20240101_PaysDeLaLoire.html.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// Politique de Pays de la Loire 2024
/* eslint-disable-next-line */
export const PaysDeLaLoire2024: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "pdll_2024";
  static readonly tz: Timezone = "Europe/Paris";

  public static mode<T>(date: Date, regular: T, booster: T): T {
    if (!PaysDeLaLoire2024.boosterDates) return regular;

    const ymd = toTzString(date).slice(0, 10);
    return PaysDeLaLoire2024.boosterDates.includes(ymd) ? booster : regular;
  }

  // List dates in the YYYY-MM-DD format in the Europe/Paris timezone
  // or use the dateRange() helper function for consecutive dates.
  protected static boosterDates: string[] = [];

  protected operators: TimestampedOperators = [
    {
      date: new Date("2024-01-01T00:00:00+0100"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
        OperatorsEnum.KLAXIT,
        OperatorsEnum.MOBICOOP,
      ],
    },
    {
      date: new Date("2024-03-18T00:00:00+0100"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
        OperatorsEnum.MOBICOOP,
      ],
    },
  ];

  protected regularSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 17_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 75),
    },
    {
      start: 17_000,
      end: 30_000,
      fn: (ctx: StatelessContextInterface) => {
        // 0,10 euro par trajet par km par passager avec un maximum de 2,00 euros
        return perSeat(
          ctx,
          Math.min(perKm(ctx, { amount: 10, offset: 17_000, limit: 30_000 }), 200 - 75),
        );
      },
    },
    {
      start: 30_000,
      end: 60_000,
      fn: () => 0,
    },
  ];

  protected boosterSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 17_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 165),
    },
    {
      start: 17_000,
      end: 30_000,
      fn: (ctx: StatelessContextInterface) => {
        // 0,10 euro par trajet par km par passager avec un maximum de 2,90 euros
        return perSeat(ctx, Math.min(perKm(ctx, { amount: 10, offset: 17_000, limit: 30_000 }), 290 - 165));
      },
    },
    {
      start: 30_000,
      end: 60_000,
      fn: () => 0,
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "5499304F-2C64-AB1A-7392-52FF88F5E78D",
        this.max_amount,
        watchForGlobalMaxAmount,
      ],
      [
        "8C5251E8-AB82-EB29-C87A-2BF59D4F6328",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "ECDE3CD4-96FF-C9D2-BA88-45754205A798",
        84_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "c5ba8ecd-f1ee-4005-b2b0-fe94901d1286",
        1008_00,
        watchForPersonMaxAmountByYear,
        LimitTargetEnum.Driver,
      ],
    ];
  }

  public processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
    super.processStateless(ctx);

    if (!PaysDeLaLoire2024.mode) {
      throw new Error("PaysDeLaLoire2024.mode is not defined");
    }

    // Par kilomètre
    let amount = 0;
    const slices = PaysDeLaLoire2024.mode(ctx.carpool.datetime, this.regularSlices, this.boosterSlices);
    for (const { start, fn } of slices) {
      if (onDistanceRange(ctx, { min: start })) {
        amount += fn(ctx);
      }
    }

    ctx.incentive.set(amount);
  }

  public params(date?: Date): PolicyHandlerParamsInterface {
    if (!PaysDeLaLoire2024.mode) {
      throw new Error("PaysDeLaLoire2024.mode is not defined");
    }

    return {
      tz: PaysDeLaLoire2024.tz,
      slices: date ? PaysDeLaLoire2024.mode(date, this.regularSlices, this.boosterSlices) : this.regularSlices,
      operators: getOperatorsAt(this.operators),
      allTimeOperators: Array.from(new Set(this.operators.flatMap((entry) => entry.operators))),
      limits: {
        glob: this.max_amount,
      },
    };
  }

  public describe(): string {
    return description;
  }

  protected processExclusion(ctx: StatelessContextInterface) {
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));
    onDistanceRangeOrThrow(ctx, { min: 5_000, max: 60_001 });
    isOperatorClassOrThrow(ctx, ["C"]);
    isAdultOrThrow(ctx);

    this.excludeLocalAOM(ctx);
    this.excludeOutsideOfAOM(ctx);
  }

  /**
   * Exclure les trajets :
   *  - 244400404: Nantes Métropole -> Nantes Métropole,
   *  - 244900015: CU Angers Loire Métropole -> CU Angers Loire Métropole,
   *  - 247200132: CU Le Mans Métropole -> CU Le Mans Métropole,
   *  - 200071678: CA Agglomération du Choletais -> CA Agglomération du Choletais
   */
  protected excludeLocalAOM(ctx: StatelessContextInterface) {
    if (
      startsAndEndsAt(ctx, { aom: ["244400404"] }) ||
      startsAndEndsAt(ctx, { aom: ["244900015"] }) ||
      startsAndEndsAt(ctx, { aom: ["247200132"] }) ||
      startsAndEndsAt(ctx, { aom: ["200071678"] })
    ) {
      throw new NotEligibleTargetException();
    }
  }

  protected excludeOutsideOfAOM(ctx: StatelessContextInterface) {
    if (!startsAt(ctx, { reg: ["52"] }) || !endsAt(ctx, { reg: ["52"] })) {
      throw new NotEligibleTargetException();
    }
  }
};
