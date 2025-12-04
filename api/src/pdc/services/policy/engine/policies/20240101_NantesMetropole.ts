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
import { endsAt, startsAt } from "../helpers/position.ts";
import { description } from "./20240101_NantesMetropole.html.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// Politique de Pays de la Loire 2024
export const NantesMetropole2024: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "nantes_metropole_2024";
  static readonly tz: Timezone = "Europe/Paris";

  public static mode<T>(
    date: Date,
    isInside: boolean,
    regularInside: T,
    regularOutside: T,
    boosterInside: T,
    boosterOutside: T,
  ): T {
    if (!NantesMetropole2024.boosterDates) return isInside ? regularInside : regularOutside;

    const ymd = toTzString(date).slice(0, 10);
    if (NantesMetropole2024.boosterDates.includes(ymd)) {
      return isInside ? boosterInside : boosterOutside;
    } else {
      return isInside ? regularInside : regularOutside;
    }
  }

  // Liste de dates au format YYYY-MM-DD dans la zone Europe/Paris
  // pour lesquelles les règles de booster s'appliquent
  protected static boosterDates: string[] = [
    // ...dateRange("2024-12-01", "2024-12-31"),
  ];

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
      date: new Date("2024-06-01T00:00:00+0200"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
        OperatorsEnum.MOBICOOP,
      ],
    },
  ];

  protected regularInsideSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 17_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 75),
    },
    {
      start: 17_000,
      end: 29_500,
      fn: (ctx: StatelessContextInterface) => {
        // 0,10 euro par trajet par km par passager avec un maximum de 2,00 euros
        return perSeat(ctx, Math.min(perKm(ctx, { amount: 10, offset: 17_000, limit: 29_500 }), 200 - 75));
      },
    },
    {
      start: 29_500,
      end: 60_000,
      fn: () => 0,
    },
  ];

  protected regularOutsideSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 17_000,
      fn: () => 0,
    },
    {
      start: 17_000,
      end: 29_500,
      fn: () => 0,
    },
    {
      start: 29_500,
      end: 60_000,
      fn: () => 0,
    },
  ];

  protected boosterInsideSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 17_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 165),
    },
    {
      start: 17_000,
      end: 29_500,
      fn: (ctx: StatelessContextInterface) => {
        // 0,10 euro par trajet par km par passager avec un maximum de 2,90 euros
        return perSeat(ctx, Math.min(perKm(ctx, { amount: 10, offset: 17_000, limit: 29_500 }), 290 - 165));
      },
    },
    {
      start: 29_500,
      end: 60_000,
      fn: () => 0,
    },
  ];

  protected boosterOutsideSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 17_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 90),
    },
    {
      start: 17_000,
      end: 29_500,
      fn: () => 0,
    },
    {
      start: 29_500,
      end: 60_000,
      fn: () => 0,
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "01A68CC8-DFEA-4DA9-8957-5BB702F17D89",
        this.max_amount,
        watchForGlobalMaxAmount,
      ],
      [
        "69A0BFF8-D6F3-4DC9-8159-27CC8115AF48",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "CC5F5094-8431-4EA3-AC60-F51A75374356",
        84_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "76004F3C-6999-47E6-B273-E9799DB3399F",
        1008_00,
        watchForPersonMaxAmountByYear,
        LimitTargetEnum.Driver,
      ],
    ];
  }

  public processStateless(ctx: StatelessContextInterface): void {
    this.processExclusions(ctx);
    super.processStateless(ctx);

    if (!NantesMetropole2024.mode) {
      throw new Error("NantesMetropole2024.mode is not defined");
    }

    let amount = 0;
    const slices = NantesMetropole2024.mode<RunnableSlices>(
      ctx.carpool.datetime,
      this.isInsideNantesMetropole(ctx),
      this.regularInsideSlices,
      this.regularOutsideSlices,
      this.boosterInsideSlices,
      this.boosterOutsideSlices,
    );

    for (const { start, fn } of slices) {
      if (onDistanceRange(ctx, { min: start })) {
        amount += fn(ctx);
      }
    }

    ctx.incentive.set(amount);
  }

  public params(date?: Date): PolicyHandlerParamsInterface {
    if (!NantesMetropole2024.mode) {
      throw new Error("NantesMetropole2024.mode is not defined");
    }

    const slices = date
      ? NantesMetropole2024.mode<RunnableSlices>(
        date,
        true,
        this.regularInsideSlices,
        this.regularOutsideSlices,
        this.boosterInsideSlices,
        this.boosterOutsideSlices,
      )
      : this.regularInsideSlices;

    return {
      tz: NantesMetropole2024.tz,
      slices,
      booster_dates: NantesMetropole2024.boosterDates,
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

  protected processExclusions(ctx: StatelessContextInterface) {
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));
    onDistanceRangeOrThrow(ctx, { min: 5_000, max: 60_001 });
    isOperatorClassOrThrow(ctx, ["C"]);
    isAdultOrThrow(ctx);

    this.isInsideTheRegion(ctx);
    this.startsOrEndsInNantesMetropole(ctx);
  }

  protected startsOrEndsInNantesMetropole(ctx: StatelessContextInterface) {
    if (!startsAt(ctx, { aom: ["244400404"] }) && !endsAt(ctx, { aom: ["244400404"] })) {
      throw new NotEligibleTargetException();
    }
  }

  protected isInsideTheRegion(ctx: StatelessContextInterface) {
    if (!startsAt(ctx, { reg: ["52"] }) || !endsAt(ctx, { reg: ["52"] })) {
      throw new NotEligibleTargetException();
    }
  }

  protected isInsideNantesMetropole(ctx: StatelessContextInterface): boolean {
    return startsAt(ctx, { aom: ["244400404"] }) && endsAt(ctx, { aom: ["244400404"] });
  }
};
