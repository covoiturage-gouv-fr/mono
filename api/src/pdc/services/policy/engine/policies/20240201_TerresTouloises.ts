import { Timezone } from "../../../../providers/validator/types.ts";
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
  watchForPersonMaxTripByDay,
} from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { perSeat } from "../helpers/per.ts";
import { startsAt } from "../helpers/position.ts";
import { description } from "./20240201_TerresTouloises.html.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

/* eslint-disable-next-line */
export const TerresTouloises2024: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "terres_touloises_2024";
  static readonly tz: Timezone = "Europe/Paris";

  protected operators: TimestampedOperators = [
    {
      date: new Date("2024-02-01T00:00:00+0100"),
      operators: [OperatorsEnum.MOBICOOP],
    },
  ];

  protected regularSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 10_000,
      fn: (ctx: StatelessContextInterface) => 100 + perSeat(ctx, 100),
    },
    {
      start: 10_000,
      end: 15_000,
      fn: (ctx: StatelessContextInterface) => 100 + perSeat(ctx, 150),
    },
    {
      start: 15_000,
      end: 80_000,
      fn: (ctx: StatelessContextInterface) => 100 + perSeat(ctx, 200),
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "E522801F-E750-4792-A4D5-C0A74AF0C673",
        this.max_amount,
        watchForGlobalMaxAmount,
      ],
      [
        "0DE698CB-6F5D-4F12-9EDD-0D0E0E5CC934",
        2,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "BDCEC555-E3D4-426C-A21A-2F8F65222497",
        2,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Passenger,
      ],
      [
        "EAB5EFB3-B020-4824-9607-51D58C34A3E1",
        100_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
    ];
  }

  protected processExclusion(
    ctx: StatelessContextInterface,
  ) {
    try {
      isOperatorOrThrow(
        ctx,
        getOperatorsAt(this.operators, ctx.carpool.datetime),
      );
      onDistanceRangeOrThrow(ctx, { min: 5_000, max: 80_000 });
      isOperatorClassOrThrow(ctx, ["B", "C"]);
      isAdultOrThrow(ctx);

      // Exclusion des trajets qui ne partent pas de l'AOM
      if (!startsAt(ctx, { aom: ["200070563"] })) {
        throw new NotEligibleTargetException("Not in CC Terres Touloises");
      }
    } catch (e) {
      throw e;
    }
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
    super.processStateless(ctx);

    for (const { start, end, fn } of this.regularSlices) {
      if (onDistanceRange(ctx, { min: start, max: end })) {
        ctx.incentive.set(fn(ctx));
        break;
      }
    }
  }

  params(): PolicyHandlerParamsInterface {
    return {
      tz: TerresTouloises2024.tz,
      slices: this.regularSlices,
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
