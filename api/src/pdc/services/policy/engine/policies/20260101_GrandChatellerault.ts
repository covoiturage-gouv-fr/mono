import { Timezone } from "@/pdc/providers/validator/types.ts";
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
import { endsAt, startsAt } from "../helpers/position.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// Politique de Grand Chatellerault 2026
// INSERT INTO policy.policies (territory_id, start_date, end_date, name, unit, status, handler, max_amount)
// VALUES (
//   36437,
//   '2026-01-01T00:00:00+0100',
//   '2027-01-01T00:00:00+0100',
//   'Grand Châtellerault 2026',
//   'euro',
//   'draft',
//   'grand_chatellerault_2026',
//   1500000
// );
export const GrandChatellerault2026: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "grand_chatellerault_2026";
  static readonly tz: Timezone = "Europe/Paris";

  protected operators: TimestampedOperators = [
    {
      date: new Date("2026-01-01T00:00:00+0100"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
      ],
    },
  ];

  protected regularSlices: RunnableSlices = [
    {
      start: 3_000,
      end: 80_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 150),
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "25021180-e8d6-4bae-9864-f2ad12a076c4",
        6n,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "eb4f1108-010d-473e-aaca-93caf6f8d3f0",
        2n,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Passenger,
      ],
      [
        "bd2c3b5e-5916-4b35-911c-34c6258096a0",
        120_00n,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "31dbab80-13da-4fe6-9ef7-5d3b4345f188",
        this.max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected processExclusion(ctx: StatelessContextInterface) {
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));
    onDistanceRangeOrThrow(ctx, { min: 3_000, max: 80_001 });
    isOperatorClassOrThrow(ctx, ["C"]);
    isAdultOrThrow(ctx);

    // Exclusion des OD hors de la zone de CA du Grand Châtellerault (248600413)
    if (
      !startsAt(ctx, { aom: ["248600413"] }) &&
      !endsAt(ctx, { aom: ["248600413"] })
    ) {
      throw new NotEligibleTargetException();
    }

    // Exclusion des OD des autres AOM
    const aomToExclude = [
      "200069854", // CU du Grand Poitiers (200069854)
    ];

    if (
      startsAt(ctx, { aom: aomToExclude }) || endsAt(ctx, { aom: aomToExclude })
    ) {
      throw new NotEligibleTargetException();
    }
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
    super.processStateless(ctx);

    let amount = 0;
    for (const { start, fn } of this.regularSlices) {
      if (onDistanceRange(ctx, { min: start })) {
        amount += fn(ctx);
      }
    }

    ctx.incentive.set(amount);
  }

  params(): PolicyHandlerParamsInterface {
    return {
      tz: GrandChatellerault2026.tz,
      slices: this.regularSlices,
      operators: getOperatorsAt(this.operators),
      limits: {
        glob: this.max_amount,
      },
    };
  }
};
