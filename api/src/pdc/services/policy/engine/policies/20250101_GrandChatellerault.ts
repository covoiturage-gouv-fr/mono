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
import { endsAt, startsAt } from "../helpers/position.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// Politique de Grand Chatellerault 2025
// INSERT INTO policy.policies (territory_id, start_date, end_date, name, unit, status, handler, max_amount)
// VALUES (
//   36437,
//   '2025-01-01T00:00:00+0200',
//   '2025-12-31T00:00:00+0100',
//   'Grand Châtellerault 2025',
//   'euro',
//   'draft',
//   'grand_chatellerault_2025',
//   1000000
// );
/* eslint-disable-next-line */
export const GrandChatellerault2025: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "grand_chatellerault_2025";
  static readonly tz: Timezone = "Europe/Paris";

  protected operators: TimestampedOperators = [
    {
      date: new Date("2025-01-01T00:00:00+0100"),
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
      end: 80_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 150),
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "24338FBE-6E41-4C7D-B3FA-969EF0CB3789",
        this.max_amount,
        watchForGlobalMaxAmount,
      ],
      [
        "36089A82-4F5D-4AB0-809B-CDB1E41330D9",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "456D6AC1-DCE9-403A-876E-91E3B2E80A80",
        2,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Passenger,
      ],
      [
        "71390F62-7377-427B-9F26-155F2225CDEF",
        120_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
    ];
  }

  protected processExclusion(ctx: StatelessContextInterface) {
    isOperatorOrThrow(
      ctx,
      getOperatorsAt(this.operators, ctx.carpool.datetime),
    );
    onDistanceRangeOrThrow(ctx, { min: 5_000, max: 80_000 });
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
      tz: GrandChatellerault2025.tz,
      slices: this.regularSlices,
      operators: getOperatorsAt(this.operators),
      limits: {
        glob: this.max_amount,
      },
    };
  }

};
