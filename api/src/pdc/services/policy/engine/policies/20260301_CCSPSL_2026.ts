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
import { endsAt, startsAt } from "../helpers/position.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// INSERT INTO policy.policies ( territory_id, start_date, end_date, name, unit, status, handler, max_amount )
// VALUES ( ???, '2026-03-01T00:00:00+0100', '2027-01-01T00:00:00+0100', 'CC Saint-Pourcain Sioule Limagne 2026', 'euro', 'draft', 'ccspsl_2026', 213500 );

// Campagne CC Saint-Pourcain Sioule Limagne 2026 - epci:200071389
export const CCSPSL2026: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "ccspsl_2026";

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      // 150 EUR max par conducteur et par mois
      [
        "a1b2c3d4-1111-4aaa-bbbb-ccccddddeeee",
        150_00n,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],

      // 6 trajets passager par conducteur et par jour
      [
        "a1b2c3d4-2222-4aaa-bbbb-ccccddddeeee",
        6n,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],

      // Plafonnement de la campagne (2 135 EUR)
      [
        "a1b2c3d4-3333-4aaa-bbbb-ccccddddeeee",
        max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected operators: TimestampedOperators = [
    {
      date: new Date("2026-03-01T00:00:00+0100"),
      operators: [OperatorsEnum.BLABLACAR_DAILY],
    },
  ];

  // Tranches de calcul des incitations (exclusives) :
  // - De 2 a 15 km : 1,00 EUR par passager transporte
  // - De 15 a 30 km : 1,00 EUR par passager + 0,10 EUR par km supplementaire par passager
  // - De 30 a 60 km : 2,00 EUR par passager transporte
  // - Au-dela de 60 km : 1,50 EUR par passager transporte
  protected slices: RunnableSlices = [
    {
      start: 2_000,
      end: 15_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 100),
    },
    {
      start: 15_000,
      end: 30_000,
      fn: (ctx: StatelessContextInterface) =>
        perSeat(ctx, 100 + perKm(ctx, { amount: 10, offset: 15_000, limit: 30_000 })),
    },
    {
      start: 30_000,
      end: 60_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 200),
    },
    {
      start: 60_000,
      end: 80_001,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 150),
    },
  ];

  protected processExclusion(ctx: StatelessContextInterface) {
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));

    onDistanceRangeOrThrow(ctx, { min: 2_000, max: 80_001 });

    if (!startsAt(ctx, { epci: ["200071389"] }) && !endsAt(ctx, { epci: ["200071389"] })) {
      throw new NotEligibleTargetException();
    }

    isOperatorClassOrThrow(ctx, ["B", "C"]);
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
    super.processStateless(ctx);

    // Tranches exclusives : on prend la premiere qui correspond
    for (const { start, end, fn } of this.slices) {
      if (onDistanceRange(ctx, { min: start, max: end })) {
        ctx.incentive.set(fn(ctx));
        break;
      }
    }
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
