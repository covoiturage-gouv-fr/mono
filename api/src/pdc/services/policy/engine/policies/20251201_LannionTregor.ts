import { RunnableSlices } from "../../interfaces/engine/PolicyInterface.ts";
import {
  OperatorsEnum,
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  StatelessContextInterface,
  TerritoryCodeEnum,
  TerritorySelectorsInterface,
} from "../../interfaces/index.ts";
import { getOperatorsAt, TimestampedOperators } from "../helpers/getOperatorsAt.ts";
import { startsOrEndsAtOrThrow } from "../helpers/index.ts";
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
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// INSERT INTO policy.policies (territory_id, start_date, end_date, name, unit, status, handler, max_amount)
// VALUES (
//   321,
//   '2025-12-01T00:00:00+0100',
//   '2027-01-01T00:00:00+0100',
//   'Lannion-Trégor 2026',
//   'euro',
//   'draft',
//   'lannion_tregor_2026',
//   6116300
// );

export const LannionTregor2026 = class extends AbstractPolicyHandler implements PolicyHandlerInterface {
  static readonly id = "lannion_tregor_2026";

  protected operator_class = ["B", "C"];

  protected readonly operators: TimestampedOperators = [
    {
      date: new Date("2025-12-01T00:00:00+0100"),
      operators: [OperatorsEnum.BLABLACAR_DAILY],
    },
  ];

  protected readonly territorySelector: TerritorySelectorsInterface = {
    [TerritoryCodeEnum.Mobility]: ["200065928"], // CA Lannion-Trégor Communauté
  };

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "001BB5F8-207D-48A8-966E-29B6CDDAE6D7A",
        6n,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "2EE2AD47-CF32-459F-84B4-58712739ADD9",
        150_00n,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "CC9368AF-E391-41A9-B16D-B3C54DD66BA0",
        this.max_amount,
        watchForGlobalMaxAmount,
      ], // required
    ];
  }

  // Tranches de calcul des incitations :
  // - De 2 à 15 km : 1.50€ par passager transporté
  // - De 15 à 40 km : 1.50€  par passager + 0,10 € par km supplémentaire par passager
  // - De 40 km à 60 km : 4€ par passager transporté
  // - Au-delà de 60 km : 4€ par passager - 0,20 € par km supplémentaire par passager
  protected slices: RunnableSlices = [
    {
      start: 2_000,
      end: 15_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 100),
    },
    {
      start: 15_000,
      end: 30_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 10, offset: 15_000, limit: 30_000 })),
    },
    {
      start: 30_000,
      end: 60_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 0, offset: 30_000, limit: 60_000 })),
    },
    {
      start: 60_000,
      end: 80_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: -20, offset: 60_000, limit: 80_000 })),
    },
  ];

  protected processExclusions(ctx: StatelessContextInterface) {
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));
    isOperatorClassOrThrow(ctx, this.operator_class);
    startsOrEndsAtOrThrow(ctx, this.territorySelector);
    onDistanceRangeOrThrow(ctx, { min: 2_000, max: 80_000 });
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusions(ctx);
    super.processStateless(ctx);

    // Calcul de l'incitation par tranches (additif)
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
      limits: { glob: this.max_amount },
    };
  }
};
