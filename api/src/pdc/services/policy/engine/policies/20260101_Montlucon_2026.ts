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
  watchForMaxPassengersPerDriverPerDay,
  watchForMaxPassengersPerTrip,
  watchForPersonMaxAmountByMonth,
  watchForPersonMaxTripByDay,
} from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { perKm, perSeat } from "../helpers/per.ts";
import { endsAt, startsAt } from "../helpers/position.ts";
import { description } from "./20260101_Montlucon_2026.html.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// INSERT INTO policy.policies ( territory_id, start_date, end_date, name, unit, status, handler, max_amount )
// VALUES (36334, '2026-01-01T00:00:00+0100', '2026-04-01T00:00:00+0200', 'Montluçon Communauté 2026', 'euro', 'draft', 'montlucon_2026', 2634400);

// Campagne Montluçon Communauté 2026 - epci:200071082
export const MontluconCommunaute2026: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "montlucon_2026";

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      // 150€ max par conducteur et par mois
      [
        "dcee0b35-222c-46f1-aa49-cb76725aabeb",
        150_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],

      // 6 trajets par conducteur et par jour
      [
        "e1771f85-8ec1-4d60-bf42-a3515e2ad9dd",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],

      // Plafonnement de la campagne (26 344 €)
      [
        "c3cdf86c-2405-42a8-8864-15155b85dabe",
        max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected operators: TimestampedOperators = [
    {
      date: new Date("2026-01-01T00:00:00+0100"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
      ],
    },
  ];

  // Tranches de calcul des incitations :
  // - De 2 à 15 km : 1.50€ par passager transporté
  // - De 15 à 40 km : 1.50€  par passager + 0,10 € par km supplémentaire par passager
  // - De 40 km à 60 km : 4€ par passager transporté
  // - Au-delà de 60 km : 4€ par passager - 0,20 € par km supplémentaire par passager
  protected slices: RunnableSlices = [
    {
      start: 2_000,
      end: 15_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 150),
    },
    {
      start: 15_000,
      end: 40_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 10, offset: 15_000, limit: 40_000 })),
    },
    {
      start: 40_000,
      end: 60_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 0, offset: 40_000, limit: 60_000 })),
    },
    {
      start: 60_000,
      end: 80_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: -20, offset: 60_000, limit: 80_000 })),
    },
  ];

  protected processExclusion(ctx: StatelessContextInterface) {
    // Opérateurs autorisés
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));

    // De plus de 2 kilomètres, dont l’origine OU la destination sont sur le territoire de Montluçon Communauté
    onDistanceRangeOrThrow(ctx, { min: 2_000 });

    // Exclure les trajets qui ne sont pas du tout dans l'AOM
    if (!startsAt(ctx, { aom: ["200071082"] }) && !endsAt(ctx, { aom: ["200071082"] })) {
      throw new NotEligibleTargetException();
    }

    // Classes d'opérateurs éligibles
    isOperatorClassOrThrow(ctx, ["B", "C"]);
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
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
      allTimeOperators: Array.from(new Set(this.operators.flatMap((entry) => entry.operators))),
      limits: { glob: this.max_amount },
      extras: {},
    };
  }

  describe(): string {
    return description;
  }
};
