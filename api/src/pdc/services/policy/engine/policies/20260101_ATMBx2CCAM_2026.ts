import { RunnableSlices } from "../../interfaces/engine/PolicyInterface.ts";
import {
  OperatorsEnum,
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  PolicyHandlerStaticInterface,
  StatefulContextInterface,
  StatelessContextInterface,
} from "../../interfaces/index.ts";
import { NotEligibleTargetException } from "../exceptions/NotEligibleTargetException.ts";
import { getOperatorsAt, TimestampedOperators } from "../helpers/getOperatorsAt.ts";
import { LimitTargetEnum } from "../helpers/index.ts";
import { isOperatorClassOrThrow } from "../helpers/isOperatorClassOrThrow.ts";
import { isOperatorOrThrow } from "../helpers/isOperatorOrThrow.ts";
import { watchForGlobalMaxAmount, watchPersonTripCount } from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { perKm, perSeat } from "../helpers/per.ts";
import { endsAt, startsAt } from "../helpers/position.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// INSERT INTO policy.policies ( territory_id, start_date, end_date, name, unit, status, handler, max_amount )
// VALUES (36368, '2026-01-01T00:00:00+0100', '2027-01-01T00:00:00+0100', 'ATMB×2CCAM 2026', 'euro', 'draft', 'atmb_2ccam_2026', 4200000);

// Campagne ATMBx2CCAM 2026
export const ATMBx2CCAM2026: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "atmb_2ccam_2026";

  // Configure the number of passenger trips before the incentive price is halved
  private readonly COUNTER_UUID = "c04ddaf2-0d3e-448f-a25f-13327f99199a";
  private readonly COUNTER_LIMIT = 50n;

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        this.COUNTER_UUID,
        10000n, // No limit, we just wanna count the total number of trips
        watchPersonTripCount,
        LimitTargetEnum.Passenger,
      ],
      // Plafonnement de la campagne (42 000 €)
      [
        "747a23e2-04af-445b-b5bb-3509fea033d2",
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
      start: 5_000,
      end: 40_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { limit: 40_000, amount: 10 })),
    },
  ];

  protected processExclusion(ctx: StatelessContextInterface) {
    // Opérateurs autorisés
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));

    // Distance : 5 km au minimum
    onDistanceRangeOrThrow(ctx, { min: 5_000 });

    // Zone d’intervention : origine ET destination sur le territoire des communautés de communes suivantes :
    //  - Communauté de communes Cluses Arve et Montagnes
    //  - Communauté de communes Pays du Mont Blanc
    //  - Communauté de communes Vallée de Chamonix
    //  - Communauté de communes des Montagnes du Giffre

    const selector = { aom: ["200033116", "200023372"], epci: ["200034882", "200034098"] };
    if (!(startsAt(ctx, selector) && endsAt(ctx, selector))) {
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

  // Incentive price is halved over 50 passenger trips
  override processStateful(ctx: StatefulContextInterface): void {
    super.processStateful(ctx);
    if (ctx.meta.get(this.COUNTER_UUID) > this.COUNTER_LIMIT) {
      ctx.incentive.set(ctx.incentive.get() / 2);
    }
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
    return "";
  }
};
