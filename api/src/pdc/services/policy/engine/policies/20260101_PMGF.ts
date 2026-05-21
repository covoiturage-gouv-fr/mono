import { RunnableSlices } from "../../interfaces/engine/PolicyInterface.ts";
import {
  OperatorsEnum,
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  PolicyHandlerStaticInterface,
  StatelessContextInterface,
  TerritoryCodeEnum,
  TerritorySelectorsInterface,
} from "../../interfaces/index.ts";
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
import { startsAndEndsAt, startsOrEndsAt } from "../helpers/position.ts";
import { startsOrEndsAtOrThrow } from "../helpers/startsOrEndsAtOrThrow.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// INSERT INTO policy.policies (territory_id, start_date, end_date, name, unit, status, handler, max_amount)
// VALUES (
//   36102,
//   '2026-01-01T00:00:00+0100',
//   '2026-07-01T00:00:00+0200',
//   'PMGF 2026',
//   'euro',
//   'draft',
//   'pmgf_2026',
//   7030000
// );

// Éligibilité
//
//   - Opérateur : Blablacar Daily
//   - Classe de preuve : B, C
//   - Distance minimum : 5 km (les "trajets passager" < 2 km ne sont pas éligibles)
//   - Distance maximum : 80 km (les "trajets passager" > 80 km ne sont pas éligibles)
//   - Zone d'intervention : origine et/ou destination sur le territoire du Genevois
//     français (EPCI membres du PMGF) + SM4CC
//   - Exclusion des trajets :
//     - au-delà de 6 "trajets passager" pour le conducteur maximum par jour
//     - au-delà de 50 euros de gain pour le conducteur par mois
//
// Modalité de calcul de l'incitation
//
//   Trajets internes au territoire de la Collectivité :
//     - De 5 à 20 km (inclus) : 1,50 EUR par passager transporté
//     - De 21 à 30 km (inclus) : 1,50 EUR + 0,05 EUR par km supplémentaire par passager
//     - De 30 à 40 km (inclus) : 2,00 EUR par passager
//     - De 41 à 50 km (inclus) : 2,00 EUR - 0,10 EUR par km supplémentaire par passager
//     - Au-delà de 50 km : plafonné à 1,00 EUR par passager transporté
//
//   Trajets avec uniquement un point de départ OU d'arrivée sur le territoire
//   ET un point de départ OU d'arrivée en France :
//     - De 5 à 20 km (inclus) : 0,50 EUR par passager transporté
//     - Au-delà de 20 km : plafonné à 1,00 EUR par passager transporté
//
//   Trajets avec uniquement un point de départ OU d'arrivée sur le territoire
//   ET un point de départ OU d'arrivée en Suisse :
//     - De 5 à 80 km (inclus) : 0,50 EUR par passager transporté

export const PMGF2026: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "pmgf_2026";

  protected operator_class = ["B", "C"];

  protected readonly operators: TimestampedOperators = [
    {
      date: new Date("2026-01-01T00:00:00+0100"),
      operators: [OperatorsEnum.BLABLACAR_DAILY],
    },
  ];

  constructor(public max_amount: number) {
    super();
    this.limits = [
      // Au-delà de 6 "trajets passager" pour le conducteur maximum par jour
      [
        "5d65be20-2e40-4e83-9cf0-3a5cf06cf738",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      // Au-delà de 50 euros de gain pour le conducteur par mois
      [
        "0401e10c-5800-457e-955d-00a5ee3cf551",
        50_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      // Budget global de la campagne
      [
        "9bfdcdb9-ce61-4f4c-b3ba-f92cb4e31591",
        this.max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  /**
   * Trajets internes (origine ET destination sur le territoire)
   * - 1,50 EUR par passager de 5 à 20 km
   * - 1,50 EUR + 0,05 EUR/km de 20 à 30 km
   * - 2,00 EUR par passager de 30 à 40 km
   * - 2,00 EUR - 0,10 EUR/km de 40 à 50 km
   * - 1,00 EUR par passager au-delà de 50 km
   */
  protected intraAOMSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 1_50),
    },
    {
      start: 20_000,
      end: 30_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 5, offset: 20_000, limit: 30_000 })),
    },
    {
      start: 30_000,
      end: 40_000,
      fn: () => 0,
    },
    {
      start: 40_000,
      end: 50_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: -10, offset: 40_000, limit: 50_000 })),
    },
    {
      start: 50_000,
      end: 80_000,
      fn: () => 0,
    },
  ];

  /**
   * Trajets externes France (origine OU destination sur le territoire,
   * l'autre extrémité en France)
   * - 0,50 EUR par passager de 5 à 20 km
   * - 1,00 EUR par passager au-delà de 20 km
   */
  protected extraAOMSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 50),
    },
    {
      start: 20_000,
      end: 80_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 50),
    },
  ];

  /**
   * Trajets transfrontaliers Suisse (origine OU destination sur le territoire,
   * l'autre extrémité en Suisse)
   * - 0,50 EUR par passager de 5 à 80 km
   */
  protected switzerlandSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 80_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 50),
    },
  ];

  // PMGF + SM4CC territory
  protected territorySelector: TerritorySelectorsInterface = {
    [TerritoryCodeEnum.Mobility]: [
      "240100750", // CA du Pays de GEX
      "247400690", // CC du Genevois
      "200011773", // CC Annemasse-Les Voirons-Agglomération
      "200067551", // CA Thonon Agglomération
    ],
    [TerritoryCodeEnum.CityGroup]: [
      "240100891", // CA du Pays Bellegardien
      "247400724", // CC du Pays Rochois
      "200000172", // CC Faucigny-Glières
      "247400583", // CC Arve et Salève
      "200069730", // CC des 4 Rivières (SM4CC)
    ],
  };

  // Suisse (code COG INSEE)
  protected switzerlandSelector: TerritorySelectorsInterface = {
    [TerritoryCodeEnum.Country]: ["99140"],
  };

  protected getSlices(ctx?: StatelessContextInterface): RunnableSlices {
    if (!ctx) return this.intraAOMSlices;

    if (startsAndEndsAt(ctx, this.territorySelector)) {
      return this.intraAOMSlices;
    }

    if (startsOrEndsAt(ctx, this.switzerlandSelector)) {
      return this.switzerlandSlices;
    }

    return this.extraAOMSlices;
  }

  protected processExclusions(ctx: StatelessContextInterface) {
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));
    isOperatorClassOrThrow(ctx, this.operator_class);
    startsOrEndsAtOrThrow(ctx, this.territorySelector);
    onDistanceRangeOrThrow(ctx, { min: 4_999, max: 80_001 });
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusions(ctx);
    super.processStateless(ctx);

    let amount = 0;
    for (const { start, fn } of this.getSlices(ctx)) {
      if (onDistanceRange(ctx, { min: start })) {
        amount += fn(ctx);
      }
    }

    ctx.incentive.set(amount);
  }

  params(): PolicyHandlerParamsInterface {
    return {
      tz: "Europe/Paris",
      slices: this.getSlices(),
      operators: getOperatorsAt(this.operators),
      limits: {
        glob: this.max_amount,
      },
    };
  }
};
