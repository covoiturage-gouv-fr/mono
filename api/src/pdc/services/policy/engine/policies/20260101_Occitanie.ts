import { set } from "@/lib/object/index.ts";
import { RunnableSlices } from "../../interfaces/engine/PolicyInterface.ts";
import {
  OperatorsEnum,
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  StatelessContextInterface,
  TerritoryCodeEnum,
} from "../../interfaces/index.ts";
import { NotEligibleTargetException } from "../exceptions/NotEligibleTargetException.ts";
import { getOperatorsAt, TimestampedOperators } from "../helpers/getOperatorsAt.ts";
import { isOperatorClassOrThrow } from "../helpers/isOperatorClassOrThrow.ts";
import { isOperatorOrThrow } from "../helpers/isOperatorOrThrow.ts";
import { LimitTargetEnum, watchForGlobalMaxAmount, watchForPersonMaxTripByDay } from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { perKm, perSeat } from "../helpers/per.ts";
import { startsAndEndsAt, startsOrEndsAt } from "../helpers/position.ts";
import { startsAndEndsAtOrThrow } from "../helpers/startsAndEndsAtOrThrow.ts";
import { occitanie2026Data } from "./20260101_Occitanie.data.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

function getContribution(ctx: StatelessContextInterface): number {
  return ctx.carpool.passenger_contribution || 0;
}

function startsOrEndsWithinArea(ctx: StatelessContextInterface) {
  const { areas } = occitanie2026Data;
  return startsOrEndsAt(ctx, { [TerritoryCodeEnum.Arr]: areas });
}

function filterInnerAOMOrThrow(ctx: StatelessContextInterface): void {
  const { inner } = occitanie2026Data;

  // check if start and end are within inner AOM
  const isInner = startsAndEndsAt(ctx, { [TerritoryCodeEnum.Mobility]: inner });
  if (!isInner) return;

  // throw if start and end are within the same AOM
  if (ctx.carpool.start[TerritoryCodeEnum.Mobility] === ctx.carpool.end[TerritoryCodeEnum.Mobility]) {
    throw new NotEligibleTargetException("Trips within the same inner AOM are rejected");
  }
}

// Éligibilité
//
//   ● Opérateurs : BlaBlaCar Daily, Karos, Mobicoop
//   ● Classe de preuve : B ou C
//   ● Distance : au minimum 2.000 et au maximum 30.000 inclus sauf sur les bassins de mobilité
//     de Mende, d’Armagnac, de Rodez, d’Alès (cf. Annexe 1) et dans ce cas les trajets éligibles
//     sont au minimum 2.000 et au maximum 50.000 inclus dont l’origine et/ou la destination est dans le bassin.
//   ● Départ ET arrivée dans le périmètre de la région Occitanie hors trajets internes aux autres
//     Autorités Organisatrices de Mobilité de la Région (cf. Annexe 2) ;
//   ● Exclusion des trajets internes aux autres AOM de la Région (cf. Annexe 2), exemple Narbonne->Narbonne.
//     (
//        Point d'attention : les trajets entre deux AOM de la Région Occitanie -
//        exemple : Narbonne ->Béziers ou Montpellier-->Gignac - sont en revanche bien éligibles
//     )
//   ● Exclusion des trajets effectué le dimanche ;
//   ● Exclusion des trajets dont la contribution passager est nulle hormis trajet effectué avant le 1er novembre non inclus ;
//     Le ticket passager minimum est de 0,5 € par trajet.
//   ● 2 trajets / jour / passager ;
//   ● 6 trajets / jour / conducteur ;
//

// Politique de la région Occitanie
export const Occitanie20262027 = class extends AbstractPolicyHandler implements PolicyHandlerInterface {
  static readonly id = "occitanie_2026";

  protected operators: TimestampedOperators = [
    {
      date: new Date("2026-01-01T00:00:00+0100"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
        OperatorsEnum.MOBICOOP,
      ],
    },
  ];

  protected operator_class = ["B", "C"];

  // Tranches de calcul des incitations :
  protected slices: RunnableSlices = [
    // De 2 à 20km : 2€ par passager transporté - contribution passager
    // Application du seuil au global
    {
      start: 2_000,
      end: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 200) - getContribution(ctx),
    },

    // De 20 à 30km : 0,10 € par trajet par km par passager moins la contribution du passager, plafonné à 2,00 €
    // Application du seuil au global
    {
      start: 20_000,
      end: 30_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 10 })) - getContribution(ctx),
    },

    // Pour les trajets au-delà de 30km et jusqu’à 50km sur les bassins listés en annexe 1 : plafonné à 2€ par passager transporté
    // Application du seuil au global
    {
      start: 30_000,
      end: 50_000,
      fn: (ctx: StatelessContextInterface) => {
        // Slice is not available on non-specific area
        if (!ctx.data?.area) return 0;

        // On specific areas, the calculation is the same as the second slice
        return perSeat(ctx, perKm(ctx, { amount: 10 })) - getContribution(ctx);
      },
    },
  ];

  constructor(public max_amount: number) {
    super();
    this.limits = [
      [
        "9f66ccb6-6464-4a7f-a5dc-8e90fe7b1a8b",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "3e8aeda5-793a-4bd7-b11f-66bd14dc0b08",
        2,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Passenger,
      ],
      [
        "db8ac0ab-a2e8-4104-b2a9-6c4afd60e020",
        max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected processExclusions(ctx: StatelessContextInterface) {
    // Exclusion des trajets dont la contribution passager est nulle
    // Le ticket passager minimum est de 0,5 € par trajet.
    if (getContribution(ctx) < 50) {
      throw new NotEligibleTargetException("Passenger contribution is too low");
    }

    // Opérateurs : BlaBlaCar Daily, Karos, Mobicoop
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));

    // Classe de preuve : B ou C
    isOperatorClassOrThrow(ctx, this.operator_class);

    // Départ ou Arrivée dans un bassin
    // Distance : au minimum 2.000 et au maximum 30.000 inclus sauf sur les bassins de mobilité
    // de Mende, d’Armagnac, de Rodez, d’Alès (cf. Annexe 1) et dans ce cas les trajets éligibles
    // sont au minimum 2.000 et au maximum 50.000 inclus dont l’origine et/ou la destination est dans le bassin.
    set(ctx, "data.area", startsOrEndsWithinArea(ctx)); // boolean
    if (ctx.data!.area) {
      onDistanceRangeOrThrow(ctx, { min: 2_000, max: 50_000 });
    } else {
      onDistanceRangeOrThrow(ctx, { min: 2_000, max: 30_000 });
      // Départ ET arrivée dans la région Occitanie (limite max)
      startsAndEndsAtOrThrow(ctx, { [TerritoryCodeEnum.Region]: [occitanie2026Data.region[TerritoryCodeEnum.Region]] });

      // Départ ET arrivée dans le périmètre de l'AOM Région et des AOM locales...
      startsAndEndsAtOrThrow(ctx, {
        [TerritoryCodeEnum.Mobility]: [
          ...occitanie2026Data.inner,
          occitanie2026Data.region[TerritoryCodeEnum.Mobility],
        ],
      });

      // ...hors trajets internes aux autres Autorités Organisatrices de Mobilité de la Région (cf. Annexe 2) ;
      // sauf si les AOM sont différentes
      filterInnerAOMOrThrow(ctx);
    }
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusions(ctx);
    super.processStateless(ctx);

    // Calcul de l'incitation pour la tranche cible
    let amount = 0;
    for (const { start, end, fn } of this.slices) {
      if (onDistanceRange(ctx, { min: start, max: end })) {
        amount = fn(ctx);
      }
    }

    // Application du seuil d'incitation maximal de 2,00 €
    // et minimal de 0,00 €
    ctx.incentive.set(Math.max(Math.min(200, amount), 0));
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
