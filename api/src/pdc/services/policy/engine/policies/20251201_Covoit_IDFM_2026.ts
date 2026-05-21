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
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// Article 5 TRAJETS ÉLIGIBLES

// Conformément aux exigences de l’article 6.2.1 du Cahier des Clauses Techniques Particulières (CCTP) du marché 2024-031, le soutien alloué par Île-de-France Mobilités à la pratique du covoiturage sera limité intrinsèquement aux trajets :
//
//   • De plus de 2 kilomètres, dont l’origine et la destination sont sur le territoire francilien et dont l’origine ou
//     la destination sont situées hors de la commune de Paris.
//   • Effectués par des conducteurs et par des passagers majeurs, pouvant être domiciliés à la même adresse.
//   • Réalisés par le biais de l’Application Covoit IDFM déployée pour le service Covoiturage Île-de-France Mobilités.
//   • Inscrits dans le Registre de Preuve de Covoiturage (RPC) avec un niveau de classe de type C tel que défini par le
//     RPC.
//
// Pour être éligible, nous nous assurerons qu’un trajet covoituré respectera l’ensemble des critères de l'article 6.2.2
// du CCTP Marché 2024_031), listés ci-dessous :
//
//   • Règles de plafonnement : Nous appliquerons un plafonnement de soutien financier d’Île de-France Mobilités à 200 €
//     par mois et par conducteur. Chaque trajet aura un maximum de 3 passagers. Un conducteur est limité à un maximum
//     de 6 passagers transportés par jour. Quant au nombre de trajets maximum par jour, ils sont plafonnés à 4 trajets
//     par utilisateur par jour (indépendamment du nombre de passagers par trajet).
//   • Non-tronçonnage des trajets : un covoitureur réalise deux trajets consécutivement un même jour, le second trajet
//     ne sera pas éligible s’il a débuté moins d’une demi-heure après la fin du précédent.
//   • Non-respect des modalités d'utilisation de l’Application : les utilisateurs s’engagent à respecter l’ensemble des
//     CGUs qu’ils ont signées lors de leur inscription.

// Campagne d'Île-de-France Mobilité
export const CovoitIDFM2026: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "covoit_idfm_2026";

  constructor(public max_amount: number) {
    super();
    this.limits = [
      // Nous appliquerons un plafonnement de soutien financier d’IDFM à 200 € par mois et par conducteur.
      [
        "dcee0b35-222c-46f1-aa49-cb76725aabeb",
        200_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],

      // Le nombre de trajets maximum par jour, ils sont plafonnés à 4 trajets par utilisateur par jour
      // (indépendamment du nombre de passagers par trajet).
      [
        "13b6f5ac-2764-4865-9224-3f49989a3734",
        4,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],

      // Chaque trajet aura un maximum de 3 passagers
      [
        "72361283-eec4-44a6-9f83-8953281f9627",
        3,
        watchForMaxPassengersPerTrip,
      ],

      // Un conducteur est limité à un maximum de 6 passagers transportés par jour
      [
        "d55bc553-7841-4e29-82f6-0505b9efaef6",
        6,
        watchForMaxPassengersPerDriverPerDay,
      ],

      // Plafonnement de la campagne (16M€)
      [
        "e1771f85-8ec1-4d60-bf42-a3515e2ad9dd",
        max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected operators: TimestampedOperators = [
    // Réalisés par le biais de l’Application Covoit IDFM
    {
      date: new Date("2025-12-01T00:00:00+0100"),
      operators: [
        OperatorsEnum.COVOIT_IDFM,
      ],
    },
  ];

  // Article 6 Montant de la campagne
  //
  //  • Prix passager : 0€ de 2 à 80km
  //  • Incitation conducteur : 2€ de 2 à 20km puis 0,10€ par km / supplémentaire (capé à 3€ par trajet et par passager)

  protected slices: RunnableSlices = [
    {
      start: 2_000,
      end: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 200),
    },
    {
      start: 20_000,
      // end: 80_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 10, offset: 20_000, limit: 30_000 })),
    },
  ];

  protected processExclusion(ctx: StatelessContextInterface) {
    // Opérateurs autorisés
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));

    // De plus de 2 kilomètres, dont l’origine et la destination sont sur le territoire francilien et dont l’origine ou
    // la destination sont situées hors de la commune de Paris.
    onDistanceRangeOrThrow(ctx, { min: 2_000 });

    // Exclure les trajets Paris-Paris
    if (startsAt(ctx, { com: ["75056"] }) && endsAt(ctx, { com: ["75056"] })) {
      throw new NotEligibleTargetException();
    }

    // Exclure les trajets qui ne sont pas dans l'aom
    // Code insee de l'aom IDFM en 2025: 287500078
    // https://annuaire-entreprises.data.gouv.fr/entreprise/ile-de-france-mobilites-287500078
    if ((!startsAt(ctx, { aom: ["287500078"] }) || !endsAt(ctx, { aom: ["287500078"] }))) {
      throw new NotEligibleTargetException();
    }

    // Inscrits dans le Registre de Preuve de Covoiturage (RPC)
    // avec un niveau de classe de type C tel que défini par le RPC.
    isOperatorClassOrThrow(ctx, ["C"]);
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
};
