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
import { atDate } from "../helpers/atDate.ts";
import { applyCooldown, registerCooldown } from "../helpers/cooldown.ts";
import { getOperatorsAt, TimestampedOperators } from "../helpers/getOperatorsAt.ts";
import { isOperatorClassOrThrow } from "../helpers/isOperatorClassOrThrow.ts";
import { isOperatorOrThrow } from "../helpers/isOperatorOrThrow.ts";
import { isPublicHoliday } from "../helpers/isPublicHoliday.ts";
import {
  LimitTargetEnum,
  watchForGlobalMaxAmount,
  watchForMaxPassengersPerDriverPerDay,
  watchForMaxPassengersPerTrip,
  watchForPersonMaxAmountByMonth,
} from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { onHourRange } from "../helpers/onHourRange.ts";
import { onWeekday } from "../helpers/onWeekday.ts";
import { perKm, perSeat } from "../helpers/per.ts";
import { endsAt, startsAt } from "../helpers/position.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// Fiche descriptive : campagne d'incitation Île-de-France Mobilités x Ecov
// (lignes de covoiturage Ecov IDFM). Validée AOM. Voir ticket RPC #347769.
//
// Éligibilité :
//   • Opérateur Ecov, classe de preuve C, distance >= 2 km.
//   • Départ ET arrivée sur le territoire d'Île-de-France Mobilités.
//   • Service ouvert du lundi au vendredi de 4h à 23h (heure de départ),
//     hors jours fériés — sauf la journée de solidarité (lundi de Pentecôte).
//   • Non-tronçonnage : un même passager ne peut être pris en charge par le
//     même conducteur moins de 60 minutes après le trajet précédent.
//
// Barème (identique à Covoit IDFM 2026) :
//   • 2 € de 2 à 20 km, puis 0,10 €/km, plafonné à 3 € par trajet et passager.
//
// Plafonds :
//   • 200 €/mois/conducteur, 6 trajets passager/jour/conducteur,
//     3 trajets passager/trip (Ecov : 1 passager par journey => sièges = journeys).
export const EcovIDFM2026: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "ecov_idfm_2026";

  // Code INSEE de l'AOM Île-de-France Mobilités
  protected readonly idfmAom = "287500078";

  // Journée de solidarité (lundi de Pentecôte) : jour férié mais travaillé,
  // donc le service reste ouvert et les trajets restent éligibles.
  protected readonly journeeSolidarite = [
    "2025-06-09",
    "2026-05-25",
    "2027-05-17",
  ];

  // À activer si la collectivité confirme l'exclusion des trajets Paris-Paris
  // (absente de la fiche descriptive, en attente retour équipe).
  protected readonly excludeParisParis = false;

  constructor(public max_amount: number) {
    super();
    this.limits = [
      // 200 € max / mois / conducteur
      [
        "0d5f2a1e-9a3c-4d0a-8b6e-1f2c3d4e5f60",
        200_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      // 6 trajets passager / jour / conducteur
      [
        "1e6f3b2f-0b4d-5e1b-9c7f-2a3b4c5d6e71",
        6,
        watchForMaxPassengersPerDriverPerDay,
      ],
      // 3 trajets passager / trip
      [
        "2f7a4c30-1c5e-6f2c-ad80-3b4c5d6e7f82",
        3,
        watchForMaxPassengersPerTrip,
      ],
      // Plafond global de la campagne
      [
        "3a8b5d41-2d6f-7a3d-be91-4c5d6e7f8093",
        max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected operators: TimestampedOperators = [
    {
      date: new Date("2025-04-28T00:00:00+0200"),
      operators: [
        OperatorsEnum.ECOV,
      ],
    },
  ];

  // Identifiant de la métadonnée de non-tronçonnage
  protected readonly cooldownUuid = "4b9c6e52-3e70-8b4e-cfa2-5d6e7f809104";
  protected readonly cooldownMinutes = 60;

  protected slices: RunnableSlices = [
    {
      start: 2_000,
      end: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 200),
    },
    {
      start: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 10, offset: 20_000, limit: 30_000 })),
    },
  ];

  protected processExclusion(ctx: StatelessContextInterface) {
    // Opérateur Ecov uniquement
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));

    // Distance minimum 2 km
    onDistanceRangeOrThrow(ctx, { min: 2_000 });

    // Départ ET arrivée dans l'AOM Île-de-France Mobilités
    if (!startsAt(ctx, { aom: [this.idfmAom] }) || !endsAt(ctx, { aom: [this.idfmAom] })) {
      throw new NotEligibleTargetException();
    }

    // Exclusion Paris-Paris (désactivée par défaut, voir excludeParisParis)
    if (this.excludeParisParis && startsAt(ctx, { com: ["75056"] }) && endsAt(ctx, { com: ["75056"] })) {
      throw new NotEligibleTargetException();
    }

    // Ouverture : lundi au vendredi, 4h-23h, hors jours fériés (sauf solidarité)
    const isHoliday = isPublicHoliday(ctx) && !atDate(ctx, { dates: this.journeeSolidarite });
    if (
      !onWeekday(ctx, { days: [1, 2, 3, 4, 5] }) ||
      !onHourRange(ctx, { start: 4, end: 23 }) ||
      isHoliday
    ) {
      throw new NotEligibleTargetException();
    }

    // Classe de preuve C
    isOperatorClassOrThrow(ctx, ["C"]);
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
    super.processStateless(ctx);

    // Non-tronçonnage : mémorise le couple conducteur/passager
    registerCooldown(ctx, this.cooldownUuid);

    // Calcul de l'incitation par tranches (additif)
    let amount = 0;
    for (const { start, fn } of this.slices) {
      if (onDistanceRange(ctx, { min: start })) {
        amount += fn(ctx);
      }
    }

    ctx.incentive.set(amount);
  }

  override processStateful(ctx: StatefulContextInterface): void {
    // Applique le non-tronçonnage avant les limites cumulatives
    applyCooldown(ctx, this.cooldownUuid, this.cooldownMinutes);
    super.processStateful(ctx);
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
