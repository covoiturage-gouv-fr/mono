import { NotEligibleTargetException } from "@/pdc/services/policy/engine/exceptions/NotEligibleTargetException.ts";
import { atDate } from "@/pdc/services/policy/engine/helpers/atDate.ts";
import { getOperatorsAt, TimestampedOperators } from "@/pdc/services/policy/engine/helpers/getOperatorsAt.ts";
import { isAfter } from "@/pdc/services/policy/engine/helpers/isAfter.ts";
import { isBefore } from "@/pdc/services/policy/engine/helpers/isBefore.ts";
import { isOperatorClassOrThrow } from "@/pdc/services/policy/engine/helpers/isOperatorClassOrThrow.ts";
import { isOperatorOrThrow } from "@/pdc/services/policy/engine/helpers/isOperatorOrThrow.ts";
import {
  LimitTargetEnum,
  watchForGlobalMaxAmount,
  watchForPersonMaxAmountByMonth,
  watchForPersonMaxTripByDay,
} from "@/pdc/services/policy/engine/helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "@/pdc/services/policy/engine/helpers/onDistanceRange.ts";
import { perKm, perSeat } from "@/pdc/services/policy/engine/helpers/per.ts";
import { endsAt, startsAt, startsOrEndsAt } from "@/pdc/services/policy/engine/helpers/position.ts";
import { AbstractPolicyHandler } from "@/pdc/services/policy/engine/policies/AbstractPolicyHandler.ts";
import { RunnableSlices } from "@/pdc/services/policy/interfaces/engine/PolicyInterface.ts";
import {
  OperatorsEnum,
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  PolicyHandlerStaticInterface,
  StatelessContextInterface,
} from "@/pdc/services/policy/interfaces/index.ts";
import { description } from "./20210520_IDFM.html.ts";

// Campagne d'Île-de-France Mobilité
export const IDFMPeriodeNormale2021: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "459";

  constructor(public max_amount: number) {
    super();
    this.limits = [
      [
        "56042464-852C-95B8-2009-8DD4808C9370",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "ECDE3CD4-96FF-C9D2-BA88-45754205A798",
        150_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "99911EAF-89AB-C346-DDD5-BD2C7704F935",
        max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected operators: TimestampedOperators = [
    {
      date: new Date("2021-05-20T00:00:00+0200"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
        OperatorsEnum.KLAXIT,
      ],
    },
    {
      date: new Date("2023-01-01T00:00:00+0100"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
        OperatorsEnum.KLAXIT,
        OperatorsEnum.YNSTANT,
      ],
    },
    {
      date: new Date("2023-03-22T00:00:00+0100"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
        OperatorsEnum.KLAXIT,
        OperatorsEnum.YNSTANT,
        OperatorsEnum.MOBICOOP,
      ],
    },
    {
      date: new Date("2024-07-01T00:00:00+0200"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
        OperatorsEnum.YNSTANT,
        OperatorsEnum.MOBICOOP,
      ],
    },
    {
      date: new Date("2025-05-12T00:00:00+0200"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
        OperatorsEnum.MOBICOOP,
      ],
    },
  ];

  protected slices: RunnableSlices = [
    {
      start: 2_000,
      end: 15_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 150),
    },
    {
      start: 15_000,
      end: 30_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 10, offset: 15_000, limit: 30_000 })),
    },
  ];

  protected boosterDates = [
    "2022-02-18",
    "2022-03-25",
    "2022-03-26",
    "2022-03-27",
    "2022-03-28",
    "2022-06-18",
    "2022-07-06",
    "2022-11-10",
    "2023-01-19",
    "2023-01-31",
    "2023-02-07",
    "2023-03-07",
    "2023-03-08",
    "2023-03-16",
    "2023-03-17",
    "2023-03-23",
    "2023-03-24",
    "2023-03-28",
    "2023-04-06",
    "2023-04-13",
    "2023-08-12",
    "2023-08-13",
    "2023-08-14",
    "2024-10-25",
    "2024-12-11",
    "2024-12-12",
    "2024-12-13",
    "2025-07-01",
    "2025-07-02",
    "2025-09-18",
  ];

  // Dates booster dans l'Oise sur la période
  // du 6 décembre 2024 au 15 mars 2025 pour
  // un mouvement de grève spécifique.
  protected boosterDatesOise2025 = {
    start_date: new Date("2024-12-06"),
    end_date: new Date("2025-03-15"),
    coms: [
      "95074",
      "78172",
      "78382",
      "78498",
      "78551",
      "78005",
      "95450",
      "95218",
      "95183",
      "95510",
      "95637",
      "95306",
      "95572",
      "95323",
      "95394",
      "95127",
      "95476",
      "95388",
      "95500",
    ],
  };

  protected processExclusion(ctx: StatelessContextInterface) {
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));
    onDistanceRangeOrThrow(ctx, { min: 2_000, max: 150_000 });

    // Exclure les trajet Paris-Paris
    if (startsAt(ctx, { com: ["75056"] }) && endsAt(ctx, { com: ["75056"] })) {
      throw new NotEligibleTargetException();
    }

    // Exclure les trajets qui ne sont pas dans l'aom
    // Code insee de l'aom IDFM 2022: 217500016
    // Code insee de l'aom IDFM 2023: 287500078
    if (
      (!startsAt(ctx, { aom: ["217500016"] }) ||
        !endsAt(ctx, { aom: ["217500016"] })) &&
      (!startsAt(ctx, { aom: ["287500078"] }) ||
        !endsAt(ctx, { aom: ["287500078"] }))
    ) {
      throw new NotEligibleTargetException();
    }

    // Classe de trajet
    isOperatorClassOrThrow(ctx, ["B", "C"]);
    // Modification de la campagne au 1er septembre
    if (isAfter(ctx, { date: new Date("2022-09-01") })) {
      isOperatorClassOrThrow(ctx, ["C"]);
    }
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
    super.processStateless(ctx);

    // Par kilomètre
    let amount = 0;
    for (const { start, fn } of this.slices) {
      if (onDistanceRange(ctx, { min: start })) {
        amount += fn(ctx);
      }
    }

    // Jour de pollution/grève
    if (atDate(ctx, { dates: this.boosterDates })) {
      amount *= 1.5;
    }

    // Tarif spécial pour les communes de l'Oise en 2024/2025
    if (
      isAfter(ctx, { date: this.boosterDatesOise2025.start_date }) &&
      isBefore(ctx, this.boosterDatesOise2025.end_date) &&
      startsOrEndsAt(ctx, { com: this.boosterDatesOise2025.coms }) &&
      !atDate(ctx, { dates: this.boosterDates })
    ) {
      amount *= 1.5;
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
      booster_dates: [...this.boosterDates],
      extras: {
        "idfm_oise2025": this.boosterDatesOise2025,
      },
    };
  }

  describe(): string {
    return description;
  }
};
