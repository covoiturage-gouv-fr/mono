import { RunnableSlices } from "../../interfaces/engine/PolicyInterface.ts";
import {
  OperatorsEnum,
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  PolicyHandlerStaticInterface,
  StatelessContextInterface,
} from "../../interfaces/index.ts";
import { getOperatorsAt, TimestampedOperators } from "../helpers/getOperatorsAt.ts";
import { isOperatorClassOrThrow } from "../helpers/isOperatorClassOrThrow.ts";
import { isOperatorOrThrow } from "../helpers/isOperatorOrThrow.ts";
import {
  LimitTargetEnum,
  watchForGlobalMaxAmount,
  watchForGlobalMaxTrip,
  watchForPassengerMaxByTripByDay,
  watchForPersonMaxTripByDay,
} from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { perKm, perSeat } from "../helpers/per.ts";
import { startsAndEndsAtOrThrow } from "../helpers/startsAndEndsAtOrThrow.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// Politique de Nantes Métropole
export const NantesMetropoleXPCovoitan2021: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "656";
  protected operators: TimestampedOperators = [
    {
      date: new Date("2021-12-02T00:00:00+0100"),
      operators: [OperatorsEnum.KLAXIT],
    },
  ];

  protected operatorClass = ["C"];
  protected slices: RunnableSlices = [
    {
      start: 2_000,
      end: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 200),
    },
    {
      start: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 10 })),
    },
  ];

  constructor(public max_amount: number) {
    super();
    this.limits = [
      [
        "D5FA9FA9-E8CC-478E-80ED-96FDC5476689",
        3,
        watchForPassengerMaxByTripByDay,
      ],
      [
        "6456EC1D-2183-71DC-B08E-0B8FC30E4A4E",
        4,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Passenger,
      ],
      [
        "286AAF87-5CDB-A7C0-A599-FBE7FB6C5442",
        4,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "D1FED21B-5160-A1BF-C052-5DA7A190996C",
        10_000_000,
        watchForGlobalMaxTrip,
      ],
      [
        "69FD0093-CEEE-0709-BB80-878D2E857630",
        max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected processExclusion(ctx: StatelessContextInterface) {
    isOperatorOrThrow(
      ctx,
      getOperatorsAt(this.operators, ctx.carpool.datetime),
    );
    onDistanceRangeOrThrow(ctx, { min: 2_000 });
    isOperatorClassOrThrow(ctx, this.operatorClass);
    // Exclure les trajets qui ne sont pas dans l'aom NM
    startsAndEndsAtOrThrow(ctx, { aom: ["244400404"] });
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusion(ctx);
    super.processStateless(ctx);

    // Par kilomètre
    let amount = 0;
    for (const { start, end, fn } of this.slices) {
      if (onDistanceRange(ctx, { min: start, max: end })) {
        amount = fn(ctx);
      }
    }

    ctx.incentive.set(amount);
  }

  params(): PolicyHandlerParamsInterface {
    return {
      tz: "Europe/Paris",
      slices: this.slices,
      operators: getOperatorsAt(this.operators),
      limits: {
        glob: this.max_amount,
      },
    };
  }

};
