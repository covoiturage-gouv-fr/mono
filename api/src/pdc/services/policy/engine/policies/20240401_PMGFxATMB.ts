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
import { LimitTargetEnum, watchForGlobalMaxAmount, watchForPersonMaxAmountByMonth } from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { perKm, perSeat } from "../helpers/per.ts";
import { startsAndEndsAt } from "../helpers/position.ts";
import { startsOrEndsAtOrThrow } from "../helpers/startsOrEndsAtOrThrow.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// INSERT INTO policy.policies (territory_id, start_date, end_date, name, unit, status, handler, max_amount)
// VALUES (
//   36102,
//   '2024-04-01T00:00:00+0200',
//   '2025-01-01T00:00:00+0100',
//   'PMGFxATMB 2024',
//   'euro',
//   'draft',
//   'pmgf_x_atmb_2024',
//   24244600
// );

export const PMGFxATMB2024: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "pmgf_x_atmb_2024";

  protected operator_class = ["B", "C"];

  protected readonly operators: TimestampedOperators = [
    {
      date: new Date("2024-04-01T00:00:00+0200"),
      operators: [OperatorsEnum.BLABLACAR_DAILY],
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "B87E2FFA-6837-43D6-B81E-D3436AB06CF1",
        50_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "F50EF28B-78F5-4366-807E-80CBDBDD2DFF",
        this.max_amount,
        watchForGlobalMaxAmount,
      ], // required
    ];
  }

  /**
   * Trajets intra-AOM (origine ET destination dans l'AOM)
   * - 1,50€ par passager de 5 à 20 km
   * - 1,50€ par passager + 0,125€ pour les km > 20 km
   * - limite d'incitation à 40 km
   */
  protected internalTripsSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 150),
    },
    {
      start: 20_000,
      end: 150_000,
      fn: (ctx: StatelessContextInterface) =>
        perSeat(
          ctx,
          perKm(ctx, { amount: 12.5, offset: 20_000, limit: 40_000 }),
        ),
    },
  ];

  /**
   * Trajets extra-AOM (origine OU destination dans l'AOM)
   * - 0,50€ par passager de 5 à 20 km
   * - 0,50€ par passager + 0,125€ pour les km > 20 km
   * - limite d'incitation à 40 km
   */
  protected externalTripsSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 50),
    },
    {
      start: 20_000,
      end: 150_000,
      fn: (ctx: StatelessContextInterface) =>
        perSeat(
          ctx,
          perKm(ctx, { amount: 12.5, offset: 20_000, limit: 40_000 }),
        ),
    },
  ];

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
    ],
  };

  protected getSlices(ctx?: StatelessContextInterface): RunnableSlices {
    if (!ctx) return this.internalTripsSlices;
    return startsAndEndsAt(ctx, this.territorySelector) ? this.internalTripsSlices : this.externalTripsSlices;
  }

  protected processExclusions(ctx: StatelessContextInterface) {
    isOperatorOrThrow(
      ctx,
      getOperatorsAt(this.operators, ctx.carpool.datetime),
    );
    isOperatorClassOrThrow(ctx, this.operator_class);
    startsOrEndsAtOrThrow(ctx, this.territorySelector);
    onDistanceRangeOrThrow(ctx, { min: 4_999, max: 150_000 });
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusions(ctx);
    super.processStateless(ctx);

    // Apply each slice and sum up the results
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
