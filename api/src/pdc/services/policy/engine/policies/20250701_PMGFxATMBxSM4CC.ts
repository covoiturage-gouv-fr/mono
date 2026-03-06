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
//   '2025-01-01T00:00:00+0200',
//   '2025-12-31T00:00:00+0100',
//   'PMGFxATMB 2025',
//   'euro',
//   'draft',
//   'pmgf_atmb_2025',
//   24244600
// );

export const PMGFxATMBxSM4CCx2025: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "pmgf_atmb_sm4cc_2025";

  protected operator_class = ["B", "C"];

  protected readonly operators: TimestampedOperators = [
    {
      date: new Date("2025-07-01T00:00:00+0100"),
      operators: [OperatorsEnum.BLABLACAR_DAILY],
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "ddf5f99c-a40c-413c-bbea-927861cbb2f2",
        50_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "cd6a2dd5-5e45-49fe-8618-09d3e8d9c679",
        this.max_amount,
        watchForGlobalMaxAmount,
      ], // required
    ];
  }

  /**
   * Trajets intra-AOM (origine ET destination dans l'AOM)
   * - 1,50€ par passager de 5 à 20 km
   * - 1,50€ par passager + 0,05€ de 20 à 30 km
   * - 2,00€ par passager de 30 à 40 km
   * - 2,00€ par passager - 0,10€ de 40 à 50 km
   * - 1,00€ par passager au delà de 50 km
   * - limite d'incitation à 80 km
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
   * Trajets extra-AOM (origine OU destination dans l'AOM)
   * - 0,50€ par passager de 5 à 20 km
   * - 1,00€ par passager > 20 km
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
    if (!ctx) return this.intraAOMSlices;

    return startsAndEndsAt(ctx, this.territorySelector) ? this.intraAOMSlices : this.extraAOMSlices;
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
