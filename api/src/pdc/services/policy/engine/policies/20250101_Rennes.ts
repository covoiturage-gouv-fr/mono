import type { GeoJSON } from "../../../geo/contracts/GeoJson.ts";
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
import { isBefore } from "../helpers/isBefore.ts";
import { isOperatorClassOrThrow } from "../helpers/isOperatorClassOrThrow.ts";
import { isOperatorOrThrow } from "../helpers/isOperatorOrThrow.ts";
import { isStartAndEndInsideOrThrow } from "../helpers/isStartAndEndInside.ts";
import {
  LimitTargetEnum,
  watchForGlobalMaxAmount,
  watchForPersonMaxAmountByMonth,
  watchForPersonMaxTripByDay,
} from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { perKm, perSeat } from "../helpers/per.ts";
import { endsAt } from "../helpers/position.ts";
import { description } from "./20250101_Rennes.html.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// INSERT INTO policy.policies (territory_id, start_date, end_date, name, unit, status, handler, max_amount)
// VALUES (
//   204,
//   '2025-01-01T00:00:00+0200',
//   '2027-12-31T00:00:00+0100',
//   'Rennes 2025',
//   'euro',
//   'draft',
//   'rennes_2025',
//   120950000
// );

export const Rennes2025: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "rennes_2025";

  protected operator_class = ["B", "C"];

  // Geojson de la zone de transports en communs de Rennes Métropole
  protected readonly tcShape: GeoJSON = {
    coordinates: [
      [
        [
          [
            -1.638948103907376,
            48.09147699849534,
          ],
          [
            -1.636795806096824,
            48.08932016578465,
          ],
          [
            -1.636429792741697,
            48.086587355320134,
          ],
          [
            -1.638116668019853,
            48.08408717357746,
          ],
          [
            -1.641420167739982,
            48.08246586417821,
          ],
          [
            -1.653576543852361,
            48.08232129182403,
          ],
          [
            -1.659312733643364,
            48.08104042207028,
          ],
          [
            -1.676148915756742,
            48.08145669251199,
          ],
          [
            -1.679936360958917,
            48.08284520814578,
          ],
          [
            -1.682303918387132,
            48.085193991177626,
          ],
          [
            -1.683335863247037,
            48.100250564600934,
          ],
          [
            -1.686921338431296,
            48.10034980257605,
          ],
          [
            -1.691778233004759,
            48.099660228391464,
          ],
          [
            -1.690030219517554,
            48.09763442229493,
          ],
          [
            -1.689552871984661,
            48.09517213113651,
          ],
          [
            -1.690495651254111,
            48.09287383790781,
          ],
          [
            -1.692871744243382,
            48.090697855291225,
          ],
          [
            -1.70117583014511,
            48.086625347312825,
          ],
          [
            -1.704565767627273,
            48.086301922575394,
          ],
          [
            -1.708050088303171,
            48.08702712742812,
          ],
          [
            -1.711678996723696,
            48.09009667533552,
          ],
          [
            -1.711934794653844,
            48.09253525031568,
          ],
          [
            -1.710542530331177,
            48.094819046415445,
          ],
          [
            -1.713167967028094,
            48.09730073615155,
          ],
          [
            -1.714528689332605,
            48.10067259101234,
          ],
          [
            -1.714533438600843,
            48.10356828141405,
          ],
          [
            -1.713085297700747,
            48.106308461169775,
          ],
          [
            -1.70999577934603,
            48.10876136156187,
          ],
          [
            -1.705610309499877,
            48.11031366309299,
          ],
          [
            -1.692706736627799,
            48.11045619846096,
          ],
          [
            -1.6873340295244,
            48.111512437062814,
          ],
          [
            -1.695498130334795,
            48.115958309962366,
          ],
          [
            -1.71525841591912,
            48.11650349589571,
          ],
          [
            -1.718963367992966,
            48.11914930206418,
          ],
          [
            -1.719389789259615,
            48.122619022697435,
          ],
          [
            -1.716562826636009,
            48.12554102131891,
          ],
          [
            -1.711888953722239,
            48.126695172818316,
          ],
          [
            -1.69165652469281,
            48.126933851579714,
          ],
          [
            -1.686184637480305,
            48.12568729343689,
          ],
          [
            -1.682475566435195,
            48.12283150017781,
          ],
          [
            -1.674174639949648,
            48.123897051607386,
          ],
          [
            -1.668252014437109,
            48.1318090381665,
          ],
          [
            -1.664171011236432,
            48.13385140874144,
          ],
          [
            -1.659529380480262,
            48.13471457529055,
          ],
          [
            -1.653990452807529,
            48.13477970023785,
          ],
          [
            -1.649867349681107,
            48.133867803773285,
          ],
          [
            -1.64607659341651,
            48.13156911020507,
          ],
          [
            -1.644126682636015,
            48.1282538743125,
          ],
          [
            -1.640861776345678,
            48.12760146240146,
          ],
          [
            -1.634517565315949,
            48.13114970930476,
          ],
          [
            -1.628450953201108,
            48.132617870678345,
          ],
          [
            -1.626471721135549,
            48.1347929820377,
          ],
          [
            -1.62330281648268,
            48.13650234045506,
          ],
          [
            -1.619039279633223,
            48.1369739433029,
          ],
          [
            -1.614843233880076,
            48.13580889850581,
          ],
          [
            -1.612412124394998,
            48.1335998304304,
          ],
          [
            -1.611952655713098,
            48.13072368923329,
          ],
          [
            -1.614233053193967,
            48.125541893567544,
          ],
          [
            -1.618157444447494,
            48.122776490168874,
          ],
          [
            -1.621965669179866,
            48.121835432287206,
          ],
          [
            -1.62620392262285,
            48.12188680490278,
          ],
          [
            -1.632862912237619,
            48.11810864153511,
          ],
          [
            -1.639633270615898,
            48.11650841274539,
          ],
          [
            -1.652737940268052,
            48.11744860352618,
          ],
          [
            -1.656322765540391,
            48.11889973087066,
          ],
          [
            -1.658567604402116,
            48.12098947002866,
          ],
          [
            -1.659444111560055,
            48.1182566862215,
          ],
          [
            -1.661414787929339,
            48.116020216991295,
          ],
          [
            -1.668127076001682,
            48.11343046529089,
          ],
          [
            -1.663039946790389,
            48.11034601855237,
          ],
          [
            -1.661083168771699,
            48.10645716189524,
          ],
          [
            -1.663653443996232,
            48.0995312295473,
          ],
          [
            -1.666819382221401,
            48.0965819552396,
          ],
          [
            -1.666426819031004,
            48.09193779869726,
          ],
          [
            -1.661176496462798,
            48.091849230170666,
          ],
          [
            -1.654503787296973,
            48.09313407118887,
          ],
          [
            -1.643028958335048,
            48.09280806606998,
          ],
          [
            -1.638948103907376,
            48.09147699849534,
          ],
        ],
      ],
    ],
    type: "MultiPolygon",
  };

  protected readonly operators: TimestampedOperators = [
    {
      date: new Date("2025-01-01T00:00:00+0200"),
      operators: [OperatorsEnum.BLABLACAR_DAILY],
    },
  ];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "ddf5f99c-a40c-413c-bbea-927861cbb2f2",
        150_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "8C5251E8-AB82-EB29-C87A-2BF59D4F6328",
        6,
        watchForPersonMaxTripByDay,
        LimitTargetEnum.Driver,
      ],
      [
        "cd6a2dd5-5e45-49fe-8618-09d3e8d9c679",
        this.max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  /**
   * Trajets avant le 1 juillet (origine OU destination dans l'AOM)
   * - 0,50€ par passager de 5 à 60 km
   */
  protected beforeJulyTripsSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 60_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 50),
    },
  ];

  /**
   * Trajets après le 1er juillet (origine OU destination dans l'AOM)
   * - 1€ par passager de 5km à 15 km
   * - + 0,10€ du km par passager de 15 à 30 km
   * - 2.5€ par passager au dela de 30 km
   */
  protected afterJulyTripsSlices: RunnableSlices = [
    {
      start: 5_000,
      end: 15_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, 100),
    },
    {
      start: 15_000,
      end: 30_000,
      fn: (ctx: StatelessContextInterface) =>
        perSeat(
          ctx,
          perKm(ctx, { amount: 10, offset: 15_000, limit: 30_000 }),
        ),
    },
  ];

  private firstOfJulyDate: Date = new Date("2025-06-31T00:00:00+0200");

  protected getSlices(ctx?: StatelessContextInterface): RunnableSlices {
    if (!ctx) {
      return this.beforeJulyTripsSlices;
    }
    return isBefore(ctx, this.firstOfJulyDate) ? this.beforeJulyTripsSlices : this.afterJulyTripsSlices;
  }

  protected processExclusions(ctx: StatelessContextInterface) {
    isOperatorOrThrow(
      ctx,
      getOperatorsAt(this.operators, ctx.carpool.datetime),
    );
    isOperatorClassOrThrow(ctx, this.operator_class);
    onDistanceRangeOrThrow(ctx, { min: 4_999, max: 60_001 });
    isStartAndEndInsideOrThrow(ctx, this.tcShape);
    if (
      endsAt(ctx, { aom: ["200039022"] }) || // Vitré
      endsAt(ctx, { com: ["35069"] }) || // Châteaugiron
      endsAt(ctx, { aom: ["243500774"] }) || // Liffré Cormier communauté
      endsAt(ctx, { aom: ["200070662"] }) || // Bretagne Porte de Loire
      endsAt(ctx, { aom: ["200043990"] }) //  Vallons de Haute-Bretagne Communauté
    ) {
      throw new NotEligibleTargetException();
    }
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

  describe(): string {
    return description;
  }
};
