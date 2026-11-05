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
import { onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { startsOrEndsAtOrThrow } from "../helpers/startsOrEndsAtOrThrow.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// INSERT INTO policy.policies (territory_id, start_date, end_date, name, unit, status, handler, max_amount)
// VALUES (
//   36102,
//   '2026-11-01T00:00:00+0100',
//   '2027-03-01T00:00:00+0100',
//   'PMGF x SM4CC 2026',
//   'euro',
//   'draft',
//   'pmgf_sm4cc_2026',
//   2500000
// );

// Éligibilité
//
//   - Opérateur : Blablacar Daily
//   - Classe de preuve : B, C
//   - Distance : de 5 à 80 km
//   - Zone d'intervention : origine et/ou destination sur le territoire du Genevois
//     français (EPCI membres du PMGF) + SM4CC
//   - Exclusion au-delà de 50 euros de gain pour le conducteur par mois
//
// Modalité de calcul de l'incitation
//
//   Montant déclaré par BlaBlaCar Daily au SIREN du PMGF : l'opérateur applique la grille,
//   les 10 trajets gratuits par passager et la répartition entre territoires financeurs.
//   La limite de 6 trajets par jour est appliquée par l'opérateur (vérifié sur 2026).

const PMGF_SIREN = "200075372";

export const PMGFxSM4CC2026: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "pmgf_sm4cc_2026";

  protected operator_class = ["B", "C"];

  protected readonly operators: TimestampedOperators = [
    {
      date: new Date("2026-11-01T00:00:00+0100"),
      operators: [OperatorsEnum.BLABLACAR_DAILY],
    },
  ];

  constructor(public max_amount: number) {
    super();
    this.limits = [
      [
        "866bc618-a4f2-4f18-add0-aae61b1cdfd7",
        50_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "153f6f4d-9b08-42b3-815c-6f4ee058a079",
        this.max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected slices: RunnableSlices = [
    {
      start: 5_000,
      end: 80_000,
      fn: (ctx: StatelessContextInterface) => declaredIncentive(ctx),
    },
  ];

  protected territorySelector: TerritorySelectorsInterface = {
    [TerritoryCodeEnum.Mobility]: [
      "240100750", // CA du Pays de Gex
      "247400690", // CC du Genevois
      "200011773", // CA Annemasse-Les Voirons-Agglomération
      "200067551", // CA Thonon Agglomération
    ],
    [TerritoryCodeEnum.CityGroup]: [
      "240100891", // CC Terre Valserhône
      "247400724", // CC du Pays Rochois
      "200000172", // CC Faucigny-Glières
      "247400583", // CC Arve et Salève
      "247400666", // CC des Quatre Rivières (74)
    ],
  };

  protected processExclusions(ctx: StatelessContextInterface) {
    isOperatorOrThrow(ctx, getOperatorsAt(this.operators, ctx.carpool.datetime));
    isOperatorClassOrThrow(ctx, this.operator_class);
    startsOrEndsAtOrThrow(ctx, this.territorySelector);
    onDistanceRangeOrThrow(ctx, { min: 4_999, max: 80_001 });
  }

  override processStateless(ctx: StatelessContextInterface): void {
    this.processExclusions(ctx);
    super.processStateless(ctx);
    ctx.incentive.set(declaredIncentive(ctx));
  }

  params(): PolicyHandlerParamsInterface {
    return {
      tz: "Europe/Paris",
      slices: this.slices,
      operators: getOperatorsAt(this.operators),
      limits: {
        glob: this.max_amount,
      },
      extras: {
        declared_incentives: { siren: PMGF_SIREN },
      },
    };
  }
};

function declaredIncentive(ctx: StatelessContextInterface): number {
  return (ctx.carpool.operator_incentives ?? [])
    .filter(({ siret }) => siret.startsWith(PMGF_SIREN))
    .reduce((sum, { amount }) => sum + amount, 0);
}
