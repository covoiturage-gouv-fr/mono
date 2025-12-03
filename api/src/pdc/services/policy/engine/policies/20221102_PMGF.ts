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
import { LimitTargetEnum, watchForGlobalMaxAmount, watchForPersonMaxAmountByMonth } from "../helpers/limits.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "../helpers/onDistanceRange.ts";
import { perKm, perSeat } from "../helpers/per.ts";
import { startsAndEndsAt } from "../helpers/position.ts";
import { description } from "./20221102_PMGF.html.ts";
import { AbstractPolicyHandler } from "./AbstractPolicyHandler.ts";

// Politique du Pôle Métropolitain Genevois Français
// eslint-disable-next-line max-len
export const PMGF2022: PolicyHandlerStaticInterface = class extends AbstractPolicyHandler
  implements PolicyHandlerInterface {
  static readonly id = "pmgf_2022";

  protected operators: TimestampedOperators = [
    {
      date: new Date("2022-11-02T00:00:00+0100"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
        OperatorsEnum.KLAXIT,
      ],
    },
    {
      date: new Date("2023-01-02T00:00:00+0100"),
      operators: [
        OperatorsEnum.BLABLACAR_DAILY,
        OperatorsEnum.KAROS,
        OperatorsEnum.KLAXIT,
        OperatorsEnum.MOBICOOP,
      ],
    },
  ];
  protected operator_class = ["B", "C"];

  constructor(public max_amount: bigint) {
    super();
    this.limits = [
      [
        "AFE1C47D-BF05-4FA9-9133-853D29797D09",
        120_00,
        watchForPersonMaxAmountByMonth,
        LimitTargetEnum.Driver,
      ],
      [
        "98B26189-C6FC-4DB1-AC1C-41F779C5B3C7",
        this.max_amount,
        watchForGlobalMaxAmount,
      ],
    ];
  }

  protected slices: RunnableSlices = [
    {
      start: 4_000,
      end: 20_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, startsAndEndsAt(ctx, { arr: this.arr }) ? 200 : 100),
    },
    {
      start: 20_000,
      end: 40_000,
      fn: (ctx: StatelessContextInterface) => perSeat(ctx, perKm(ctx, { amount: 10, offset: 20_000, limit: 40_000 })),
    },
  ];

  protected arr = [
    "74008",
    "74005",
    "74007",
    "74012",
    "74013",
    "74015",
    "74016",
    "74018",
    "74020",
    "74021",
    "74024",
    "74025",
    "74031",
    "01044",
    "74040",
    "74042",
    "74043",
    "74044",
    "74048",
    "74049",
    "74053",
    "01071",
    "01078",
    "01081",
    "01082",
    "74070",
    "74074",
    "01103",
    "01104",
    "74069",
    "01109",
    "74082",
    "01114",
    "74087",
    "74090",
    "74094",
    "01135",
    "74101",
    "01143",
    "74105",
    "74106",
    "01153",
    "74116",
    "74118",
    "74121",
    "01158",
    "74124",
    "01160",
    "74126",
    "74133",
    "01173",
    "01174",
    "74212",
    "01180",
    "01189",
    "74144",
    "74145",
    "74059",
    "74193",
    "74224",
    "74150",
    "74153",
    "74156",
    "74157",
    "01209",
    "01210",
    "74158",
    "74163",
    "74164",
    "74171",
    "74180",
    "01247",
    "74185",
    "01257",
    "74197",
    "74199",
    "74201",
    "74206",
    "01281",
    "74210",
    "74211",
    "01298",
    "01308",
    "74216",
    "01313",
    "01288",
    "74220",
    "74229",
    "01354",
    "01357",
    "01360",
    "74243",
    "74244",
    "74250",
    "74253",
    "01397",
    "74260",
    "74262",
    "74263",
    "01401",
    "01215",
    "01399",
    "01419",
    "74281",
    "74288",
    "01033",
    "74293",
    "74296",
    "01435",
    "01436",
    "74305",
    "01448",
    "74309",
    "74312",
    "74314",
    "74298",
    "74315",
  ];

  protected processExclusion(ctx: StatelessContextInterface) {
    isOperatorOrThrow(
      ctx,
      getOperatorsAt(this.operators, ctx.carpool.datetime),
    );
    onDistanceRangeOrThrow(ctx, { min: 4_000 });
    isOperatorClassOrThrow(ctx, this.operator_class);
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

  describe(): string {
    return description;
  }
};
