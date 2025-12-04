import {
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  StatefulContextInterface,
  StatelessContextInterface,
} from "../../interfaces/index.ts";
import { applyLimitsOnStatefulStage, applyLimitsOnStatelessStage, ConfiguredLimitInterface } from "../helpers/index.ts";

export abstract class AbstractPolicyHandler implements PolicyHandlerInterface {
  public limits: Array<ConfiguredLimitInterface> = [];

  async load(): Promise<void> {
    return;
  }

  processStateless(ctx: StatelessContextInterface): void {
    // Mise en place des limites
    applyLimitsOnStatelessStage(this.limits, ctx);
  }

  processStateful(ctx: StatefulContextInterface): void {
    applyLimitsOnStatefulStage(this.limits, ctx);
  }

  abstract params(): PolicyHandlerParamsInterface;
  abstract describe(): string;
}
