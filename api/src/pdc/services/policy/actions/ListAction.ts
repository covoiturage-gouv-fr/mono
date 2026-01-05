import { handler, KernelInterfaceResolver } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { copyFromContextMiddleware, hasPermissionMiddleware } from "@/pdc/providers/middleware/index.ts";
import {
  ParamsInterface as OperatorParamsInterface,
  ResultInterface as OperatorResultInterface,
  signature as operatorFindSignature,
} from "../../operator/contracts/find.contract.ts";
import { handlerConfig, ParamsInterface, ResultInterface, SingleResultInterface } from "../contracts/list.contract.ts";
import { alias } from "../contracts/list.schema.ts";
import { Policy } from "../engine/entities/Policy.ts";
import { SerializedPolicyInterface } from "../interfaces/engine/PolicyInterface.ts";
import { PolicyRepositoryProviderInterfaceResolver } from "../interfaces/index.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    hasPermissionMiddleware("common.policy.list"),
    copyFromContextMiddleware("call.user.territory_id", "territory_id"),
    copyFromContextMiddleware(`call.user.operator_id`, "operator_id"),
    ["validate", alias],
  ],
  apiRoute: {
    path: "/campaign/list",
    method: "POST",
  },
})
export class ListAction extends AbstractAction {
  constructor(
    private kernel: KernelInterfaceResolver,
    private repository: PolicyRepositoryProviderInterfaceResolver,
  ) {
    super();
  }

  public override async handle(params: ParamsInterface): Promise<ResultInterface> {
    const policies: SerializedPolicyInterface[] = await this.repository.findWhere(params);

    const result: ResultInterface = await Promise.all(
      policies.map(async (r) => {
        const policy: SingleResultInterface = { ...r, params: {} };
        try {
          const importedPolicy = await Policy.import(r);
          policy.params = importedPolicy.params();
          return policy;
        } catch (e) {
          const errMsg = e instanceof Error ? e.message : String(e);
          logger.warn(`Could not import policy ${r._id}`, errMsg);
          return policy;
        }
      }),
    );

    if (!params.operator_id) {
      return result;
    }

    const operator: OperatorResultInterface = await this.kernel.call<OperatorParamsInterface, OperatorResultInterface>(
      operatorFindSignature,
      { _id: params.operator_id },
      {
        channel: { service: handlerConfig.service },
        call: { user: { permissions: ["registry.operator.find"] } },
      },
    );

    return result.filter((p) => this.withOperator(p, operator)).filter((p) => this.withoutDraft(p));
  }

  private withoutDraft(p: SingleResultInterface): boolean {
    return p.status !== "draft";
  }

  private withOperator(p: SingleResultInterface, operator: OperatorResultInterface): boolean {
    return (p.params?.allTimeOperators ?? p.params?.operators)?.includes(
      operator.uuid,
    ) as boolean;
  }
}
