import { ForbiddenException, handler, KernelInterfaceResolver, NotFoundException } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { Policy } from "../engine/entities/Policy.ts";

import {
  ParamsInterface as OperatorParamsInterface,
  ResultInterface as OperatorResultInterface,
  signature as operatorFindSignature,
} from "../../operator/contracts/find.contract.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/find.contract.ts";
import { alias } from "../contracts/find.schema.ts";
import { PolicyRepositoryProviderInterfaceResolver } from "../interfaces/index.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      territory: "territory.policy.find",
      registry: "registry.policy.find",
      operator: "operator.policy.find",
    }),
    ["validate", alias],
  ],
  apiRoute: {
    path: "/campaign/find",
    method: "POST",
  },
})
export class FindAction extends AbstractAction {
  constructor(
    private kernel: KernelInterfaceResolver,
    private policyRepository: PolicyRepositoryProviderInterfaceResolver,
  ) {
    super();
  }

  public async handle(params: ParamsInterface): Promise<ResultInterface> {
    const policyData = await this.policyRepository.find(
      params._id,
      params.territory_id,
    );

    if (!policyData) {
      throw new NotFoundException(`policy #${params._id} not found`);
    }
    const policy = await Policy.import(policyData);

    const result: ResultInterface = {
      ...policy.export(),
      description: policy.describe(),
      params: policy.params(),
    };

    if (!params.operator_id) {
      return result;
    }

    const operator: OperatorResultInterface = await this.kernel.call<
      OperatorParamsInterface,
      OperatorResultInterface
    >(
      operatorFindSignature,
      { _id: params.operator_id },
      {
        channel: { service: handlerConfig.service },
        call: { user: { permissions: ["registry.operator.find"] } },
      },
    );

    if (
      !(result.params?.allTimeOperators ?? result.params?.operators)?.includes(
        operator.uuid,
      )
    ) {
      throw new ForbiddenException(
        `Operator ${operator.uuid} is not allowed to access policy #${params._id}`,
      );
    }
    return result;
  }
}
