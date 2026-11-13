import { handler } from "@/ilos/common/Decorators.ts";
import { NotFoundException } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { enforceOperatorMiddleware, hasPermissionByScopeMiddleware } from "@/pdc/providers/middleware/middlewares.ts";
import { DexClient } from "@/pdc/services/auth/providers/DexClient.ts";
import { CreateCredentialsParams, CreateCredentialsResult } from "../dto/Credentials.ts";
import { OperatorRepository } from "../providers/OperatorRepository.ts";

@handler({
  service: "auth",
  method: "createCredentials",
  middlewares: [
    enforceOperatorMiddleware(),
    hasPermissionByScopeMiddleware("registry.credentials.create", [
      "operator.credentials.create",
      "call.user.operator_id",
      "operator_id",
    ]),
    ["validate", CreateCredentialsParams],
  ],
  apiRoute: {
    path: "/auth/credentials",
    method: "POST",
    successHttpCode: 201,
  },
})
export class CreateCredentialsAction extends AbstractAction {
  constructor(private dexClient: DexClient, private operatorRepository: OperatorRepository) {
    super();
  }

  protected override async handle(params: CreateCredentialsParams): Promise<CreateCredentialsResult> {
    if (!params.operator_id || !(await this.operatorRepository.exists(params.operator_id, { active: true }))) {
      throw new NotFoundException(`Operator with ID ${params.operator_id} does not exist`);
    }

    return this.dexClient.createForOperator(params.operator_id);
  }
}
