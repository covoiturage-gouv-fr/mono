import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { hasPermissionMiddleware } from "@/pdc/providers/middleware/middlewares.ts";
import { OperatorResult } from "@/pdc/services/dashboard/actions/operators/OperatorsAction.ts";
import { Operators } from "@/pdc/services/dashboard/dto/Operators.ts";
import { OperatorsRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/OperatorsRepositoryInterface.ts";
export type ResultInterface = {
  meta: {
    page: number;
    total: number;
    totalPages: number;
  };
  data: OperatorResult[];
};

@handler({
  service: "dashboard",
  // Méthode distincte : OperatorsAction déclarait déjà `dashboard:operators`, et le registre
  // garde silencieusement le premier handler enregistré — celui-ci ne servait jamais.
  method: "operator",
  middlewares: [
    ["validate", Operators],
    hasPermissionMiddleware("common.operator.list"),
  ],
  apiRoute: {
    path: "/dashboard/operator/:id",
    action: "dashboard:operator",
    method: "GET",
  },
})
export class OperatorAction extends AbstractAction {
  constructor(private repository: OperatorsRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(params: Operators): Promise<ResultInterface> {
    return this.repository.getOperators(params);
  }
}
