import type { ResultInterface as CreateOperatorResultInterface } from "@/pdc/services/dashboard/actions/operators/CreateOperatorAction.ts";
import type { ResultInterface as DeleteOperatorResultInterface } from "@/pdc/services/dashboard/actions/operators/DeleteOperatorAction.ts";
import type { OperatorResult, ResultInterface as OperatorsResultInterface } from "@/pdc/services/dashboard/actions/operators/OperatorsAction.ts";
import type { ResultInterface as UpdateOperatorResultInterface } from "@/pdc/services/dashboard/actions/operators/UpdateOperatorAction.ts";
import {
  CreateOperator as CreateOperatorDataInterface,
  DeleteOperator as DeleteOperatorParamsInterface,
  Operators as OperatorsParamsInterface,
  UpdateOperator as UpdateOperatorDataInterface,
} from "@/pdc/services/dashboard/dto/Operators.ts";

export type {
  CreateOperatorDataInterface,
  CreateOperatorResultInterface,
  DeleteOperatorParamsInterface,
  DeleteOperatorResultInterface,
  OperatorsParamsInterface,
  OperatorsResultInterface,
  UpdateOperatorDataInterface,
  UpdateOperatorResultInterface,
};

export interface OperatorsRepositoryInterface {
  getOperators(params: OperatorsParamsInterface): Promise<OperatorsResultInterface>;
  getOperatorById(id: number): Promise<OperatorResult | null>;
  deleteOperator(params: DeleteOperatorParamsInterface): Promise<DeleteOperatorResultInterface>;
  updateOperator(params: UpdateOperatorDataInterface): Promise<UpdateOperatorResultInterface>;
  createOperator(params: CreateOperatorDataInterface): Promise<CreateOperatorResultInterface>;
}

export abstract class OperatorsRepositoryInterfaceResolver implements OperatorsRepositoryInterface {
  async getOperators(params: OperatorsParamsInterface): Promise<OperatorsResultInterface> {
    throw new Error();
  }
  async getOperatorById(id: number): Promise<OperatorResult | null> {
    throw new Error();
  }
  async deleteOperator(params: DeleteOperatorParamsInterface): Promise<DeleteOperatorResultInterface> {
    throw new Error();
  }
  async updateOperator(params: UpdateOperatorDataInterface): Promise<UpdateOperatorResultInterface> {
    throw new Error();
  }
  async createOperator(params: CreateOperatorDataInterface): Promise<CreateOperatorResultInterface> {
    throw new Error();
  }
}
