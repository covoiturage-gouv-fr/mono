import {
  CreateUser as CreateUserDataInterface,
  DeleteUser as DeleteUserParamsInterface,
  UpdateUser as UpdateUserDataInterface,
  Users as UsersParamsInterface,
} from "@/pdc/services/dashboard/dto/Users.ts";
import type { ResultInterface as CreateUserResultInterface } from "../actions/users/CreateUserAction.ts";
import type { ResultInterface as DeleteUserResultInterface } from "../actions/users/DeleteUserAction.ts";
import type { ResultInterface as UpdateUserResultInterface } from "../actions/users/UpdateUserAction.ts";
import type { ResultInterface as UsersResultInterface } from "../actions/users/UsersAction.ts";

export type {
  CreateUserDataInterface,
  CreateUserResultInterface,
  DeleteUserParamsInterface,
  DeleteUserResultInterface,
  UpdateUserDataInterface,
  UpdateUserResultInterface,
  UsersParamsInterface,
  UsersResultInterface,
};

export interface UsersRepositoryInterface {
  getUsers(
    params: UsersParamsInterface,
  ): Promise<UsersResultInterface>;

  createUser(
    data: CreateUserDataInterface,
  ): Promise<CreateUserResultInterface>;

  deleteUser(
    params: DeleteUserParamsInterface,
  ): Promise<DeleteUserResultInterface>;
  updateUser(
    data: UpdateUserDataInterface,
  ): Promise<UpdateUserResultInterface>;
}

export abstract class UsersRepositoryInterfaceResolver implements UsersRepositoryInterface {
  async getUsers(
    params: UsersParamsInterface,
  ): Promise<UsersResultInterface> {
    throw new Error();
  }

  async createUser(
    data: CreateUserDataInterface,
  ): Promise<CreateUserResultInterface> {
    throw new Error();
  }

  async deleteUser(
    params: DeleteUserParamsInterface,
  ): Promise<DeleteUserResultInterface> {
    throw new Error();
  }

  async updateUser(
    data: UpdateUserDataInterface,
  ): Promise<UpdateUserResultInterface> {
    throw new Error();
  }
}
