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

export type InactiveUserToWarn = {
  _id: number;
  email: string;
  firstname: string | null;
  lastname: string | null;
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

  findUsersToWarn(inactivity: string): Promise<InactiveUserToWarn[]>;
  markUserWarned(id: number): Promise<void>;
  findUsersToDelete(grace: string): Promise<Array<{ _id: number }>>;
  deleteInactiveUsers(grace: string): Promise<Array<{ _id: number }>>;
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

  async findUsersToWarn(_inactivity: string): Promise<InactiveUserToWarn[]> {
    throw new Error();
  }

  async markUserWarned(_id: number): Promise<void> {
    throw new Error();
  }

  async findUsersToDelete(_grace: string): Promise<Array<{ _id: number }>> {
    throw new Error();
  }

  async deleteInactiveUsers(_grace: string): Promise<Array<{ _id: number }>> {
    throw new Error();
  }
}
