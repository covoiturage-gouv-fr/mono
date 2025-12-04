import { Infer, object, optional } from "@/lib/superstruct/index.ts";
import { Email, Id, NullableId, Role, Varchar } from "@/pdc/providers/superstruct/shared/index.ts";

export const Users = object({
  id: optional(Id),
  territory_id: optional(Id),
  operator_id: optional(Id),
  search: optional(Varchar),
  page: optional(Id),
  limit: optional(Id),
});

export const CreateUser = object({
  firstname: Varchar,
  lastname: Varchar,
  email: Email,
  role: Role,
  operator_id: optional(NullableId),
  territory_id: optional(NullableId),
});

export const DeleteUser = object({
  id: Id,
});

export const UpdateUser = object({
  id: Id,
  firstname: Varchar,
  lastname: Varchar,
  email: Email,
  role: Role,
  operator_id: optional(NullableId),
  territory_id: optional(NullableId),
});

export type Users = Infer<typeof Users>;
export type DeleteUser = Infer<typeof DeleteUser>;
export type CreateUser = Infer<typeof CreateUser>;
export type UpdateUser = Infer<typeof UpdateUser>;
