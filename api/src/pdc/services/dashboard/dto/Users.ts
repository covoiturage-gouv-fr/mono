import { array, Infer, object, optional } from "@/lib/superstruct/index.ts";
import { nullable, pattern, string } from "@/lib/superstruct/index.ts";
import { Email, Id, NullableId, Role, Varchar } from "@/pdc/providers/superstruct/shared/index.ts";

// SIREN de connexion ProConnect : 9 chiffres, distinct du SIRET du territoire.
export const LoginSiren = nullable(pattern(string(), /^\d{9}$/));

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
  login_siren: optional(LoginSiren),
  scopes: optional(array(Id)),
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
  login_siren: optional(LoginSiren),
  scopes: optional(array(Id)),
});

export type Users = Infer<typeof Users>;
export type DeleteUser = Infer<typeof DeleteUser>;
export type CreateUser = Infer<typeof CreateUser>;
export type UpdateUser = Infer<typeof UpdateUser>;
