import { array, Infer, object, optional } from "@/lib/superstruct/index.ts";
import { boolean, nullable, pattern, string } from "@/lib/superstruct/index.ts";
import { Email, Id, NullableId, Role, Varchar } from "@/pdc/providers/superstruct/shared/index.ts";

// SIREN de connexion ProConnect : 9 chiffres, distinct du SIRET du territoire.
export const LoginSiren = nullable(pattern(string(), /^\d{9}$/));

// Périmètre soumis par le formulaire. scopes[] est la liste complète et fait autorité ;
// territory_id ne sert de repli que si scopes est absent (appels API hors dashboard).
export const UserScopeInput = object({
  territory_id: Id,
  is_default: optional(boolean()),
});

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
  scopes: optional(array(UserScopeInput)),
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
  scopes: optional(array(UserScopeInput)),
});

export type UserScopeInput = Infer<typeof UserScopeInput>;
export type Users = Infer<typeof Users>;
export type DeleteUser = Infer<typeof DeleteUser>;
export type CreateUser = Infer<typeof CreateUser>;
export type UpdateUser = Infer<typeof UpdateUser>;
