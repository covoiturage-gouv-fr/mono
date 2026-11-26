import { array, Infer, object, optional } from "@/lib/superstruct/index.ts";
import { boolean, nullable, pattern, string } from "@/lib/superstruct/index.ts";
import { Email, Id, NullableId, PlainText, Role, Varchar } from "@/pdc/providers/superstruct/shared/index.ts";

// SIRET de connexion ProConnect : 14 chiffres ; le contrôle de connexion ne compare que les 9 premiers (SIREN).
export const LoginSiret = nullable(pattern(string(), /^\d{14}$/));

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
  firstname: PlainText,
  lastname: PlainText,
  email: Email,
  role: Role,
  operator_id: optional(NullableId),
  territory_id: optional(NullableId),
  login_siret: optional(LoginSiret),
  scopes: optional(array(UserScopeInput)),
});

export const DeleteUser = object({
  id: Id,
});

export const UpdateUser = object({
  id: Id,
  firstname: PlainText,
  lastname: PlainText,
  email: Email,
  role: Role,
  operator_id: optional(NullableId),
  territory_id: optional(NullableId),
  login_siret: optional(LoginSiret),
  scopes: optional(array(UserScopeInput)),
});

export type UserScopeInput = Infer<typeof UserScopeInput>;
export type Users = Infer<typeof Users>;
export type DeleteUser = Infer<typeof DeleteUser>;
export type CreateUser = Infer<typeof CreateUser>;
export type UpdateUser = Infer<typeof UpdateUser>;
