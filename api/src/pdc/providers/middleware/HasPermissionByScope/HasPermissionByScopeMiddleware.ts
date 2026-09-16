import {
  ContextType,
  ForbiddenException,
  InvalidParamsException,
  middleware,
  MiddlewareInterface,
  ParamsType,
  ResultType,
} from "@/ilos/common/index.ts";
import { get } from "@/lib/object/index.ts";
import { NextFunction } from "dep:express";
import { ConfiguredMiddleware } from "../interfaces.ts";

/**
 * Check if user has basePermission, if not check if contextPath equals paramsPath and challenge
 * against scoped permission.
 * Example :
 * [registry.user.list, [[ 'territory.user.list', 'call.user.territory_id', 'territory_id']]]
 *    ^ base permission       ^ scoped permission    ^ context path            ^ params path
 */
@middleware()
export class HasPermissionByScopeMiddleware implements MiddlewareInterface<HasPermissionByScopeMiddlewareParams> {
  async process(
    params: ParamsType,
    context: ContextType,
    next: NextFunction,
    options: HasPermissionByScopeMiddlewareParams,
  ): Promise<ResultType> {
    const [basePermission, permissionScopes] = options;

    if (!permissionScopes.length) {
      throw new InvalidParamsException("No permissions defined");
    }

    const permissions = get(context, "call.user.permissions", []) || [] as string[];

    if (permissions.length === 0) {
      throw new ForbiddenException("Invalid permissions");
    }

    // If the user has basePermission --> OK
    if (permissions.indexOf(String(basePermission)) > -1) {
      return next(params, context);
    }

    // If the user does not have basePermission, check if the contextPath equals paramsPath
    // and challenge against scoped permission.
    //
    // the param to check is usually a territory_id or operator_id to make sure the user
    // is allowed to access the data for that specific territory or operator.
    for (const [scopedPermission, contextPath, paramsPath] of permissionScopes) {
      if (
        this.paramMatchesContext(
          get(params, paramsPath, Symbol()),
          get(context, contextPath, Symbol()),
        ) &&
        permissions.indexOf(scopedPermission) > -1
      ) {
        return next(params, context);
      }
    }

    throw new ForbiddenException("Invalid permissions");
  }

  /**
   * Correspondance fail-closed entre le paramètre et la valeur du contexte.
   *
   * Deux pièges corrigés ici : un tableau vide passait (`[].reduce(..., true)`), et deux valeurs
   * absentes se comparaient via `String(Symbol())` — donc « manquant » égalait « manquant », et
   * un paramètre non déclaré par le DTO ouvrait la voie. Rien à comparer = pas de correspondance.
   */
  private paramMatchesContext(value: unknown | unknown[], list: unknown | unknown[]): boolean {
    const missing = (v: unknown) => v === undefined || v === null || typeof v === "symbol";
    const val = Array.isArray(value) ? value : [value];
    const lst = (Array.isArray(list) ? list : [list]).filter((i) => !missing(i));

    if (!val.length || !lst.length || val.some(missing)) return false;

    return val.every((v) => lst.some((i) => String(i) === String(v)));
  }
}

export type ScopeAndPermission = [string, string, string];
export type HasPermissionByScopeMiddlewareParams = [
  string | undefined | null,
  ScopeAndPermission[],
];

const alias = "has_permission.by_scope";

export const hasPermissionByScopeMiddlewareBinding = [
  alias,
  HasPermissionByScopeMiddleware,
];

export function hasPermissionByScopeMiddleware(
  globalPermission: string | null | undefined,
  ...scopes: ScopeAndPermission[]
): ConfiguredMiddleware<HasPermissionByScopeMiddlewareParams> {
  return [alias, [globalPermission, scopes]];
}
