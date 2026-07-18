import {
  ContextType,
  ForbiddenException,
  middleware,
  MiddlewareInterface,
  ParamsType,
  ResultType,
} from "@/ilos/common/index.ts";
import { get } from "@/lib/object/index.ts";
import { NextFunction } from "dep:express";
import { ConfiguredMiddleware } from "@/pdc/providers/middleware/interfaces.ts";

// Permission de manipulation des champs privilégiés, détenue par registry.admin seul.
export const MANAGE_SCOPES_PERMISSION = "registry.user.manageScopes";

// Rang hiérarchique : plus grand = plus de privilèges (registry.admin au sommet, inconnu = 0 fail-closed).
const ROLE_RANK: Record<string, number> = {
  "operator.user": 1,
  "territory.user": 1,
  "registry.user": 3,
  "operator.admin": 2,
  "territory.admin": 2,
  "registry.admin": 4,
};

export function roleRank(role: string): number {
  return ROLE_RANK[role] ?? 0;
}

// Détecte l'octroi d'un scope multi-territoire (tableau de territoires non vide).
function grantsMultiScope(params: ParamsType): boolean {
  const scopes: unknown = get(params, "scopes", undefined);
  return Array.isArray(scopes) && scopes.length > 0;
}

/**
 * Garde les actions create/update user :
 * - champs privilégiés (login_siren, octroi de scope multi-territoire) réservés à MANAGE_SCOPES_PERMISSION ;
 * - interdit d'attribuer un rôle de rang supérieur à celui du caller (anti-escalade).
 */
@middleware()
export class UserScopeGuardMiddleware implements MiddlewareInterface {
  async process(
    params: ParamsType,
    context: ContextType,
    next: NextFunction,
  ): Promise<ResultType> {
    const permissions = (get(context, "call.user.permissions", []) || []) as string[];
    const callerRole = get(context, "call.user.role", "") as string;
    const hasManageScopes = permissions.includes(MANAGE_SCOPES_PERMISSION);

    // Gate champ privilégié : login_siren / octroi multi-territoire sans la permission → refus.
    const asksPrivileged = get(params, "login_siren", null) != null || grantsMultiScope(params);
    if (asksPrivileged && !hasManageScopes) {
      throw new ForbiddenException("login_siren / octroi de scope réservé à registry.admin");
    }

    // Garde hiérarchie : ne pas attribuer un rôle plus privilégié que soi.
    const targetRole = get(params, "role", "") as string;
    if (targetRole && roleRank(targetRole) > roleRank(callerRole)) {
      throw new ForbiddenException("Attribution d'un rôle de rang supérieur interdite");
    }

    return next(params, context);
  }
}

const alias = "dashboard.user_scope_guard";

export const userScopeGuardMiddlewareBinding = [alias, UserScopeGuardMiddleware];

export function userScopeGuardMiddleware(): ConfiguredMiddleware<undefined> {
  return [alias, undefined];
}
