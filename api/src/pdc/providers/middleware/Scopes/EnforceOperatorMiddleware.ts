import { middleware } from "@/ilos/common/Decorators.ts";
import {
  ContextType,
  ForbiddenException,
  InvalidRequestException,
  MiddlewareInterface,
  ParamsType,
  UnauthorizedException,
} from "@/ilos/common/index.ts";
import { get } from "@/lib/object/index.ts";
import { NextFunction } from "dep:express";
import { ConfiguredMiddleware } from "../interfaces.ts";

@middleware()
export class EnforceOperatorMiddleware implements MiddlewareInterface<void> {
  async process(
    params: ParamsType,
    context: ContextType,
    next: NextFunction,
  ): Promise<void> {
    // Sentinelle unique : comparer à `Symbol("...")` crée un nouveau symbole à chaque appel,
    // l'égalité était donc toujours fausse et les trois gardes ci-dessous ne s'exécutaient jamais.
    const NOT_FOUND = Symbol("not found");
    const role = get(context, "call.user.role", NOT_FOUND);
    const context_id = get(context, "call.user.operator_id", NOT_FOUND);
    const params_id = get(params, "operator_id", NOT_FOUND);

    if (role === NOT_FOUND) {
      throw new UnauthorizedException("User role is required");
    }

    // If the user is a registry admin, we don't need to enforce an operator ID
    if (typeof role === "string" && role === "registry.admin") {
      if (params_id === NOT_FOUND) {
        throw new InvalidRequestException("Operator ID is required in the request parameters");
      }

      return next(params, context);
    }

    if (context_id === NOT_FOUND) {
      throw new UnauthorizedException("Operator ID is required in the session context");
    }

    if (typeof context_id === "number" && Number(params.operator_id) !== context_id) {
      throw new ForbiddenException(`Operator ID mismatch: expected ${String(context_id)}, got ${params.operator_id}`);
    }

    return next(params, context);
  }
}

const alias = "enforce.operator";
export const enforceOperatorMiddlewareBinding = [alias, EnforceOperatorMiddleware];
export function enforceOperatorMiddleware(): ConfiguredMiddleware<void> {
  return [alias, undefined];
}
