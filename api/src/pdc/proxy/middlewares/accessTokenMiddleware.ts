import { KernelInterface, UnauthorizedException } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { get } from "@/lib/object/index.ts";
import { getPermissions } from "@/pdc/services/auth/config/permissions.ts";
import { DexOIDCProvider } from "@/pdc/services/auth/providers/DexOIDCProvider.ts";
import { NextFunction, Request as ExpressRequest, Response } from "dep:express";

function getTokenFromRequest(req: ExpressRequest): string {
  const authHeader = get(req, "headers.authorization", "");
  if (!authHeader) {
    throw new UnauthorizedException("No token provided");
  }
  const token = String(authHeader).replace("Bearer ", "");
  if (!token.length) {
    throw new UnauthorizedException("Empty token provided");
  }
  return token;
}

/**
 * Returns an Express middleware that checks the token using DexOIDCProvider.
 * Accepts either a KernelInterface or a DexOIDCProvider instance as argument.
 */
export function dexAccessTokenMiddleware(kernel: KernelInterface) {
  return async (req: ExpressRequest, _res: Response, next: NextFunction): Promise<void> => {
    try {
      const token = getTokenFromRequest(req);
      const dexProvider = kernel.get(DexOIDCProvider);

      const data = await dexProvider.verifyToken(token);

      req.stateless = {};

      req.stateless.user = {
        operator_id: data.operator_id,
        role: data.role,
        email: data.token_id,
        permissions: getPermissions(data.role),
      };

      next();
    } catch (e) {
      // skip the unsupported algorithm message falling back to legacy token middleware
      // to avoid bloating the logs.
      if (Error.isError(e) && !e.message.includes("value for a JSON Web Key Set")) {
        logger.warn(`[dexMiddleware] ${e.message}`);
      }

      next(e);
    }
  };
}

/**
 * Check the token using DexOIDCProvider and
 * fallback to legacy serverTokenMiddleware if it fails.
 */
export function accessTokenMiddleware(kernel: KernelInterface) {
  return async (req: ExpressRequest, _res: Response, next: NextFunction): Promise<void> => {
    dexAccessTokenMiddleware(kernel)(req, _res, async (err: Error | null) => {
      if (typeof err === "undefined" || err === null) {
        // If no error, continue to the next middleware
        return next();
      }

      if (Error.isError(err)) return next(err);
      if (typeof err === "string") {
        return next(new Error(err));
      }

      next();
    });
  };
}
