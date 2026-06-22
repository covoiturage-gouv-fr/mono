import { KernelInterface, UnauthorizedException } from "@/ilos/common/index.ts";
import { getPermissions } from "@/pdc/services/auth/config/permissions.ts";
import { DexOIDCProvider } from "@/pdc/services/auth/providers/DexOIDCProvider.ts";
import { NextFunction, Request as ExpressRequest, Response } from "dep:express";

function getTokenFromRequest(req: ExpressRequest): string {
  const match = String(req.headers.authorization ?? "").match(/^Bearer\s+(.+)$/i);
  if (!match) {
    throw new UnauthorizedException("No token provided");
  }
  return match[1];
}

/**
 * Verify the Bearer access token issued by Dex and attach the authenticated
 * user to the request.
 *
 * Any failure is forwarded to the error handler: a request whose token cannot
 * be verified is rejected (401), never allowed to continue as anonymous.
 * Token validation semantics (401 vs 500) live in DexOIDCProvider.verifyToken.
 */
export function accessTokenMiddleware(kernel: KernelInterface) {
  return async (req: ExpressRequest, _res: Response, next: NextFunction): Promise<void> => {
    try {
      const token = getTokenFromRequest(req);
      const data = await kernel.get(DexOIDCProvider).verifyToken(token);

      req.stateless = {
        user: {
          operator_id: data.operator_id,
          role: data.role,
          email: data.token_id,
          permissions: getPermissions(data.role),
        },
      };

      next();
    } catch (err) {
      next(err);
    }
  };
}
