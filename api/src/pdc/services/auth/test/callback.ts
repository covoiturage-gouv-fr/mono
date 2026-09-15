import { ConfigInterfaceResolver } from "@/ilos/common/index.ts";
import { safeCompare } from "@/lib/crypto/safeCompare.ts";
import { asyncHandler } from "@/pdc/proxy/helpers/asyncHandler.ts";
import { UserRepository } from "@/pdc/services/auth/providers/UserRepository.ts";
import { Request, Response } from "dep:express";
import { getPermissions } from "../config/permissions.ts";

function rpcError(res: Response, code: number, message: string) {
  return res.status(code).json({ id: 1, jsonrpc: "2.0", error: { code, data: "Error", message } });
}

export const testCallbackRoute = (config: ConfigInterfaceResolver, userRepository: UserRepository) =>
  asyncHandler(async (req: Request, res: Response) => {
    const { email, password } = req.body ?? {};
    if (!email || !password) return rpcError(res, 400, "Bad Request");

    const expected = config.get("test.accounts")().get(email);
    if (!expected || !safeCompare(password, expected)) return rpcError(res, 401, "Unauthorized Error");

    // Même chargement que ProConnect (sans le gate SIREN) : la session de test porte
    // les vrais rôle et périmètres, seule façon de tester le multi-périmètre de bout en bout.
    const local = await userRepository.authenticateByEmail(email);
    if (!local) return rpcError(res, 401, "Unauthorized Error");

    const user = {
      ...local,
      name: `Test ${local.role} user`,
      permissions: getPermissions(local.role),
    };

    await new Promise<void>((resolve, reject) =>
      req.session.regenerate((err?: Error) => (err ? reject(err) : resolve()))
    );
    req.session.auth = { id_token: 1, test_login: true };
    req.session.user = user;

    return res.json(user);
  });
