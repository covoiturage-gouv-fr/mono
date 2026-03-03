import { ConfigInterfaceResolver } from "@/ilos/common/index.ts";
import { asyncHandler } from "@/pdc/proxy/helpers/asyncHandler.ts";
import { Request, Response } from "dep:express";
import { getPermissions } from "../config/permissions.ts";

export const testCallbackRoute = (config: ConfigInterfaceResolver) =>
  asyncHandler(async (req: Request, res: Response) => {
    const { email } = req.body;

    if (!email || !req.body.password) {
      return res.status(400).json({
        id: 1,
        jsonrpc: "2.0",
        error: {
          code: 400,
          data: "Error",
          message: "Bad Request",
        },
      });
    }

    const password = config.get("test.accounts").get(email);

    if (!password || req.body.password !== password) {
      return res.status(401).json({
        id: 1,
        jsonrpc: "2.0",
        error: {
          code: 401,
          data: "Error",
          message: "Unauthorized Error",
        },
      });
    }

    // Create a mock user based on the email
    const kind = email.includes("admin") ? "registry" : email.includes("operator") ? "operator" : "territory";
    const user = {
      email,
      name: `Test ${kind} user`,
      role: `${kind}.admin`,
      permissions: getPermissions(`${kind}.admin`),
    };

    if (kind === "operator") {
      // @ts-ignore no-type
      user["operator_id"] = 1;
    }
    if (kind === "territory") {
      // @ts-ignore no-type
      user["territory_id"] = 1;
    }

    // Create session
    req.session = req.session || {};
    req.session.auth = { id_token: 1, test_login: true };
    req.session.user = user;

    return res.json(req.session.user);
  });
