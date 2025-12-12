import { ConfigInterfaceResolver, inject, injectable, KernelInterfaceResolver, proxy } from "@/ilos/common/index.ts";
import { asyncHandler } from "@/pdc/proxy/helpers/asyncHandler.ts";
import { ProConnectOIDCProvider } from "@/pdc/services/auth/providers/ProConnectOIDCProvider.ts";
import express, { NextFunction, Request, Response } from "dep:express";
import { session } from "../../../config/proxy.ts";
import { authGuard } from "../../proxy/middlewares/authGuard.ts";
import { sessionMiddleware } from "../../proxy/middlewares/sessionMiddleware.ts";
import { testLoginRoute } from "./test/login.ts";

@injectable()
export class AuthRouter {
  constructor(
    @inject(proxy) private app: express.Express,
    private kernel: KernelInterfaceResolver,
    private proConnectOIDCProvider: ProConnectOIDCProvider,
    private config: ConfigInterfaceResolver,
  ) {
  }

  register() {
    this.app.get(
      "/auth/login",
      asyncHandler(async (req: Request, res: Response, _next: NextFunction) => {
        const { redirectUrl, state, nonce } = await this.proConnectOIDCProvider.getLoginUrl();
        req.session = req.session || {};
        req.session.auth = {
          state,
          nonce,
        };

        return res.redirect(redirectUrl);
      }),
    );

    this.app.get(
      "/auth/login/callback",
      asyncHandler(async (req: Request, res: Response) => {
        const url = new URL(`${req.protocol}://${req.get("host")}${req.originalUrl}`);
        const { state, nonce } = req.session?.auth || {};

        // Fetch tokens and user info from ProConnect OIDC Provider
        const tokens = await this.proConnectOIDCProvider.getToken(url, nonce, state);
        const claims = tokens.claims();
        const user = await this.proConnectOIDCProvider.getUserInfo(tokens.access_token, claims!.sub);

        // Store user and token information in the session
        req.session = req.session || {};
        req.session.auth = {};
        req.session.auth.id_token = tokens.id_token;
        req.session.user = user;

        return res.redirect(this.config.get("app_url"));
      }),
    );

    this.app.get(
      "/auth/logout",
      authGuard(this.kernel),
      asyncHandler(async (req: Request, res: Response, _next: NextFunction) => {
        const { id_token } = req.session?.auth || {};
        const { redirectUrl } = await this.proConnectOIDCProvider.getLogoutUrl(id_token);
        req.session.destroy((err: Error) => {
          if (err) {
            console.error("Failed to destroy session during logout:", err);
          }
          res.clearCookie(session.name);
          return res.redirect(redirectUrl);
        });
        return res.redirect(redirectUrl);
      }),
    );

    this.app.get(
      "/auth/logout/callback",
      asyncHandler(async (req: Request, res: Response) => {
        const { state: expectedState } = req.session?.auth || {};
        const state = req.query?.state;
        if (state === expectedState) {
          req.session.destroy((err: Error) => {
            if (err) {
              throw err;
            }
          });
        }
        return res.redirect(this.config.get("app_url"));
      }),
    );

    this.app.get(
      "/auth/me",
      sessionMiddleware(this.kernel),
      (req: express.Request, res: express.Response) => {
        if (!req.session?.user) {
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

        return res.json(req.session?.user);
      },
    );

    /**
     * Test-only login route to create a session without going through OIDC.
     * This route should only be available in test environments.
     */
    if (["demo", "production"].includes(this.config.get("env")) === false) {
      this.app.post("/auth/test/login", testLoginRoute.bind(this)(this.config));
    }
  }
}
