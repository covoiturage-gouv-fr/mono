import { ConfigInterfaceResolver, inject, injectable, KernelInterfaceResolver, proxy } from "@/ilos/common/index.ts";
import { env_or_default } from "@/lib/env/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { asyncHandler } from "@/pdc/proxy/helpers/asyncHandler.ts";
import { MfaRequiredError, ProConnectOIDCProvider } from "@/pdc/services/auth/providers/ProConnectOIDCProvider.ts";
import { UserRepository } from "@/pdc/services/auth/providers/UserRepository.ts";
import { UserScopeRepository } from "@/pdc/services/auth/providers/UserScopeRepository.ts";
import express, { NextFunction, Request, Response } from "dep:express";
import { session } from "../../../config/proxy.ts";
import { authGuard } from "../../proxy/middlewares/authGuard.ts";
import { loginRateLimiter } from "../../proxy/middlewares/rateLimiter.ts";
import { sessionMiddleware } from "../../proxy/middlewares/sessionMiddleware.ts";
import { contextRoute } from "./context.ts";
import { testCallbackRoute } from "./test/callback.ts";
import { isTestAuthEnabled } from "./test/enabled.ts";

@injectable()
export class AuthRouter {
  constructor(
    @inject(proxy) private app: express.Express,
    private kernel: KernelInterfaceResolver,
    private proConnectOIDCProvider: ProConnectOIDCProvider,
    private config: ConfigInterfaceResolver,
    private userScopeRepository: UserScopeRepository,
    private userRepository: UserRepository,
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
        const url = new URL(req.originalUrl, this.config.get("proxy.apiUrl"));
        const { state, nonce } = req.session?.auth || {};

        // Fetch tokens and user info from ProConnect OIDC Provider
        // (état OIDC state/nonce lu ci-dessus, avant la régénération de session)
        let tokens;
        try {
          tokens = await this.proConnectOIDCProvider.getToken(url, nonce, state);
        } catch (e) {
          // name en second critère : instanceof casse si le module est résolu deux fois
          if (!(e instanceof MfaRequiredError) && (e as Error)?.name !== "MfaRequiredError") throw e;
          logger.warn(`[auth] ${(e as Error).message}`);

          // pas d'identité attachée, mais on jette l'état OIDC de la tentative échouée
          await new Promise<void>((resolve) => req.session ? req.session.destroy(() => resolve()) : resolve());
          res.clearCookie(session.name);

          return res.redirect(`${this.config.get("app_url")}?error=mfa_required`);
        }

        const claims = tokens.claims();
        const user = await this.proConnectOIDCProvider.getUserInfo(tokens.access_token, claims!.sub);

        // Anti-fixation : régénère la session avant d'attacher l'utilisateur authentifié.
        await new Promise<void>((resolve, reject) =>
          req.session.regenerate((err: Error) => err ? reject(err) : resolve())
        );

        // Store user and token information in the fresh session
        req.session.auth = { id_token: tokens.id_token };
        req.session.user = user;
        await new Promise<void>((resolve, reject) => req.session.save((err: Error) => err ? reject(err) : resolve()));

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
            logger.error("Failed to destroy session during logout:", err);
          }
          res.clearCookie(session.name);
          res.redirect(redirectUrl);
        });
      }),
    );

    this.app.get(
      "/auth/logout/callback",
      asyncHandler(async (req: Request, res: Response) => {
        // Session is already destroyed by /auth/logout; state cannot be checked here
        req.session.destroy((err: Error) => {
          if (err) {
            logger.error(`[auth] failed to destroy session on logout callback: ${err.message}`);
          }
          res.clearCookie(session.name);
          res.redirect(this.config.get("app_url"));
        });
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

    // Bascule du contexte actif (users territoire) — revalidée en DB, cf. spec §6.
    this.app.post("/auth/context", contextRoute(this.userScopeRepository));

    // Test-only login, opt-in via APP_ENABLE_TEST_AUTH and never in demo/production
    const envs = [this.config.get("env"), env_or_default("APP_ENV", "local")];
    if (isTestAuthEnabled(envs, this.config.get("test.enabled"))) {
      this.config.get("test.accounts")(); // fail fast at boot if APIE2E_AUTH_* are missing
      logger.warn("[auth] test login route /auth/test/callback is ENABLED");
      this.app.post("/auth/test/callback", loginRateLimiter(), testCallbackRoute(this.config, this.userRepository));
    }
  }
}
