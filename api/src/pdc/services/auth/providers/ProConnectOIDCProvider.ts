import { ConfigInterfaceResolver, InitHookInterface, provider } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { LocalSiretUser, UserRepository } from "@/pdc/services/auth/providers/UserRepository.ts";
import { createRemoteJWKSet } from "dep:jose";
import * as client from "dep:openid-client";
import { getPermissions } from "../config/permissions.ts";

@provider()
export class ProConnectOIDCProvider implements InitHookInterface {
  #enabled = false;
  protected JWKS: ReturnType<typeof createRemoteJWKSet> | undefined;
  protected clientConfig: client.Configuration | undefined;

  constructor(
    protected config: ConfigInterfaceResolver,
    private userRepository: UserRepository,
  ) {
    this.#enabled = this.config.get("proconnect.enabled");
  }

  async #configure(fail = true): Promise<void> {
    if (!this.#enabled) {
      const msg = "[proconnect] is disabled! Use PROCONNECT_ENABLED=true to enable";

      if (fail) {
        throw new Error(msg);
      }

      logger.warn(msg);
      return;
    }

    await this.getConfig();
  }

  async init(): Promise<void> {
    await this.#configure(false);
  }

  protected async getConfig() {
    const clientId = this.config.get("proconnect.client_id");
    const clientSecret = this.config.get("proconnect.client_secret");
    const server = this.config.get("proconnect.base_url");

    this.clientConfig = await client.discovery(
      server,
      clientId,
      clientSecret,
    );
  }

  async getLoginUrl() {
    await this.#configure();

    const redirect_uri = this.config.get("proconnect.redirect_url");
    const scope = "openid email siret given_name usual_name";

    let state = client.randomState();
    const nonce = client.randomNonce();

    const parameters: Record<string, string> = {
      redirect_uri,
      scope,
      nonce,
      state,
    };

    if (!this.clientConfig!.serverMetadata().supportsPKCE()) {
      state = client.randomState();
      parameters.state = state;
    }

    const redirectUrl: URL = client.buildAuthorizationUrl(this.clientConfig!, parameters);
    return { redirectUrl, state, nonce };
  }

  async getToken(url: URL, expectedNonce: string, expectedState: string) {
    await this.#configure();

    const tokens = await client.authorizationCodeGrant(this.clientConfig!, url, {
      expectedNonce,
      expectedState,
      idTokenExpected: true,
    });
    return tokens;
  }

  async getUserInfo(accessToken: string, sub: string) {
    await this.#configure();

    const userInfo = await client.fetchUserInfo(this.clientConfig!, accessToken, sub);
    const user = await this.getLocalUser(
      userInfo.email!,
      userInfo.siret! as string,
      userInfo.given_name,
      userInfo.usual_name as string,
    );

    return user;
  }

  protected async getLocalUser(email: string, siret: string, given_name?: string, family_name?: string) {
    try {
      const user = await this.userRepository.authenticateByEmail(email);

      if (!user) throw new Error(`User not found: ${email}`);
      if (this.failsSirenCheck(user, siret)) throw new Error(`SIRET check failed: ${email} / ${siret}`);

      return {
        ...user,
        ...(given_name || family_name) ? { name: given_name + " " + family_name } : {},
        permissions: getPermissions(user.role),
      };
    } catch (e) {
      const m = e instanceof Error ? e.message : e;
      logger.error("[ProConnectOIDCProvider] Error fetching local user:", m);

      return {
        email: email,
        role: "anonymous",
        permissions: [],
        ...(given_name || family_name) ? { name: given_name + " " + family_name } : {},
      };
    }
  }

  async getLogoutUrl(idToken: string) {
    await this.#configure();

    const state = client.randomState();
    const redirect = this.config.get("proconnect.logout_redirect_url");
    const redirectUrl = client.buildEndSessionUrl(this.clientConfig!, {
      post_logout_redirect_uri: redirect,
      id_token_hint: idToken,
      state,
    });
    return { state, redirectUrl };
  }

  private failsSirenCheck(user: LocalSiretUser, siret: string): boolean {
    // admins bypass siret check
    if (user.role === "registry.admin") return false;

    const siren = siret.substring(0, 9);
    const userSiren = String(user.siret).substring(0, 9);
    const fails = siren !== userSiren;

    if (fails) {
      console.warn(`[ProConnectOIDCProvider] SIRET mismatch for user: expected SIREN ${siren}, got ${userSiren}`);
    }

    return fails;
  }
}
