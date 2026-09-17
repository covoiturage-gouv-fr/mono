import { ConfigInterfaceResolver, InitHookInterface, provider } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { UserRepository } from "@/pdc/services/auth/providers/UserRepository.ts";
import { createRemoteJWKSet } from "dep:jose";
import * as client from "dep:openid-client";
import { getPermissions } from "../config/permissions.ts";

// Niveaux acr ProConnect valant double authentification.
// https://partenaires.proconnect.gouv.fr/docs/fournisseur-service/double_authentification
export const MFA_ACR_VALUES = Object.freeze(["eidas0-mfa", "eidas1-mfa", "eidas2", "eidas3"]);

export class MfaRequiredError extends Error {
  override name = "MfaRequiredError";
}

// Paramètre `claims` de /authorize : exige un acr MFA (ProConnect enrôle l'agent sinon).
export function mfaClaimsParameter(values: readonly string[]): string {
  return JSON.stringify({ id_token: { acr: { essential: true, values } } });
}

// Gate MFA fail-closed : l'acr rendu par ProConnect doit figurer dans les niveaux MFA.
export function failsMfaCheck(acr: unknown, values: readonly string[]): boolean {
  return typeof acr !== "string" || !values.includes(acr);
}

// Gate ProConnect fail-closed : SIREN(ProConnect) doit égaler login_siren (registry.admin bypass).
export function failsSirenCheck(
  proconnect: { siren: string },
  user: { login_siren?: string | null; role: string },
): boolean {
  if (user.role === "registry.admin") return false; // admin : bypass du gate
  const loginSiren = user.login_siren?.trim();
  if (!loginSiren) return true; // fail-closed explicite : pas de SIREN de connexion, jamais de String(null)
  return proconnect.siren.substring(0, 9) !== loginSiren;
}

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

    try {
      await this.getConfig();
    } catch (e) {
      // Au démarrage (fail=false) : un ProConnect injoignable ne doit pas empêcher l'API de
      // démarrer. La découverte est rejouée à la première connexion, où l'échec reste bloquant.
      if (fail) throw e;
      logger.warn(`[proconnect] découverte impossible au démarrage : ${(e as Error).message}`);
    }
  }

  async init(): Promise<void> {
    if (this.#enabled && !this.config.get("proconnect.require_mfa")) {
      logger.warn("[proconnect] double authentification non exigée. Use PROCONNECT_REQUIRE_MFA=true to enforce");
    }

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

    if (this.config.get("proconnect.require_mfa")) {
      parameters.claims = mfaClaimsParameter(MFA_ACR_VALUES);
    }

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

    // La demande `claims` ne garantit rien : on revérifie l'acr rendu.
    if (this.config.get("proconnect.require_mfa")) {
      const acr = tokens.claims()?.acr;
      if (failsMfaCheck(acr, MFA_ACR_VALUES)) {
        throw new MfaRequiredError(`[proconnect] double authentification requise (acr=${acr})`);
      }
    }

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
      if (failsSirenCheck({ siren: siret.substring(0, 9) }, user)) {
        throw new Error(`SIRET check failed: ${email} / ${siret}`);
      }

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
}
