import { ConfigInterfaceResolver, InitHookInterface, provider } from "@/ilos/common/index.ts";
import { UserRepository } from "@/pdc/services/auth/providers/UserRepository.ts";
import { createRemoteJWKSet } from "dep:jose";
import * as client from "dep:openid-client";
import { getPermissions } from "../config/permissions.ts";
import { errorCode, type Masked, maskEmail, maskSiren, proconnectLog } from "./proconnectLog.ts";

// Niveaux acr ProConnect valant double authentification.
// https://partenaires.proconnect.gouv.fr/docs/fournisseur-service/double_authentification
export const MFA_ACR_VALUES = Object.freeze(["eidas0-mfa", "eidas1-mfa", "eidas2", "eidas3"]);

// idp_id (fournisseur d'identité utilisé) : lien avec le support ProConnect dans les logs [proconnect].
export const PROCONNECT_SCOPE = "openid email siret given_name usual_name idp_id";

export class MfaRequiredError extends Error {
  override name = "MfaRequiredError";
  constructor(message: string, public readonly acr?: string) {
    super(message);
  }
}

// Paramètre `claims` de /authorize : exige un acr MFA (ProConnect enrôle l'agent sinon).
export function mfaClaimsParameter(values: readonly string[]): string {
  return JSON.stringify({ id_token: { acr: { essential: true, values } } });
}

// Gate MFA fail-closed : l'acr rendu par ProConnect doit figurer dans les niveaux MFA.
export function failsMfaCheck(acr: unknown, values: readonly string[]): boolean {
  return typeof acr !== "string" || !values.includes(acr);
}

export type LocalUserRefusal =
  | "not_found"
  | "anonymous_role"
  | "no_login_siren"
  | "siren_mismatch"
  | "missing_claims"
  | "db_error";

type LoginIdentifiers = { login_siret?: string | null; login_siren?: string | null; role: string };

// Gate ProConnect fail-closed : SIREN(ProConnect) doit égaler les 9 premiers chiffres de login_siret,
// à défaut login_siren (comptes non migrés). registry.admin bypass.
export function sirenRefusal(
  proconnect: { siren: string },
  user: LoginIdentifiers,
): "no_login_siren" | "siren_mismatch" | null {
  if (user.role === "registry.admin") return null; // admin : bypass du gate
  const loginSiren = (user.login_siret?.trim() || user.login_siren?.trim())?.substring(0, 9);
  if (!loginSiren) return "no_login_siren"; // fail-closed explicite : pas d'identifiant de connexion, jamais de String(null)
  return proconnect.siren.substring(0, 9) !== loginSiren ? "siren_mismatch" : null;
}

export function failsSirenCheck(
  proconnect: { siren: string },
  user: LoginIdentifiers,
): boolean {
  return sirenRefusal(proconnect, user) !== null;
}

type AuthenticatedUser = NonNullable<Awaited<ReturnType<UserRepository["authenticateByEmail"]>>>;
export type ProConnectLocalUser = AuthenticatedUser & { name?: string; permissions: string[] };
export type LocalUserResult =
  | { user: ProConnectLocalUser }
  | { user: null; reason: LocalUserRefusal; error?: string };
export type ProConnectUserInfo = {
  user: ProConnectLocalUser | null;
  reason?: LocalUserRefusal;
  error?: string;
  idp_id?: string;
  email: Masked;
  siren: Masked;
  has_email: boolean;
  has_siret: boolean;
};

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

      proconnectLog.warn("disabled");
      return;
    }

    const start = performance.now();
    try {
      await this.getConfig();
      proconnectLog.info("discovery ok", { ms: Math.round(performance.now() - start) });
    } catch (e) {
      proconnectLog.warn("discovery failed", { ms: Math.round(performance.now() - start), error: errorCode(e) });
      // Au démarrage (fail=false) : un ProConnect injoignable ne doit pas empêcher l'API de
      // démarrer. La découverte est rejouée à la première connexion, où l'échec reste bloquant.
      if (fail) throw e;
    }
  }

  async init(): Promise<void> {
    if (this.#enabled && !this.config.get("proconnect.require_mfa")) {
      proconnectLog.warn("mfa not enforced");
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
    let state = client.randomState();
    const nonce = client.randomNonce();

    const parameters: Record<string, string> = {
      redirect_uri,
      scope: PROCONNECT_SCOPE,
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
        throw new MfaRequiredError(
          `[proconnect] double authentification requise (acr=${acr})`,
          typeof acr === "string" ? acr : undefined,
        );
      }
    }

    return tokens;
  }

  async getUserInfo(accessToken: string, sub: string): Promise<ProConnectUserInfo> {
    await this.#configure();

    const userInfo = await client.fetchUserInfo(this.clientConfig!, accessToken, sub);
    const email = typeof userInfo.email === "string" ? userInfo.email : "";
    const siret = typeof userInfo.siret === "string" ? userInfo.siret : "";
    const trace = {
      email: maskEmail(email),
      siren: maskSiren(siret),
      has_email: !!email,
      has_siret: !!siret,
      ...(typeof userInfo.idp_id === "string" ? { idp_id: userInfo.idp_id } : {}),
    };

    if (!email || !siret) return { ...trace, user: null, reason: "missing_claims" };

    const res = await this.getLocalUser(email, siret, userInfo.given_name, userInfo.usual_name as string | undefined);
    return { ...trace, ...res };
  }

  protected async getLocalUser(
    email: string,
    siret: string,
    given_name?: string,
    family_name?: string,
  ): Promise<LocalUserResult> {
    try {
      const user = await this.userRepository.authenticateByEmail(email);
      if (!user) return { user: null, reason: "not_found" };
      // role est un varchar libre : un compte « anonymous » en base ne doit pas ouvrir de session
      if (user.role === "anonymous") return { user: null, reason: "anonymous_role" };

      const reason = sirenRefusal({ siren: siret.substring(0, 9) }, user);
      if (reason) return { user: null, reason };

      return {
        user: {
          ...user,
          ...(given_name || family_name) ? { name: given_name + " " + family_name } : {},
          permissions: getPermissions(user.role),
        },
      };
    } catch (e) {
      return { user: null, reason: "db_error", error: errorCode(e) };
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
