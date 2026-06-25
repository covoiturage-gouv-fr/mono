import { ConfigInterfaceResolver, InitHookInterface, provider, UnauthorizedException } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { encodeBase64 } from "dep:encoding";
import { createRemoteJWKSet, errors, jwtVerify } from "dep:jose";

const { JOSEError, JWKSTimeout } = errors;

/**
 * Translate a token-verification failure into the right HTTP semantics:
 * - a JWKS timeout means OUR auth backend (Dex) is unreachable → keep it as-is
 *   so it surfaces as a 500 (server error, retryable);
 * - any other jose error means the client's token cannot be trusted → 401;
 * - anything else (non-jose) is left untouched.
 */
export function toVerificationError(err: unknown): unknown {
  if (err instanceof JWKSTimeout) {
    return err;
  }
  if (err instanceof JOSEError) {
    return new UnauthorizedException(err.message);
  }
  return err;
}

/**
 * Parse the Dex token subject, expected as "<role>:<operator_id>"
 * (e.g. "operator.admin:1", "registry.admin:0").
 *
 * A validly-signed token whose subject does not match this shape cannot be
 * trusted to identify an operator → reject as 401 rather than silently
 * producing an operator_id of NaN.
 */
export function parseTokenSubject(name: string | undefined): { role: string; operator_id: number } {
  const [role, operatorId, ...rest] = (name ?? "").split(":");
  if (!role || rest.length > 0 || !/^\d+$/.test(operatorId ?? "")) {
    throw new UnauthorizedException("Malformed token subject");
  }
  return { role, operator_id: Number(operatorId) };
}

@provider()
export class DexOIDCProvider implements InitHookInterface {
  protected JWKS: ReturnType<typeof createRemoteJWKSet> | undefined;

  constructor(protected config: ConfigInterfaceResolver) {}

  async init(): Promise<void> {
    this.getJWKS();
  }

  protected getJWKS() {
    if (this.JWKS) {
      return this.JWKS;
    }
    const authBaseUrl = this.config.get("dex.base_url");
    this.JWKS = createRemoteJWKSet(new URL(`${authBaseUrl}/keys`));
    return this.JWKS;
  }

  async verifyToken(token: string) {
    try {
      const clientId = this.config.get("dex.client_id");
      const authBaseUrl = this.config.get("dex.base_url");
      const { payload } = await jwtVerify<{ name: string; email: string }>(token, this.getJWKS(), {
        issuer: authBaseUrl,
        audience: clientId,
      });

      const { role, operator_id } = parseTokenSubject(payload.name);

      return {
        operator_id,
        role,
        token_id: payload.email,
      };
    } catch (err: unknown) {
      // Surface why verification failed (signature, issuer, expiry, ...) for
      // support without leaking it to the client, then translate the error.
      if (err instanceof JOSEError && !(err instanceof JWKSTimeout)) {
        logger.warn(`[DexOIDCProvider:verifyToken] ${err.code} ${err.message}`);
      }

      throw toVerificationError(err);
    }
  }

  async getToken(access_key: string, secret_key: string): Promise<string> {
    const clientId = this.config.get("dex.client_id");
    const clientSecret = this.config.get("dex.client_secret");
    const authBaseUrl = this.config.get("dex.base_url");

    const url = new URL("/token", authBaseUrl);
    const form = new URLSearchParams();

    // DEX calls these "username" and "password"...
    form.set("username", access_key);
    form.set("password", secret_key);
    form.set("grant_type", "password");
    form.set("scope", "openid email profile");

    const response = await fetch(url, {
      body: form.toString(),
      method: "POST",
      headers: {
        "Content-type": "application/x-www-form-urlencoded",
        "Authorization": `Basic ${encodeBase64(`${clientId}:${clientSecret}`)}`,
      },
    });

    if (!response.ok) {
      logger.error(`[OIDCProvider:getToken] ${response.status} ${response.statusText}`);
      throw new Error(response.statusText);
    }

    const json = await response.json();
    return json.access_token;
  }
}
