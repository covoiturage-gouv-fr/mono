import { ConfigInterfaceResolver, InitHookInterface, provider, UnauthorizedException } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { encodeBase64 } from "dep:encoding";
import { createRemoteJWKSet, jwtVerify } from "dep:jose";
import { JWTExpired } from "https://deno.land/x/jose@v5.6.3/util/errors.ts";

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

      const [role, operator_id] = (payload.name || "").split(":");

      return {
        operator_id: parseInt(operator_id),
        role,
        token_id: payload.email,
      };
    } catch (err: unknown) {
      if (err instanceof JWTExpired) {
        throw new UnauthorizedException(err.message);
      }

      throw err;
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
