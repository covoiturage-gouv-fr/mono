import { ConfigInterfaceResolver } from "@/ilos/common/index.ts";
import { UserRepository } from "@/pdc/services/auth/providers/UserRepository.ts";
import { assertEquals, assertRejects } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import {
  failsMfaCheck,
  failsSirenCheck,
  MFA_ACR_VALUES,
  mfaClaimsParameter,
  ProConnectOIDCProvider,
} from "./ProConnectOIDCProvider.ts";

describe("ProConnectOIDCProvider.failsSirenCheck", () => {
  it("fails closed when login_siren is null and user is not registry.admin", () => {
    assertEquals(failsSirenCheck({ siren: "123456789" }, { login_siren: null, role: "territory.admin" }), true);
  });

  it("fails closed when login_siren is empty and user is not registry.admin", () => {
    assertEquals(failsSirenCheck({ siren: "123456789" }, { login_siren: "", role: "territory.admin" }), true);
  });

  it("passes for registry.admin regardless of siren", () => {
    assertEquals(failsSirenCheck({ siren: "999999999" }, { login_siren: null, role: "registry.admin" }), false);
  });

  it("passes when proconnect siren matches login_siren", () => {
    assertEquals(failsSirenCheck({ siren: "123456789" }, { login_siren: "123456789", role: "territory.admin" }), false);
  });

  it("fails when proconnect siren differs from login_siren", () => {
    assertEquals(failsSirenCheck({ siren: "123456789" }, { login_siren: "987654321", role: "territory.admin" }), true);
  });
});

describe("ProConnectOIDCProvider MFA", () => {
  it("builds the claims parameter requesting an essential MFA acr", () => {
    assertEquals(
      mfaClaimsParameter(["eidas1-mfa", "eidas2"]),
      '{"id_token":{"acr":{"essential":true,"values":["eidas1-mfa","eidas2"]}}}',
    );
  });

  it("accepts an acr listed in the MFA values", () => {
    assertEquals(failsMfaCheck("eidas1-mfa", MFA_ACR_VALUES), false);
  });

  it("rejects an acr without MFA", () => {
    assertEquals(failsMfaCheck("eidas1", MFA_ACR_VALUES), true);
  });

  it("fails closed when the acr claim is missing", () => {
    assertEquals(failsMfaCheck(undefined, MFA_ACR_VALUES), true);
  });

  it("fails closed when the acr claim is not a string", () => {
    assertEquals(failsMfaCheck(["eidas1-mfa"], MFA_ACR_VALUES), true);
  });

  it("covers every acr level documented as MFA-capable", () => {
    assertEquals(MFA_ACR_VALUES, ["eidas0-mfa", "eidas1-mfa", "eidas2", "eidas3"]);
  });
});

// Découverte OIDC : le démarrage ne doit pas dépendre de la disponibilité de ProConnect.
class DiscoveryProvider extends ProConnectOIDCProvider {
  public calls = 0;

  constructor(config: ConfigInterfaceResolver, private readonly unreachable: boolean) {
    super(config, {} as UserRepository);
  }

  protected override getConfig(): Promise<void> {
    this.calls++;
    return this.unreachable ? Promise.reject(new Error("connect ECONNREFUSED")) : Promise.resolve();
  }
}

function makeConfig(enabled: boolean): ConfigInterfaceResolver {
  const values: Record<string, unknown> = {
    "proconnect.enabled": enabled,
    "proconnect.require_mfa": true,
    "proconnect.redirect_url": "http://localhost:8080/auth/login/callback",
  };
  return { get: (k: string) => values[k] } as unknown as ConfigInterfaceResolver;
}

describe("ProConnectOIDCProvider discovery", () => {
  it("démarre malgré un fournisseur injoignable", async () => {
    const provider = new DiscoveryProvider(makeConfig(true), true);
    await provider.init();
    assertEquals(provider.calls, 1);
  });

  it("tente la découverte au démarrage quand le fournisseur répond", async () => {
    const provider = new DiscoveryProvider(makeConfig(true), false);
    await provider.init();
    assertEquals(provider.calls, 1);
  });

  it("ne tente rien quand ProConnect est désactivé", async () => {
    const provider = new DiscoveryProvider(makeConfig(false), true);
    await provider.init();
    assertEquals(provider.calls, 0);
  });

  // Hors démarrage l'échec reste bloquant : pas de connexion sur un client non configuré.
  it("remonte l'échec de découverte à la connexion", async () => {
    const provider = new DiscoveryProvider(makeConfig(true), true);
    await assertRejects(() => provider.getLoginUrl(), Error, "ECONNREFUSED");
  });
});
