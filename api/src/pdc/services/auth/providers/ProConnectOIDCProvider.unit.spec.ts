import { ConfigInterfaceResolver } from "@/ilos/common/index.ts";
import { UserRepository } from "@/pdc/services/auth/providers/UserRepository.ts";
import { assertEquals, assertRejects } from "dep:assert";
import { getPermissions } from "../config/permissions.ts";
import { errorCode } from "./proconnectLog.ts";
import { describe, it } from "dep:testing-bdd";
import {
  failsMfaCheck,
  failsSirenCheck,
  MFA_ACR_VALUES,
  mfaClaimsParameter,
  MfaRequiredError,
  PROCONNECT_SCOPE,
  ProConnectOIDCProvider,
  sirenRefusal,
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

describe("ProConnectOIDCProvider.sirenRefusal", () => {
  it("compare le SIREN ProConnect aux 9 premiers chiffres de login_siret", () => {
    const user = { login_siret: "12345678900012", login_siren: null, role: "territory.admin" };
    assertEquals(sirenRefusal({ siren: "123456789" }, user), null);
    assertEquals(sirenRefusal({ siren: "987654321" }, user), "siren_mismatch");
  });

  it("login_siret prime sur login_siren", () => {
    const user = { login_siret: "12345678900012", login_siren: "987654321", role: "territory.admin" };
    assertEquals(sirenRefusal({ siren: "123456789" }, user), null);
    assertEquals(sirenRefusal({ siren: "987654321" }, user), "siren_mismatch");
  });

  it("repli sur login_siren quand login_siret est absent ou vide", () => {
    for (const login_siret of [null, undefined, "", "  "]) {
      const user = { login_siret, login_siren: "123456789", role: "territory.admin" };
      assertEquals(sirenRefusal({ siren: "123456789" }, user), null);
    }
  });

  it("no_login_siren quand ni login_siret ni login_siren", () => {
    assertEquals(
      sirenRefusal({ siren: "123456789" }, { login_siret: null, login_siren: null, role: "territory.admin" }),
      "no_login_siren",
    );
  });

  it("renvoie null pour registry.admin", () => {
    assertEquals(sirenRefusal({ siren: "999999999" }, { login_siren: null, role: "registry.admin" }), null);
  });

  it("renvoie no_login_siren quand login_siren est null, vide ou blanc", () => {
    for (const login_siren of [null, undefined, "", "  "]) {
      assertEquals(sirenRefusal({ siren: "123456789" }, { login_siren, role: "territory.admin" }), "no_login_siren");
    }
  });

  it("renvoie siren_mismatch quand le SIREN diffère", () => {
    assertEquals(
      sirenRefusal({ siren: "123456789" }, { login_siren: "987654321", role: "territory.admin" }),
      "siren_mismatch",
    );
  });

  it("renvoie null quand le SIREN correspond", () => {
    assertEquals(sirenRefusal({ siren: "123456789" }, { login_siren: "123456789", role: "territory.admin" }), null);
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

describe("MfaRequiredError", () => {
  it("exposes the refused acr", () => {
    assertEquals(new MfaRequiredError("x", "eidas1").acr, "eidas1");
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

// getLocalUser est protected : exposé par sous-classe.
class LocalUserProvider extends ProConnectOIDCProvider {
  constructor(authenticateByEmail: () => Promise<unknown>) {
    super(makeConfig(true), { authenticateByEmail } as unknown as UserRepository);
  }

  lookup(email: string, siret: string, given_name?: string, family_name?: string) {
    return this.getLocalUser(email, siret, given_name, family_name);
  }
}

describe("ProConnectOIDCProvider.getLocalUser", () => {
  const email = "jean.dupont@idfm.fr";
  const base = { _id: 1, email, role: "territory.admin", login_siren: "123456789" };

  it("not_found quand l'utilisateur n'existe pas", async () => {
    const res = await new LocalUserProvider(() => Promise.resolve(null)).lookup(email, "12345678900012");
    assertEquals(res, { user: null, reason: "not_found" });
  });

  it("anonymous_role quand le compte porte le rôle anonymous", async () => {
    const provider = new LocalUserProvider(() => Promise.resolve({ ...base, role: "anonymous" }));
    assertEquals(await provider.lookup(email, "12345678900012"), { user: null, reason: "anonymous_role" });
  });

  it("no_login_siren quand l'utilisateur n'a pas de SIREN de connexion", async () => {
    const provider = new LocalUserProvider(() => Promise.resolve({ ...base, login_siren: null }));
    assertEquals(await provider.lookup(email, "12345678900012"), { user: null, reason: "no_login_siren" });
  });

  it("siren_mismatch quand le SIRET ne correspond pas", async () => {
    const provider = new LocalUserProvider(() => Promise.resolve(base));
    assertEquals(await provider.lookup(email, "98765432100012"), { user: null, reason: "siren_mismatch" });
  });

  it("db_error avec un code sans donnée personnelle", async () => {
    const provider = new LocalUserProvider(() => Promise.reject(new TypeError(`boom ${email}`)));
    const res = await provider.lookup(email, "12345678900012");
    assertEquals(res.user, null);
    if (res.user !== null) return;
    assertEquals(res.reason, "db_error");
    assertEquals(res.error, errorCode(new TypeError("x")));
    assertEquals(res.error?.includes(email), false);
    assertEquals(res.error?.includes("@"), false);
  });

  it("renvoie l'utilisateur enrichi du nom et des permissions", async () => {
    const provider = new LocalUserProvider(() => Promise.resolve(base));
    const res = await provider.lookup(email, "12345678900012", "Jean", "Dupont");
    if (res.user === null) throw new Error("user attendu");
    assertEquals(res.user.name, "Jean Dupont");
    assertEquals(res.user.role, "territory.admin");
    assertEquals(res.user.permissions, getPermissions("territory.admin"));
  });
});

describe("ProConnectOIDCProvider scope", () => {
  it("demande idp_id pour tracer le fournisseur d'identité", () => {
    assertEquals(PROCONNECT_SCOPE.split(" ").includes("idp_id"), true);
  });
});
