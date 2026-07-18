import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { failsSirenCheck } from "./ProConnectOIDCProvider.ts";

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
