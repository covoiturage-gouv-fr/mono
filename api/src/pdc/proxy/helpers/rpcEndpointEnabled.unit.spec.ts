import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { isRpcEndpointEnabled } from "./rpcEndpointEnabled.ts";

describe("isRpcEndpointEnabled", () => {
  it("ouvre le canal en local et pour les suites de tests", () => {
    assertEquals(isRpcEndpointEnabled("local", false), true);
    assertEquals(isRpcEndpointEnabled("test", false), true);
    assertEquals(isRpcEndpointEnabled(["local", "local"], false), true);
  });

  it("le ferme partout ailleurs", () => {
    assertEquals(isRpcEndpointEnabled("production", false), false);
    assertEquals(isRpcEndpointEnabled("demo", false), false);
    assertEquals(isRpcEndpointEnabled("staging", false), false);
  });

  // NODE_ENV et APP_ENV sont lus tous les deux : un désaccord ne doit pas ouvrir la voie.
  it("le ferme si un seul des environnements n'est pas de développement", () => {
    assertEquals(isRpcEndpointEnabled(["local", "production"], false), false);
    assertEquals(isRpcEndpointEnabled([], false), false);
  });

  it("le drapeau force l'ouverture, y compris en production", () => {
    assertEquals(isRpcEndpointEnabled("production", true), true);
  });
});
