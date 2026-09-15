import { assertEquals, assertThrows } from "dep:assert";
import { afterEach, beforeEach, describe, it } from "dep:testing-bdd";
import { EnvNotFoundException } from "@/lib/env/index.ts";
import { accounts } from "./test.ts";

const KEYS = [
  "APIE2E_AUTH_ADMIN_EMAIL",
  "APIE2E_AUTH_ADMIN_PASSWORD",
  "APIE2E_AUTH_OPERATOR_EMAIL",
  "APIE2E_AUTH_OPERATOR_PASSWORD",
  "APIE2E_AUTH_TERRITORY_EMAIL",
  "APIE2E_AUTH_TERRITORY_PASSWORD",
  "APIE2E_AUTH_MULTI_EMAIL",
  "APIE2E_AUTH_MULTI_PASSWORD",
];

describe("auth test config", () => {
  // Nettoyage avant ET après : la stack de test exporte ces variables dans l'environnement.
  beforeEach(() => KEYS.forEach((k) => Deno.env.delete(k)));
  afterEach(() => KEYS.forEach((k) => Deno.env.delete(k)));

  function setRequired() {
    Deno.env.set("APIE2E_AUTH_ADMIN_EMAIL", "a@x.test");
    Deno.env.set("APIE2E_AUTH_ADMIN_PASSWORD", "pa");
    Deno.env.set("APIE2E_AUTH_OPERATOR_EMAIL", "o@x.test");
    Deno.env.set("APIE2E_AUTH_OPERATOR_PASSWORD", "po");
    Deno.env.set("APIE2E_AUTH_TERRITORY_EMAIL", "t@x.test");
    Deno.env.set("APIE2E_AUTH_TERRITORY_PASSWORD", "pt");
  }

  it("accounts() throws when a variable is missing", () => {
    assertThrows(() => accounts(), EnvNotFoundException);
  });

  it("accounts() builds the map from env", () => {
    setRequired();
    const map = accounts();
    assertEquals(map.get("a@x.test"), "pa");
    assertEquals(map.get("o@x.test"), "po");
    assertEquals(map.get("t@x.test"), "pt");
    assertEquals(map.size, 3);
  });

  it("accounts() ajoute le compte multi-périmètre quand il est configuré", () => {
    setRequired();
    Deno.env.set("APIE2E_AUTH_MULTI_EMAIL", "m@x.test");
    Deno.env.set("APIE2E_AUTH_MULTI_PASSWORD", "pm");
    const map = accounts();
    assertEquals(map.get("m@x.test"), "pm");
    assertEquals(map.size, 4);
  });

  it("accounts() ignore un compte multi-périmètre incomplet", () => {
    setRequired();
    Deno.env.set("APIE2E_AUTH_MULTI_EMAIL", "m@x.test");
    assertEquals(accounts().size, 3);
  });
});
