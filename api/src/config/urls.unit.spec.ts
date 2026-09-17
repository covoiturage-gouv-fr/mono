import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { appUrlsDiverge, callbackUrl, mergeOrigins, pickAppUrl } from "./urls.ts";

describe("pickAppUrl", () => {
  it("préfère APP_APP_URL", () => {
    assertEquals(pickAppUrl("https://app.example", "https://legacy.example"), "https://app.example");
  });

  it("retombe sur l'ancien nom quand le nouveau est absent", () => {
    assertEquals(pickAppUrl(undefined, "https://legacy.example"), "https://legacy.example");
  });

  it("retombe sur le défaut quand aucun n'est posé", () => {
    assertEquals(pickAppUrl(undefined, undefined), "http://localhost:4200");
  });

  it("ignore une valeur vide", () => {
    assertEquals(pickAppUrl("", "https://legacy.example"), "https://legacy.example");
  });
});

describe("appUrlsDiverge", () => {
  it("signale deux valeurs différentes : l'une des deux sera ignorée", () => {
    assertEquals(appUrlsDiverge("https://app.example", "https://autre.example"), true);
  });

  it("ne signale rien quand elles coïncident", () => {
    assertEquals(appUrlsDiverge("https://app.example", "https://app.example"), false);
  });

  it("ne signale rien quand l'ancien nom n'est pas posé", () => {
    assertEquals(appUrlsDiverge("https://app.example", undefined), false);
  });
});

describe("callbackUrl", () => {
  it("déduit le rappel de l'origine de l'API", () => {
    assertEquals(
      callbackUrl("http://localhost:3000", "/auth/login/callback"),
      "http://localhost:3000/auth/login/callback",
    );
  });

  it("tolère une barre oblique finale", () => {
    assertEquals(
      callbackUrl("http://localhost:3000/", "/auth/login/callback"),
      "http://localhost:3000/auth/login/callback",
    );
  });

  it("garde le port et le schéma", () => {
    assertEquals(
      callbackUrl("https://api.example:8443", "/auth/logout/callback"),
      "https://api.example:8443/auth/logout/callback",
    );
  });
});

describe("mergeOrigins", () => {
  it("découpe sur la virgule et retire les espaces", () => {
    assertEquals(mergeOrigins("https://a.example, https://b.example"), ["https://a.example", "https://b.example"]);
  });

  it("fusionne plusieurs variables sans doublon", () => {
    assertEquals(mergeOrigins("https://a.example", "https://a.example,https://b.example"), [
      "https://a.example",
      "https://b.example",
    ]);
  });

  it("ignore les valeurs absentes ou vides", () => {
    assertEquals(mergeOrigins(undefined, "", "https://a.example"), ["https://a.example"]);
  });
});
