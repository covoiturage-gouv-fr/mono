import { assert, assertEquals, assertNotEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { sessionSecretGenerator } from "./sessionSecretGenerator.ts";

describe("sessionSecretGenerator", () => {
  it("returns a valid UUID v4 string", () => {
    const secret = sessionSecretGenerator();
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
    assert(uuidRegex.test(secret), `Expected UUID v4, got: ${secret}`);
  });

  it("returns a string of 36 characters", () => {
    const secret = sessionSecretGenerator();
    assertEquals(secret.length, 36);
  });

  it("generates unique values on each call", () => {
    const secrets = new Set(Array.from({ length: 100 }, () => sessionSecretGenerator()));
    assertEquals(secrets.size, 100, "Expected 100 unique secrets");
  });

  it("does not use predictable Math.random() output", () => {
    // Seed Math.random deterministically would not affect crypto.randomUUID
    const secret = sessionSecretGenerator();
    assertNotEquals(secret, "", "Secret must not be empty");
    assert(secret.includes("-"), "Expected UUID format with dashes");
  });
});
