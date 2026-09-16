import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { isTestAuthEnabled } from "./enabled.ts";

describe("isTestAuthEnabled", () => {
  it("is off by default", () => assertEquals(isTestAuthEnabled("local", false), false));
  it("is on with flag in local", () => assertEquals(isTestAuthEnabled("local", true), true));
  it("is on with flag in test and ci", () => {
    assertEquals(isTestAuthEnabled("test", true), true);
    assertEquals(isTestAuthEnabled("ci", true), true);
  });
  it("refuses flag in production", () => assertEquals(isTestAuthEnabled("production", true), false));
  it("refuses flag in demo", () => assertEquals(isTestAuthEnabled("demo", true), false));
  // Liste blanche : un environnement inconnu n'ouvre pas la route, même sans être listé comme interdit.
  it("refuses flag in an unknown env", () => {
    assertEquals(isTestAuthEnabled("staging", true), false);
    assertEquals(isTestAuthEnabled("preview", true), false);
    assertEquals(isTestAuthEnabled("", true), false);
  });
  it("refuses flag when any of several envs is not allowed", () => {
    assertEquals(isTestAuthEnabled(["local", "production"], true), false);
    assertEquals(isTestAuthEnabled(["local", "staging"], true), false);
    assertEquals(isTestAuthEnabled(["local", "local"], true), true);
  });
  it("refuses an empty env list", () => assertEquals(isTestAuthEnabled([], true), false));
});
