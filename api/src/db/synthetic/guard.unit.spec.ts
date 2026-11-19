import { assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { assertEnvAllowed } from "./guard.ts";
import { profiles } from "./profiles/index.ts";

describe("synthetic / guard", () => {
  it("accepts an allowed environment", () => {
    assertEnvAllowed("local", ["local", "sandbox"]);
    assertEnvAllowed(" sandbox ", ["local", "sandbox"]);
  });

  it("refuses a missing or unknown environment", () => {
    assertThrows(() => assertEnvAllowed(undefined, ["local"]), Error, "pas défini");
    assertThrows(() => assertEnvAllowed("", ["local"]), Error, "pas défini");
    assertThrows(() => assertEnvAllowed("staging", ["local"]), Error, "pas autorisé");
    assertThrows(() => assertEnvAllowed("demo", ["local"]), Error, "pas autorisé");
    assertThrows(() => assertEnvAllowed("Local", ["local"]), Error, "pas autorisé");
  });

  it("refuses production even when the profile allows it", () => {
    assertThrows(() => assertEnvAllowed("production", ["local", "production"]), Error, "interdite");
    assertThrows(() => assertEnvAllowed("Production", ["Production"]), Error, "interdite");
    assertThrows(() => assertEnvAllowed("prod", ["prod"]), Error, "interdite");
  });

  it("opens every profile to its own environment and local only", () => {
    for (const profile of Object.values(profiles)) {
      assertEnvAllowed("local", profile.allowedEnvs);
      assertEnvAllowed(profile.name, profile.allowedEnvs);
      for (const env of ["demo", "dev", "staging", "production"].filter((e) => e !== profile.name)) {
        assertThrows(() => assertEnvAllowed(env, profile.allowedEnvs), Error, undefined, env);
      }
    }
  });
});
