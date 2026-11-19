import { assert, assertEquals, assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { policies } from "@/pdc/services/policy/engine/policies/index.ts";
import { parseTerritoryCodes } from "@/pdc/services/territory/helpers/perimeters.ts";
import { parseReferenceDate, profileErrors } from "./profile.ts";
import { getProfile, profiles } from "./profiles/index.ts";
import { SyntheticProfile } from "./types.ts";

describe("synthetic / profile", () => {
  it("parses the reference date as UTC midnight", () => {
    assertEquals(parseReferenceDate("2026-11-01").toISOString(), "2026-11-01T00:00:00.000Z");
    assertThrows(() => parseReferenceDate("01/11/2026"), Error, "invalide");
    assertThrows(() => parseReferenceDate("2026-13-01"), Error, "invalide");
  });

  it("refuses an unknown profile", () => {
    assertThrows(() => getProfile("nope"), Error, "Profil inconnu");
    assertThrows(() => getProfile(undefined), Error, "Profil inconnu");
  });

  it("reports inconsistencies", () => {
    const base = getProfile("pentest");
    const broken: SyntheticProfile = {
      ...base,
      operators: [{ ...base.operators[0], siret: "00000001800011" }],
      users: [
        { email: "a@example.com", firstname: "A", lastname: "A", role: "operator.user", operator: "beta" },
        { email: "a@example.com", firstname: "A", lastname: "A", role: "registry.admin" },
      ],
    };
    const errors = profileErrors(broken);
    assert(errors.some((e) => e.includes("SIRET invalide")), errors.join("\n"));
    assert(errors.some((e) => e.includes("Opérateur inconnu 'beta'")));
    assert(errors.some((e) => e.includes("a@example.com : opérateur inconnu")));
    assert(errors.some((e) => e.includes("login_siren")));
    assert(errors.some((e) => e.includes("double")));
  });

  for (const profile of Object.values(profiles)) {
    describe(`profile ${profile.name}`, () => {
      it("is consistent", () => {
        assertEquals(profileErrors(profile), []);
        parseReferenceDate(profile.referenceDate);
      });

      it("uses well-formed territory codes and known campaign handlers", () => {
        for (const territory of profile.territories) {
          parseTerritoryCodes(territory.codes);
          for (const campaign of territory.campaigns) {
            assert(policies.has(campaign.handler), campaign.handler);
            assert(new Date(campaign.start_date) < new Date(campaign.end_date), campaign.name);
          }
        }
      });

      it("only uses fictitious e-mail addresses", () => {
        assert(profile.users.every((u) => u.email.endsWith("@example.com")));
      });
    });
  }

  it("pentest profile holds the expected accounts", () => {
    const { users, operators, territories } = getProfile("pentest");
    const count = (role: string) => users.filter((u) => u.role === role).length;
    assertEquals([operators.length, territories.length, users.length], [2, 3, 11]);
    assertEquals(count("registry.admin"), 1);
    assertEquals([count("operator.admin"), count("operator.user")], [2, 2]);
    assertEquals([count("territory.admin"), count("territory.user")], [3, 3]);
  });
});
