import { assertObjectMatch } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { ExportRecipient } from "./ExportRecipient.ts";

describe("ExportRecipient", () => {
  it("should parse email with fullname", () => {
    assertObjectMatch(
      ExportRecipient.parseEmail("John Doe <jon.doe@example.com>"),
      {
        fullname: "John Doe",
        email: "jon.doe@example.com",
      },
    );
  });

  it("should parse email with fullname with accents", () => {
    assertObjectMatch(
      ExportRecipient.parseEmail("Aimé Césaire <aime.cesaire@example.com>"),
      {
        fullname: "Aimé Césaire",
        email: "aime.cesaire@example.com",
      },
    );
  });

  it("should parse email without fullname", () => {
    assertObjectMatch(ExportRecipient.parseEmail("jon.doe@example.com"), {
      fullname: null,
      email: "jon.doe@example.com",
    });
  });

  it("should parse email without fullname with a plus sign", () => {
    assertObjectMatch(ExportRecipient.parseEmail("jon.doe+label@example.com"), {
      fullname: null,
      email: "jon.doe+label@example.com",
    });
  });
});
