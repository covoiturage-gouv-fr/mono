import { assertEquals, assertStringIncludes } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { InactiveUserDeletionWarningNotification } from "./InactiveUserDeletionWarningNotification.ts";

describe("InactiveUserDeletionWarningNotification", () => {
  it("porte le sujet, le CTA et le lien de reconnexion", () => {
    const n = new InactiveUserDeletionWarningNotification("Jane Doe <jane@example.com>", {
      fullname: "Jane Doe",
      action_href: "https://partenaire.covoiturage.beta.gouv.fr",
    });
    assertEquals(
      (n.constructor as typeof InactiveUserDeletionWarningNotification).subject,
      "Votre compte Registre de preuve de covoiturage va être supprimé",
    );
    assertEquals(n.to, "Jane Doe <jane@example.com>");
    assertEquals(n.data.action_message, "Je me connecte au RPC");
    assertEquals(n.data.action_href, "https://partenaire.covoiturage.beta.gouv.fr");
    assertStringIncludes(n.data.message_text ?? "", "RGPD");
    assertStringIncludes(n.data.message_text ?? "", "7 jours");
  });
});
