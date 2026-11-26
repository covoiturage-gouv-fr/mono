import { assertEquals, assertStringIncludes } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { WelcomeUserNotification } from "./WelcomeUserNotification.ts";

describe("WelcomeUserNotification", () => {
  const data = { email: "u@example.com", siret: "12345678901234", action_href: "https://partenaire.example.com" };

  it("porte le sujet, le CTA ProConnect et le lien de connexion", () => {
    const n = new WelcomeUserNotification("u@example.com", data);
    assertEquals(
      (n.constructor as typeof WelcomeUserNotification).subject,
      "Confirmation de la création de votre compte utilisateur sur l'espace partenaire",
    );
    assertEquals(n.data.title, "Confirmation de la création de votre compte utilisateur sur l'espace partenaire");
    assertEquals(n.data.action_message, "Je me connecte avec ProConnect");
    assertEquals(n.data.action_href, data.action_href);
  });

  it("mentionne le SIRET de connexion, l'email et le lien vers la documentation", () => {
    const n = new WelcomeUserNotification("u@example.com", data);
    for (const text of [n.data.message_html ?? "", n.data.message_text ?? ""]) {
      assertStringIncludes(text, "SIRET de connexion fourni : {{ siret }}");
      assertStringIncludes(text, "{{ email }}");
      assertStringIncludes(text, "doc.covoiturage.beta.gouv.fr");
    }
  });

  it("omet la ligne SIRET quand la structure n'en a pas", () => {
    const n = new WelcomeUserNotification("u@example.com", { ...data, siret: "" });
    assertEquals((n.data.message_html ?? "").includes("SIRET"), false);
    assertEquals((n.data.message_text ?? "").includes("SIRET"), false);
  });
});
