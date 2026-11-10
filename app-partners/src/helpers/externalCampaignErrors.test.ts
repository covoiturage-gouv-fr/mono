import { campaignErrorMessage } from "@/helpers/externalCampaignErrors";
import { describe, expect, it } from "vitest";

// Corps réels : ValidatorMiddleware -> InvalidParamsException -> formatRouteError (400, tableau de chaînes).
describe("campaignErrorMessage", () => {
  it("traduit une valeur manquante", () => {
    expect(campaignErrorMessage(400, ["date_debut : Expected a string, but received: null"])).toBe(
      "Date de début : valeur requise",
    );
    expect(campaignErrorMessage(400, ["date_debut : Expected a string, but received: undefined"])).toBe(
      "Date de début : valeur requise",
    );
  });

  it("traduit les nombres", () => {
    expect(campaignErrorMessage(400, ["budget_incitations : Expected an integer, but received: 1.5"])).toBe(
      "Budget des incitations : nombre entier attendu",
    );
    expect(
      campaignErrorMessage(400, [
        "budget_incitations : Expected a integer between `0` and `2147483647` but received `-3`",
      ]),
    ).toBe("Budget des incitations : nombre positif attendu");
  });

  it("garde une ligne par champ, la première", () => {
    expect(
      campaignErrorMessage(400, [
        'date_debut : Expected a string matching `/^\\d{4}-\\d{2}-\\d{2}$/` but received "abc"',
        "date_debut : Date invalide",
        'territory_code : Expected a string matching `/^[0-9A-Za-z]{1,20}$/` but received ""',
        "territory_label : Expected a string, but received: null",
      ]),
    ).toBe("Date de début : date attendue (AAAA-MM-JJ)\nTerritoire : format invalide");
  });

  it("relaie un message déjà en français, en minuscule initiale", () => {
    expect(campaignErrorMessage(400, ["autres_informations : Balises interdites"])).toBe(
      "Autres informations : balises interdites",
    );
  });

  it("traduit tableaux, énumérations, booléens et liens", () => {
    expect(
      campaignErrorMessage(400, [
        "operator_ids : Expected a array with a length between `1` and `50` but received one with a length of `0`",
        'trajet_classe_preuve,0 : Expected one of `"A","B","C"`, but received: "Z"',
        'premiere_campagne : Expected a value of type `boolean`, but received: `"x"`',
        'lien_page_collectivite : Expected a string matching `/^https?:\\/\\/[^\\s<>"]+$/` but received "ftp://a"',
      ]),
    ).toBe(
      [
        "Opérateurs participants : au moins une valeur requise",
        "Classes de preuve acceptées : valeur non reconnue",
        "Première campagne sur ce territoire : oui ou non attendu",
        "Lien vers la page de la collectivité : adresse web attendue (https://…)",
      ].join("\n"),
    );
  });

  it("reformule les règles croisées sans chemin", () => {
    expect(campaignErrorMessage(400, [" : date_fin doit suivre date_debut"])).toBe(
      "La date de fin doit être postérieure à la date de début.",
    );
  });

  it("repli sur le nom technique d'un champ inconnu", () => {
    expect(campaignErrorMessage(400, ["nouveau_champ : Expected a string, but received: null"])).toBe(
      "nouveau_champ : valeur requise",
    );
  });

  it("relaie tel quel un message métier français (corps = chaîne)", () => {
    const msg = "Une campagne existe déjà pour ce territoire à cette date de début";
    expect(campaignErrorMessage(409, msg)).toBe(msg);
    const territoire =
      "Votre territoire n'a pas d'équivalent dans le datalake : contactez l'équipe RPC pour déclarer la campagne";
    expect(campaignErrorMessage(400, territoire)).toBe(territoire);
  });

  it("relaie le motif d'un 403 chaîne, sinon garde le message fixe", () => {
    const msg = "Campagne en lecture seule : contactez l'équipe RPC";
    expect(campaignErrorMessage(403, msg)).toBe(msg);
    const fixe = "Vous n'avez pas les droits nécessaires pour cette action.";
    expect(campaignErrorMessage(403, undefined)).toBe(fixe);
    expect(campaignErrorMessage(403, "  ")).toBe(fixe);
    expect(campaignErrorMessage(403, { message: "x" })).toBe(fixe);
  });

  it("garde le repli générique", () => {
    expect(campaignErrorMessage(400, { error: "Bad" })).toBe("Bad");
    expect(campaignErrorMessage(400, undefined)).toBe("La requête a été refusée (400).");
    expect(campaignErrorMessage(400, ["texte libre sans chemin"])).toBe("texte libre sans chemin");
  });

  it("met en forme un vrai corps 400 de l'API (dédoublonné par champ)", () => {
    expect(
      campaignErrorMessage(400, [
        'territory_type : Expected one of `"com","epci"`, but received: undefined',
        'date_debut : Expected a string matching `/^\\d{4}-\\d{2}-\\d{2}$/` but received "pas-une-date"',
        "date_debut : Date invalide",
        "budget_incitations : Expected a integer between `0` and `2147483647` but received `-5`",
      ]),
    ).toBe(
      [
        "Territoire : valeur requise",
        "Date de début : date attendue (AAAA-MM-JJ)",
        "Budget des incitations : nombre positif attendu",
      ].join("\n"),
    );
  });
});
