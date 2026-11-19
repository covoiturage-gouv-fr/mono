import { describe, expect, it } from "vitest";
import { errorMessageFor, toErrorMessage } from "./useApi";

describe("toErrorMessage", () => {
  it("renvoie le texte brut si le corps n'est pas du JSON", () => {
    expect(toErrorMessage("Bad Gateway")).toBe("Bad Gateway");
  });

  it("renvoie une chaîne JSON telle quelle", () => {
    expect(toErrorMessage('"introuvable"')).toBe("introuvable");
  });

  it("extrait le detail FastAPI", () => {
    expect(toErrorMessage('{"detail":"Territoire inconnu"}')).toBe("Territoire inconnu");
  });

  it("extrait message à défaut de detail", () => {
    expect(toErrorMessage('{"message":"boom"}')).toBe("boom");
  });

  it("n'expose pas un detail non textuel (422 FastAPI)", () => {
    const body = '{"detail":[{"loc":["query","q"],"msg":"too long"}]}';
    expect(toErrorMessage(body)).toBe("Erreur inattendue de l'API");
  });
});

describe("errorMessageFor", () => {
  it("masque le corps brut d'une 429 de l'edge", () => {
    expect(errorMessageFor(429, "local_rate_limited")).toBe("Trop de requêtes, réessayez dans quelques instants.");
  });

  it("affiche un message de maintenance fixe sur 503", () => {
    expect(errorMessageFor(503, '{"status":"maintenance"}')).toBe("Le service est en maintenance. Réessayez plus tard.");
    expect(errorMessageFor(503, "no healthy upstream")).toBe(
      "Le service a rencontré une erreur interne (503). Réessayez plus tard.",
    );
  });

  it("masque le corps brut des autres 5xx", () => {
    expect(errorMessageFor(500, '{"detail":"psycopg: connection refused"}')).toBe(
      "Le service a rencontré une erreur interne (500). Réessayez plus tard.",
    );
    expect(errorMessageFor(502, "Bad Gateway")).toBe("Le service a rencontré une erreur interne (502). Réessayez plus tard.");
  });

  it("relaie le corps des 4xx", () => {
    expect(errorMessageFor(404, '{"detail":"Territoire inconnu"}')).toBe("Territoire inconnu");
  });
});
