import { describe, expect, it } from "vitest";
import { errorMessageFor, toErrorMessage } from "./useApi";

describe("toErrorMessage", () => {
  const generic = "La requête a été refusée (400).";

  it("masque un texte brut (proxy, passerelle)", () => {
    expect(toErrorMessage(400, "upstream connect error")).toBe(generic);
  });

  it("masque une page d'erreur HTML", () => {
    expect(toErrorMessage(400, "<html><body>Bad Request</body></html>")).toBe(generic);
  });

  it("masque une chaîne JSON nue", () => {
    expect(toErrorMessage(400, '"introuvable"')).toBe(generic);
  });

  it("masque un JSON sans detail ni message", () => {
    expect(toErrorMessage(400, '{"error":"x"}')).toBe(generic);
  });

  it("extrait le detail FastAPI", () => {
    expect(toErrorMessage(404, '{"detail":"Territoire inconnu"}')).toBe("Territoire inconnu");
  });

  it("extrait message à défaut de detail", () => {
    expect(toErrorMessage(400, '{"message":"boom"}')).toBe("boom");
  });

  it("n'expose pas un detail non textuel (422 FastAPI)", () => {
    const body = '{"detail":[{"loc":["query","q"],"msg":"too long"}]}';
    expect(toErrorMessage(422, body)).toBe("La requête a été refusée (422).");
  });

  it("tronque un detail à 500 caractères", () => {
    expect(toErrorMessage(400, JSON.stringify({ detail: "a".repeat(600) }))).toBe("a".repeat(500));
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

describe("errorMessageFor 4xx", () => {
  it("masque le corps brut d'une 4xx de proxy", () => {
    expect(errorMessageFor(403, "RBAC: access denied")).toBe("La requête a été refusée (403).");
  });
});
