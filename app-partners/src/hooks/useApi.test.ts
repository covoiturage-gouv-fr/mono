import {
  AUTH_UNAUTHORIZED_EVENT,
  apiErrorMessage,
  parseBody,
  signalUnauthorized,
  toUserError,
  UNREACHABLE_MESSAGE,
} from "@/hooks/useApi";
import { afterEach, describe, expect, it, vi } from "vitest";

describe("signalUnauthorized", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("dispatche l'événement sur window", () => {
    const target = new EventTarget();
    vi.stubGlobal("window", target);
    const received: string[] = [];
    target.addEventListener(AUTH_UNAUTHORIZED_EVENT, (e) => received.push(e.type));

    signalUnauthorized();

    expect(received).toEqual([AUTH_UNAUTHORIZED_EVENT]);
  });

  it("ne plante pas hors navigateur", () => {
    expect(() => signalUnauthorized()).not.toThrow();
  });
});

describe("apiErrorMessage", () => {
  it("traduit les statuts d'accès sans relayer le corps", () => {
    expect(apiErrorMessage(401, { message: "jwt expired" })).toBe("Votre session a expiré, reconnectez-vous.");
    expect(apiErrorMessage(403, { message: "Forbidden" })).toBe(
      "Vous n'avez pas les droits nécessaires pour cette action.",
    );
    expect(apiErrorMessage(404, "not found")).toBe("Élément introuvable.");
  });

  it("impose un message fixe sur 429, edge (texte brut) ou API (JSON)", () => {
    const message = "Trop de requêtes, réessayez dans quelques instants.";
    expect(apiErrorMessage(429, "local_rate_limited")).toBe(message);
    expect(apiErrorMessage(429, { error: { code: 429, message: "Too many requests" } })).toBe(message);
  });

  it("ne relaie jamais le corps d'une 5xx", () => {
    expect(apiErrorMessage(500, { message: "relation users does not exist" })).toBe(
      "Le service a rencontré une erreur interne (500). Réessayez plus tard.",
    );
  });

  it("relaie message, tableau de violations ou chaîne nue d'une 4xx", () => {
    expect(apiErrorMessage(400, { message: "email déjà utilisé" })).toBe("email déjà utilisé");
    expect(apiErrorMessage(400, ["firstname trop court", "email invalide"])).toBe(
      "firstname trop court\nemail invalide",
    );
    expect(apiErrorMessage(409, "Conflict")).toBe("Conflict");
    expect(apiErrorMessage(400, { error: "Bad Request" })).toBe("Bad Request");
  });

  it("borne le relais à 500 caractères", () => {
    expect(apiErrorMessage(400, "x".repeat(2000))).toHaveLength(500);
  });

  it("retombe sur un message générique quand le corps est vide ou inattendu", () => {
    expect(apiErrorMessage(400, undefined)).toBe("La requête a été refusée (400).");
    expect(apiErrorMessage(422, { message: { nested: true } })).toBe("La requête a été refusée (422).");
    expect(apiErrorMessage(400, [1, 2])).toBe("La requête a été refusée (400).");
  });
});

describe("parseBody", () => {
  it("parse le JSON, garde le texte brut sinon, undefined si vide", () => {
    expect(parseBody('{"a":1}')).toEqual({ a: 1 });
    expect(parseBody("Bad Gateway")).toBe("Bad Gateway");
    expect(parseBody("")).toBeUndefined();
  });
});

describe("toUserError", () => {
  it("remplace le TypeError réseau de fetch par un message français", () => {
    expect(toUserError(new TypeError("Failed to fetch")).message).toBe(UNREACHABLE_MESSAGE);
  });

  it("garde une Error et enveloppe le reste", () => {
    const e = new Error("refus");
    expect(toUserError(e)).toBe(e);
    expect(toUserError("boom").message).toBe("boom");
  });
});
