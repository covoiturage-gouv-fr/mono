import { afterEach, describe, expect, test, vi } from "vitest";
import { castPerimeterType, fetchSearchAPI, getUrl } from "./search";

// Corps réel renvoyé par Meilisearch quand la clé est absente ou invalide.
const invalidApiKey = {
  message: "The provided API key is invalid.",
  code: "invalid_api_key",
  type: "auth",
  link: "https://docs.meilisearch.com/errors#invalid_api_key",
};

const respondWith = (status: number, body: unknown) =>
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(JSON.stringify(body), {
      status,
      headers: { "Content-Type": "application/json" },
    }),
  );

afterEach(() => vi.restoreAllMocks());

describe("fetchSearchAPI", () => {
  test("lève une erreur portant le statut quand la recherche répond 403", async () => {
    respondWith(403, invalidApiKey);

    await expect(fetchSearchAPI("indexes/geo/search")).rejects.toThrow(/403/);
  });

  test("lève une erreur portant le code Meilisearch quand la recherche répond 403", async () => {
    respondWith(403, invalidApiKey);

    await expect(fetchSearchAPI("indexes/geo/search")).rejects.toThrow(
      /invalid_api_key/,
    );
  });

  test("retourne le corps décodé quand la recherche répond 200", async () => {
    respondWith(200, { hits: [{ l_territory: "France" }] });

    await expect(fetchSearchAPI("indexes/geo/search")).resolves.toEqual({
      hits: [{ l_territory: "France" }],
    });
  });
});

describe("castPerimeterType", () => {
  test("nomme les territoires custom", () => {
    expect(castPerimeterType("custom")).toBe("Territoire personnalisé");
  });
});

describe("getUrl", () => {
  test("renvoie le chemin nu sans territoire", () => {
    expect(getUrl("territoire")).toBe("/observatoire/territoire");
  });

  test("porte le code et le type du territoire", () => {
    expect(
      getUrl("territoire", { territory: "69123", l_territory: "Lyon", type: "com" }),
    ).toBe("/observatoire/territoire?code=69123&type=com");
  });

  test("tronque le code au SIREN (9 caractères)", () => {
    expect(
      getUrl("territoire", {
        territory: "200046977000",
        l_territory: "Métropole de Lyon",
        type: "epci",
      }),
    ).toBe("/observatoire/territoire?code=200046977&type=epci");
  });

  test("encode un code qui casserait la query string", () => {
    expect(
      getUrl("territoire", { territory: "69&type=x", l_territory: "Bidon", type: "com" }),
    ).toBe("/observatoire/territoire?code=69%26type%3Dx&type=com");
  });
});
