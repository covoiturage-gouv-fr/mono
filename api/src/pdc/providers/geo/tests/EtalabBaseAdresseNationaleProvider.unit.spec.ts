import { assertEquals, assertRejects } from "dep:assert";
import { afterEach, beforeEach, describe, it } from "dep:testing-bdd";
import { Stub, stub } from "dep:testing-mock";
import { NotFoundException } from "@/ilos/common/index.ts";

import { EtalabBaseAdresseNationaleProvider } from "../providers/index.ts";
import { geo, geoError, insee, inseeError } from "./data.ts";
import { ban } from "./fixtures/etalab.ts";

const json = (body: unknown, status = 200) =>
  Promise.resolve(
    new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } }),
  );

describe("EtalabBaseAdresseNationaleProvider (fetch simulé)", () => {
  const provider = new EtalabBaseAdresseNationaleProvider();
  let fetchStub: Stub;

  const stubFetch = (response: () => Promise<Response>) => {
    fetchStub = stub(globalThis, "fetch", response);
  };
  const calledUrl = () => new URL(fetchStub.calls[0].args[0] as string);

  beforeEach(() => {
    fetchStub = undefined as unknown as Stub;
  });
  afterEach(() => fetchStub?.restore());

  describe("positionToInsee", () => {
    it("envoie lat/lon sur /reverse et retourne le citycode", async () => {
      stubFetch(() => json(ban.reverse));
      assertEquals(await provider.positionToInsee(insee.position), insee.code);

      const url = calledUrl();
      assertEquals(url.origin + url.pathname, "https://api-adresse.data.gouv.fr/reverse");
      assertEquals(Object.fromEntries(url.searchParams), { lat: "48.721505", lon: "2.26188" });
      assertEquals(fetchStub.calls[0].args[1]?.method, "GET");
    });

    it("NotFound si aucune feature", async () => {
      stubFetch(() => json(ban.empty));
      await assertRejects(() => provider.positionToInsee(inseeError.position), NotFoundException);
    });

    it("NotFound si la feature n'a pas de citycode", async () => {
      stubFetch(() => json({ features: [{ properties: { city: "Massy" } }] }));
      await assertRejects(() => provider.positionToInsee(insee.position), NotFoundException);
    });

    it("NotFound sur HTTP 404", async () => {
      stubFetch(() => json({}, 404));
      await assertRejects(() => provider.positionToInsee(insee.position), NotFoundException);
    });

    it("erreur sur HTTP 500", async () => {
      stubFetch(() => json({}, 500));
      await assertRejects(() => provider.positionToInsee(insee.position), Error, "HTTP Error 500");
    });

    it("erreur sur HTTP 504", async () => {
      stubFetch(() => Promise.resolve(new Response("Gateway Timeout", { status: 504 })));
      await assertRejects(() => provider.positionToInsee(insee.position), Error, "HTTP Error 504");
    });

    it("propage l'échec réseau", async () => {
      stubFetch(() => Promise.reject(new TypeError("network down")));
      await assertRejects(() => provider.positionToInsee(insee.position), TypeError, "network down");
    });

    it("rejette un corps non JSON", async () => {
      stubFetch(() => Promise.resolve(new Response("<html>oops</html>", { status: 200 })));
      await assertRejects(() => provider.positionToInsee(insee.position), SyntaxError);
    });
  });

  describe("literalToPosition", () => {
    it("envoie q/limit/autocomplete sur /search et retourne lon/lat", async () => {
      stubFetch(() => json(ban.search));
      assertEquals(await provider.literalToPosition(geo.literal), {
        lon: geo.position.lon,
        lat: geo.position.lat,
      });

      const url = calledUrl();
      assertEquals(url.origin + url.pathname, "https://api-adresse.data.gouv.fr/search");
      assertEquals(Object.fromEntries(url.searchParams), {
        q: "30 avenue carnot massy",
        limit: "1",
        autocomplete: "0",
      });
    });

    it("NotFound si aucune feature", async () => {
      stubFetch(() => json(ban.empty));
      await assertRejects(() => provider.literalToPosition(geoError.literal), NotFoundException);
    });

    it("NotFound si pas de coordonnées", async () => {
      stubFetch(() => json({ features: [{ geometry: {} }] }));
      await assertRejects(() => provider.literalToPosition(geo.literal), NotFoundException);
    });

    it("NotFound sur HTTP 404", async () => {
      stubFetch(() => json({}, 404));
      await assertRejects(() => provider.literalToPosition(geo.literal), NotFoundException);
    });

    it("erreur sur HTTP 500", async () => {
      stubFetch(() => json({}, 500));
      await assertRejects(() => provider.literalToPosition(geo.literal), Error, "HTTP Error 500");
    });

    it("erreur sur HTTP 504", async () => {
      stubFetch(() => json({}, 504));
      await assertRejects(() => provider.literalToPosition(geo.literal), Error, "HTTP Error 504");
    });

    it("propage l'échec réseau", async () => {
      stubFetch(() => Promise.reject(new TypeError("network down")));
      await assertRejects(() => provider.literalToPosition(geo.literal), TypeError, "network down");
    });
  });
});
