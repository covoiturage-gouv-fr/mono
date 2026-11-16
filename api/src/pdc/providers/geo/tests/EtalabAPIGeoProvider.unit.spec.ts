import { assertEquals, assertRejects } from "dep:assert";
import { afterEach, beforeEach, describe, it } from "dep:testing-bdd";
import { Stub, stub } from "dep:testing-mock";
import { NotFoundException } from "@/ilos/common/index.ts";

import { EtalabAPIGeoProvider } from "../providers/index.ts";
import { insee, inseeError, inseeGeo, inseeGeoError } from "./data.ts";
import { geoApi } from "./fixtures/etalab.ts";

const json = (body: unknown, status = 200) =>
  Promise.resolve(
    new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } }),
  );

describe("EtalabAPIGeoProvider (fetch simulé)", () => {
  const provider = new EtalabAPIGeoProvider();
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
    it("envoie lon/lat/fields/format et retourne le code", async () => {
      stubFetch(() => json(geoApi.positionToInsee));
      assertEquals(await provider.positionToInsee(insee.position), insee.code);

      const url = calledUrl();
      assertEquals(url.origin + url.pathname, "https://geo.api.gouv.fr/communes");
      assertEquals(Object.fromEntries(url.searchParams), {
        lon: "2.26188",
        lat: "48.721505",
        fields: "code",
        format: "json",
      });
      assertEquals(fetchStub.calls[0].args[1]?.method, "GET");
    });

    it("NotFound si liste vide", async () => {
      stubFetch(() => json(geoApi.empty));
      await assertRejects(() => provider.positionToInsee(inseeError.position), NotFoundException);
    });

    it("NotFound si l'élément n'a pas de code", async () => {
      stubFetch(() => json([{ nom: "Massy" }]));
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

  describe("inseeToPosition", () => {
    it("envoie code/fields/format et retourne lon/lat", async () => {
      stubFetch(() => json(geoApi.inseeToPosition));
      assertEquals(await provider.inseeToPosition(inseeGeo.code), inseeGeo.position);

      const url = calledUrl();
      assertEquals(url.origin + url.pathname, "https://geo.api.gouv.fr/communes");
      assertEquals(Object.fromEntries(url.searchParams), {
        code: "91377",
        fields: "centre",
        format: "json",
      });
    });

    it("NotFound si liste vide", async () => {
      stubFetch(() => json(geoApi.empty));
      await assertRejects(() => provider.inseeToPosition(inseeGeoError.code), NotFoundException);
    });

    it("NotFound si pas de centre", async () => {
      stubFetch(() => json([{ code: "91377" }]));
      await assertRejects(() => provider.inseeToPosition(inseeGeo.code), NotFoundException);
    });

    it("NotFound sur HTTP 404", async () => {
      stubFetch(() => json({}, 404));
      await assertRejects(() => provider.inseeToPosition(inseeGeo.code), NotFoundException);
    });

    it("erreur sur HTTP 500", async () => {
      stubFetch(() => json({}, 500));
      await assertRejects(() => provider.inseeToPosition(inseeGeo.code), Error, "HTTP Error 500");
    });

    it("erreur sur HTTP 504", async () => {
      stubFetch(() => json({}, 504));
      await assertRejects(() => provider.inseeToPosition(inseeGeo.code), Error, "HTTP Error 504");
    });

    it("propage l'échec réseau", async () => {
      stubFetch(() => Promise.reject(new TypeError("network down")));
      await assertRejects(() => provider.inseeToPosition(inseeGeo.code), TypeError, "network down");
    });
  });
});
