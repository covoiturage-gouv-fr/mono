import { assertEquals, assertRejects } from "dep:assert";
import { describe, it } from "dep:testing-bdd";

import { EtalabAPIGeoProvider } from "../providers/index.ts";
import { insee, inseeError, inseeGeo, inseeGeoError } from "./data.ts";

// Appels réels à une API externe, instables depuis la CI : opt-in.
describe({
  name: "EtalabAPIGeoProvider (API externe)",
  ignore: Deno.env.get("APP_TEST_EXTERNAL_API") !== "true",
}, () => {
  const provider = new EtalabAPIGeoProvider();

  it("EtalabAPIGeoProvider: positionToInsee", async () => {
    assertEquals(await provider.positionToInsee(insee.position), insee.code);
  });

  it("EtalabAPIGeoProvider: positionToInsee not found", async () => {
    await assertRejects(async () => provider.positionToInsee(inseeError.position));
  });

  it("EtalabAPIGeoProvider: inseeToPosition", async () => {
    const { lat, lon } = await provider.inseeToPosition(inseeGeo.code);
    assertEquals(lon, inseeGeo.position.lon);
    assertEquals(lat, inseeGeo.position.lat);
  });

  it("EtalabAPIGeoProvider: inseeToPosition error", async () => {
    await assertRejects(async () => provider.inseeToPosition(inseeGeoError.code));
  });
});
