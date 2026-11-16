import { assertEquals, assertRejects } from "dep:assert";
import { describe, it } from "dep:testing-bdd";

import { EtalabBaseAdresseNationaleProvider } from "../providers/index.ts";
import { geo, geoError, insee, inseeError } from "./data.ts";

// Appels réels à une API externe, instables depuis la CI : opt-in.
describe({
  name: "EtalabBaseAdresseNationaleProvider (API externe)",
  ignore: Deno.env.get("APP_TEST_EXTERNAL_API") !== "true",
}, () => {
  const provider = new EtalabBaseAdresseNationaleProvider();

  it("EtalabBaseAdresseNationaleProvider: positionToInsee", async () => {
    assertEquals(await provider.positionToInsee(insee.position), insee.code);
  });

  it("EtalabBaseAdresseNationaleProvider: positionToInsee not found", async () => {
    await assertRejects(async () => provider.positionToInsee(inseeError.position));
  });

  it("EtalabBaseAdresseNationaleProvider: literalToPosition", async () => {
    const { lat, lon } = await provider.literalToPosition(geo.literal);
    assertEquals(lon, geo.position.lon);
    assertEquals(lat, geo.position.lat);
  });

  it("EtalabBaseAdresseNationaleProvider: literalToPosition error", async () => {
    await assertRejects(async () => provider.literalToPosition(geoError.literal));
  });
});
