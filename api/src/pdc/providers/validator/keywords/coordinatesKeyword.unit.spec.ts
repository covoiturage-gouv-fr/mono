import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";
import { coordinatesKeyword } from "./coordinatesKeyword.ts";

function test(input: { lat: number; lon: number }, expected: boolean) {
  return () => {
    const latResult = (coordinatesKeyword as any).compile("lat")(input.lat);
    const lonResult = (coordinatesKeyword as any).compile("lon")(input.lon);
    assertEquals(latResult && lonResult, expected);
  };
}

it("valid lon and lat integer", test({ lon: 10, lat: 10 }, true));
it(
  "valid lon and lat decimals",
  test({
    lon: 1.12321373,
    lat: -45.1233312333333,
  }, true),
);
it("out of bounds lon 1", test({ lon: 181, lat: 10 }, false));
it("out of bounds lon 2", test({ lon: -181, lat: 10 }, false));
it("out of bounds lat 1", test({ lon: 123, lat: 91 }, false));
it("out of bounds lat 2", test({ lon: 123, lat: -91 }, false));
it("out of bounds lon and lat", test({ lon: 1000, lat: 1000 }, false));
