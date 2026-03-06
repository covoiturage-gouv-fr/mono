import { assertEquals, assertThrows } from "dep:assert";
import { it } from "dep:testing-bdd";
import { TerritoryCodeInterface } from "../../interfaces/index.ts";
import { StatelessContext } from "../entities/Context.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { startsAndEndsAtOrThrow } from "./startsAndEndsAtOrThrow.ts";

function setup(start: TerritoryCodeInterface, end: TerritoryCodeInterface) {
  return StatelessContext.fromCarpool(1, generateCarpool({ start, end }));
}

it("should throw if start and end not in perimeter", async () => {
  const ctx = setup(
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
  );
  assertThrows(
    () => {
      startsAndEndsAtOrThrow(ctx, { reg: ["84"] });
    },
  );
});

it("should pass if start and end is in perimeter", async () => {
  const ctx = setup(
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
  );
  const res = startsAndEndsAtOrThrow(ctx, { aom: ["217500016"] });
  assertEquals(res, true);
});
