import { assertEquals, assertThrows } from "dep:assert";
import { it } from "dep:testing-bdd";
import { TerritoryCodeInterface } from "../../interfaces/index.ts";
import { StatelessContext } from "../entities/Context.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { startsOrEndsAtOrThrow } from "./startsOrEndsAtOrThrow.ts";

function setup(start: TerritoryCodeInterface, end: TerritoryCodeInterface) {
  return StatelessContext.fromCarpool(1, generateCarpool({ start, end }));
}

it("should throw if not start or end in perimeter", async () => {
  const ctx = setup(
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
  );
  assertThrows(
    () => {
      startsOrEndsAtOrThrow(ctx, { reg: ["84"] });
    },
  );
});

it("should pass if start OR end in perimeter", async () => {
  const ctx = setup(
    { aom: "200067551", com: "74206", reg: "74", epci: "200067551" },
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
  );
  const res = startsOrEndsAtOrThrow(ctx, { aom: ["217500016"] });
  assertEquals(res, true);
});
