import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";

import { TerritoryCodeInterface } from "../../interfaces/index.ts";
import { StatelessContext } from "../entities/Context.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { endsAt, startsAt } from "./position.ts";

function setup(start: TerritoryCodeInterface, end: TerritoryCodeInterface) {
  return StatelessContext.fromCarpool(1, generateCarpool({ start, end }));
}

it("should return false if start list is empty", async () => {
  const ctx = setup(
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
  );
  const res = startsAt(ctx, {});
  assertEquals(res, false);
});

it("should return false if starts not in list", async () => {
  const ctx = setup(
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
  );
  const res = startsAt(ctx, { com: ["91377"] });
  assertEquals(res, false);
});

it("should return true if starts in list", async () => {
  const ctx = setup(
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
  );
  const res = startsAt(ctx, { com: ["91471"] });
  assertEquals(res, true);
});

it("should return false if ends not in list", async () => {
  const ctx = setup(
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
  );
  const res = endsAt(ctx, { epci: ["999056233"] });
  assertEquals(res, false);
});

it("should return true if ends in list", async () => {
  const ctx = setup(
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
    { aom: "217500016", com: "91471", reg: "11", epci: "200056232" },
  );
  const res = endsAt(ctx, { epci: ["200056232"] });
  assertEquals(res, true);
});
