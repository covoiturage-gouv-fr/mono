import { assertEquals, assertThrows } from "dep:assert";
import { it } from "dep:testing-bdd";

import { StatelessContext } from "../entities/Context.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { isOperatorClassOrThrow } from "./isOperatorClassOrThrow.ts";

function setup(operator_class: string) {
  return StatelessContext.fromCarpool(1, generateCarpool({ operator_class }));
}

it("should throw if not in list", async () => {
  const ctx = setup("A");
  assertThrows(() => {
    isOperatorClassOrThrow(ctx, ["B"]);
  });
});

it("should return true if is driver", async () => {
  const ctx = setup("B");
  const res = isOperatorClassOrThrow(ctx, ["B"]);
  assertEquals(res, true);
});

it("should throw if not an array", async () => {
  const ctx = setup("B");
  assertThrows(
    () => {
      isOperatorClassOrThrow(ctx, "" as unknown as Array<string>);
    },
  );
});
