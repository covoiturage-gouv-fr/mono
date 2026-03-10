import { assertEquals, assertThrows } from "dep:assert";
import { it } from "dep:testing-bdd";
import { StatelessContext } from "../entities/Context.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { isOperatorOrThrow } from "./isOperatorOrThrow.ts";

function setup(operator_uuid: string) {
  return StatelessContext.fromCarpool(1, generateCarpool({ operator_uuid }));
}

it("should throw if not in list", async () => {
  const ctx = setup("1234");
  assertThrows(() => {
    isOperatorOrThrow(ctx, ["5678"]);
  });
});

it("should return true if is driver", async () => {
  const ctx = setup("1234");
  const res = isOperatorOrThrow(ctx, ["1234"]);
  assertEquals(res, true);
});

it("should throw if not an array", async () => {
  const ctx = setup("1234");
  assertThrows(
    () => {
      isOperatorOrThrow(ctx, "" as unknown as Array<string>);
    },
  );
});
