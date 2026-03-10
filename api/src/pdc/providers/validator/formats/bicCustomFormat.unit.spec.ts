import { assert, assertFalse } from "dep:assert";
import { it } from "dep:testing-bdd";
import { bicCustomFormat } from "./bicCustomFormat.ts";

it("valid BIC short string", () => {
  const bic = "ABNANL2A";
  assert((bicCustomFormat as any)(bic));
});

it("valid BIC padding X", () => {
  const bic = "ABNANL2AXXX";
  assert((bicCustomFormat as any)(bic));
});

it("invalid BIC too short", () => {
  const bic = "012345";
  assertFalse((bicCustomFormat as any)(bic));
});

it("invalid BIC too long", () => {
  const bic = "00331234567890";
  assertFalse((bicCustomFormat as any)(bic));
});
