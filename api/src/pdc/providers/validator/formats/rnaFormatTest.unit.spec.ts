import { assert, assertFalse } from "dep:assert";
import { it } from "dep:testing-bdd";

import { rnaCustomFormat } from "./rnaCustomFormat.ts";

it("valid RNA", () => {
  const rna = "W802005251";
  assert((rnaCustomFormat as any)(rna));
});

it("too short", () => {
  const rna = "W12345";
  assertFalse((rnaCustomFormat as any)(rna));
});

it("too long", () => {
  const rna = "W00331234567890";
  assertFalse((rnaCustomFormat as any)(rna));
});
