import { assert, assertFalse } from "dep:assert";
import { it } from "dep:testing-bdd";

import { objectidCustomFormat } from "./objectidCustomFormat.ts";

it("valid ObjectId", () => {
  const id = "5d07eabd57ce4d70ae6a8508";
  assert((objectidCustomFormat as any)(id));
});

it("valid ObjectId uppercase", () => {
  const id = "5d07eb19990207328440c338".toUpperCase();
  assert((objectidCustomFormat as any)(id));
});

it("too short", () => {
  const id = "5d07eb199902";
  assertFalse((objectidCustomFormat as any)(id));
});

it("too long", () => {
  const id = "5d07eabd57ce4d70ae6a8508d57ce4d7";
  assertFalse((objectidCustomFormat as any)(id));
});

it("wrong chars", () => {
  const id = "5d07eb1-.^0207328440c338";
  assertFalse((objectidCustomFormat as any)(id));
});
