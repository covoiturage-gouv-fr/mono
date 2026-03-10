import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { get } from "@/lib/object/index.ts";

describe("get object helper", () => {
  it("should get value from object", () => {
    const obj = { a: { b: { c: 1 } } };
    assertEquals(get(obj, "a.b.c"), 1);
  });

  it("should get object from object", () => {
    const obj = { a: { b: { c: 1 } } };
    assertEquals(get(obj, "a.b"), { c: 1 });
  });

  it("should get value from indexed array", () => {
    const obj = { a: { b: { c: [1, 2, 3] } } };
    assertEquals(get(obj, "a.b.c.1"), 2);
  });

  it("should return default value if not found", () => {
    const obj = { a: { b: { c: 1 } } };
    assertEquals(get(obj, "a.b.d", 2), 2);
  });

  it("should return value from an array of objects", () => {
    const obj = { a: [{ b: 1 }, { b: 2 }] };
    assertEquals(get(obj, "a.1.b"), 2);
  });

  it("should return value from an array of objects at root level", () => {
    const arr = [{ b: 1 }, { b: 2 }];
    assertEquals(get(arr, "0.b"), 1);
    assertEquals(get(arr, "1.b"), 2);
  });

  it("should return default value if the obj is null or undefined", () => {
    assertEquals(get(null, "a.b.c", 2), 2);
    assertEquals(get(undefined, "a.b.c", 2), 2);
  });

  it("should return null or undefined if the path ends and the value is null", () => {
    const obj = { a: { b: null, c: undefined } };
    assertEquals(get(obj, "a.b"), null);
    assertEquals(get(obj, "a.c"), undefined);
    assertEquals(get(obj, "a.d", 2), 2);
  });

  it("should return undefined as defaultValue if the defaultValue is not provided", () => {
    const obj = { a: { b: 1 } };
    assertEquals(get(obj, "a.b"), 1);
    assertEquals(get(obj, "a.c", 2), 2);
    assertEquals(get(obj, "a.d"), undefined);
  });
});
