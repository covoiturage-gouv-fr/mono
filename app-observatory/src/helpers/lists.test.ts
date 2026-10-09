import { describe, expect, test } from "vitest";
import { observeList } from "./lists";

const ids = (type: string) => observeList(type).map((d) => d.id);

describe("observeList", () => {
  test("propose les niveaux plus fins que le territoire sélectionné", () => {
    expect(ids("dep")).toEqual(["com", "epci", "aom"]);
    expect(ids("com")).toEqual([]);
  });

  test("propose communes, EPCI, AOM et départements pour un territoire custom", () => {
    expect(ids("custom")).toEqual(["com", "epci", "aom", "dep"]);
  });
});
