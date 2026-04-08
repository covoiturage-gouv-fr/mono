import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { TerritoryRepository } from "./TerritoryRepository.ts";

describe("TerritoryRepository.getTerritoryNamesBatch", () => {
  // Create repository with a null connection -- batch validation
  // short-circuits before any DB call for invalid inputs.
  // deno-lint-ignore no-explicit-any
  const repository = new TerritoryRepository(null as any);

  it("should return empty array for invalid type", async () => {
    const result = await repository.getTerritoryNamesBatch("invalid_type", ["code1"]);
    assertEquals(result, []);
  });

  it("should return empty array for empty codes", async () => {
    const result = await repository.getTerritoryNamesBatch("com", []);
    assertEquals(result, []);
  });
});
