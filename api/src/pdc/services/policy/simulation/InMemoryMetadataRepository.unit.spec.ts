import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { InMemoryMetadataRepository } from "./InMemoryMetadataRepository.ts";

describe("InMemoryMetadataRepository", () => {
  const meta = (key: string, value: number, datetime: string, policy_id = 1) => ({
    policy_id,
    key,
    value,
    datetime: new Date(datetime),
  });

  it("starts empty", async () => {
    assertEquals(await new InMemoryMetadataRepository().get(1, ["a"], new Date()), []);
  });

  it("returns the latest value at or before the datetime", async () => {
    const repo = new InMemoryMetadataRepository();
    await repo.set([meta("a", 10, "2026-01-01"), meta("a", 30, "2026-03-01"), meta("a", 20, "2026-02-01")]);

    assertEquals((await repo.get(1, ["a"], new Date("2026-02-15")))[0].value, 20);
    assertEquals((await repo.get(1, ["a"], new Date("2026-03-01")))[0].value, 30);
    assertEquals(await repo.get(1, ["a"], new Date("2025-12-31")), []);
  });

  it("breaks datetime ties with the last inserted row", async () => {
    const repo = new InMemoryMetadataRepository();
    await repo.set([meta("a", 10, "2026-01-01")]);
    await repo.set([meta("a", 15, "2026-01-01")]);
    assertEquals((await repo.get(1, ["a"]))[0].value, 15);
  });

  it("isolates policies and keys", async () => {
    const repo = new InMemoryMetadataRepository();
    await repo.set([meta("a", 1, "2026-01-01"), meta("b", 2, "2026-01-01"), meta("a", 3, "2026-01-01", 2)]);
    assertEquals((await repo.get(1, ["a", "b"])).map((r) => r.value), [1, 2]);
  });
});
