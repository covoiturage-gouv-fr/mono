import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { IncentiveStateEnum, IncentiveStatusEnum, SerializedIncentiveInterface } from "../interfaces/index.ts";
import { InMemoryIncentiveRepository } from "./InMemoryIncentiveRepository.ts";

function incentive(journey: string, datetime: string, amount: number): SerializedIncentiveInterface<undefined> {
  return {
    _id: undefined,
    policy_id: 1,
    operator_id: 2,
    operator_journey_id: journey,
    datetime: new Date(datetime),
    statelessAmount: amount,
    statefulAmount: amount,
    status: IncentiveStatusEnum.Draft,
    state: IncentiveStateEnum.Regular,
    meta: [],
  };
}

async function drafts(repo: InMemoryIncentiveRepository, to: string, from?: string) {
  const out = [];
  for await (const batch of repo.findDraftIncentive(new Date(to), 2, from ? new Date(from) : undefined)) {
    out.push(...batch);
  }
  return out;
}

describe("InMemoryIncentiveRepository", () => {
  it("upserts on policy/operator/journey, keeping id and datetime", async () => {
    const repo = new InMemoryIncentiveRepository();
    await repo.createOrUpdateMany([incentive("a", "2026-01-01T10:00:00Z", 100)]);
    await repo.createOrUpdateMany([incentive("a", "2026-01-02T10:00:00Z", 0)]);

    const [row] = repo.all();
    assertEquals(repo.all().length, 1);
    assertEquals(row._id, 1);
    assertEquals(row.datetime, new Date("2026-01-01T10:00:00Z"));
    assertEquals([row.statelessAmount, row.state], [0, IncentiveStateEnum.Null]);
  });

  it("lists drafts by datetime within [from, to] in batches, as copies", async () => {
    const repo = new InMemoryIncentiveRepository();
    await repo.createOrUpdateMany([
      incentive("c", "2026-01-01T12:00:00Z", 1),
      incentive("a", "2026-01-01T08:00:00Z", 1),
      incentive("b", "2026-01-01T10:00:00Z", 1),
      incentive("d", "2026-01-01T14:00:00Z", 1),
    ]);

    const rows = await drafts(repo, "2026-01-01T12:00:00Z", "2026-01-01T09:00:00Z");
    assertEquals(rows.map((r) => r.operator_journey_id), ["b", "c"]);

    rows[0].statefulAmount = 999;
    assertEquals(repo.all().find((r) => r.operator_journey_id === "b")?.statefulAmount, 1);
  });

  it("follows the finalize transitions draft -> pending -> validated", async () => {
    const repo = new InMemoryIncentiveRepository();
    await repo.createOrUpdateMany([
      incentive("a", "2026-01-01T08:00:00Z", 100),
      incentive("b", "2026-01-01T09:00:00Z", 100),
    ]);
    const rows = await drafts(repo, "2026-01-01T12:00:00Z");

    await repo.updateStatefulAmount([{ ...rows[0], statefulAmount: 0 }, rows[1]], IncentiveStatusEnum.Pending);
    await repo.setStatus(new Date("2026-01-01T06:00:00Z"), new Date("2026-01-01T12:00:00Z"));

    const [a, b] = repo.all();
    assertEquals([a.status, a.statefulAmount, a.state], [IncentiveStatusEnum.Validated, 0, IncentiveStateEnum.Null]);
    assertEquals([b.status, b.statefulAmount, b.state], [
      IncentiveStatusEnum.Validated,
      100,
      IncentiveStateEnum.Regular,
    ]);
    assertEquals(await drafts(repo, "2026-01-02T00:00:00Z"), []);
  });

  it("unlocks pending incentives back to draft on failure", async () => {
    const repo = new InMemoryIncentiveRepository();
    await repo.createOrUpdateMany([incentive("a", "2026-01-01T08:00:00Z", 100)]);
    const rows = await drafts(repo, "2026-01-02T00:00:00Z");
    await repo.updateStatefulAmount(rows, IncentiveStatusEnum.Pending);
    await repo.setStatus(new Date("2026-01-01T00:00:00Z"), new Date("2026-01-02T00:00:00Z"), true);
    assertEquals(repo.all()[0].status, IncentiveStatusEnum.Draft);
  });

  it("disables incentives of excluded carpools in the window only", async () => {
    const repo = new InMemoryIncentiveRepository(new Set(["2/a", "2/late"]));
    await repo.createOrUpdateMany([
      incentive("a", "2026-01-01T08:00:00Z", 100),
      incentive("b", "2026-01-01T09:00:00Z", 100),
      incentive("late", "2026-01-01T13:00:00Z", 100),
    ]);

    await repo.disableOnExcludedCarpool(new Date("2026-01-01T06:00:00Z"), new Date("2026-01-01T12:00:00Z"));

    const [a, b, late] = repo.all();
    assertEquals([a.status, a.state], [IncentiveStatusEnum.Error, IncentiveStateEnum.Disabled]);
    assertEquals(b.status, IncentiveStatusEnum.Draft);
    assertEquals(late.status, IncentiveStatusEnum.Draft);
    assertEquals((await drafts(repo, "2026-01-02T00:00:00Z")).map((r) => r.operator_journey_id), ["b", "late"]);
  });
});
