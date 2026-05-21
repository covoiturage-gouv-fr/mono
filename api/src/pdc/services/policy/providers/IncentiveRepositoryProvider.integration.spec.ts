import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import sql, { raw } from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";

import { IncentiveStateEnum, IncentiveStatusEnum } from "../interfaces/index.ts";
import { IncentiveRepositoryProvider } from "./IncentiveRepositoryProvider.ts";

describe("IncentiveRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: IncentiveRepositoryProvider | undefined;
  const { before, after } = makeDenoDbBeforeAfter();
  beforeAll(async () => {
    db = await before();
    repository = new IncentiveRepositoryProvider(
      db.connection,
    );
  });
  afterAll(async () => {
    repository = undefined;
    await after(db);
  });

  it("Should create many incentives", async () => {
    const incentives = [
      {
        _id: undefined,
        policy_id: 0,
        operator_id: 1,
        operator_journey_id: "operator_journey_id-1",
        datetime: new Date("2024-03-15"),
        statelessAmount: 0,
        statefulAmount: 0,
        status: IncentiveStatusEnum.Draft,
        state: IncentiveStateEnum.Regular,
        meta: [],
      },

      // same operator_id / operator_journey_id
      // -> should update the existing incentive
      {
        _id: undefined,
        policy_id: 0,
        operator_id: 1,
        operator_journey_id: "operator_journey_id-1",
        datetime: new Date("2024-03-15"),
        statelessAmount: 100,
        statefulAmount: 100,
        status: IncentiveStatusEnum.Draft,
        state: IncentiveStateEnum.Regular,
        meta: [],
      },
      {
        _id: undefined,
        policy_id: 0,
        operator_id: 1,
        operator_journey_id: "operator_journey_id-2",
        datetime: new Date("2024-03-16"),
        statelessAmount: 200,
        statefulAmount: 200,
        status: IncentiveStatusEnum.Draft,
        state: IncentiveStateEnum.Regular,
        meta: [
          {
            uuid: "uuid",
            key: "key",
          },
        ],
      },
    ];

    await repository?.createOrUpdateMany(incentives);

    type IncentiveRow = { operator_journey_id: string; result: number; state: string };
    const incentiveResults = await db.connection.query<IncentiveRow>(sql`
      SELECT * FROM ${raw(repository!.incentivesTable)} WHERE policy_id = ${0}
    `);
    assertEquals(incentiveResults.length, 2);
    assertEquals(
      incentiveResults.find((i) => i.operator_journey_id === "operator_journey_id-1")?.result,
      100,
    );
    assertEquals(
      incentiveResults.find((i) => i.operator_journey_id === "operator_journey_id-2")?.result,
      200,
    );
  });

  it("Should update many incentives", async () => {
    const incentives = [
      // update
      {
        _id: undefined,
        policy_id: 0,
        operator_id: 1,
        operator_journey_id: "operator_journey_id-1",
        datetime: new Date("2024-03-15"),
        statelessAmount: 0,
        statefulAmount: 0,
        status: IncentiveStatusEnum.Draft,
        state: IncentiveStateEnum.Regular,
        meta: [
          {
            uuid: "uuid",
            key: "key",
          },
        ],
      },

      // update
      {
        _id: undefined,
        policy_id: 0,
        operator_id: 1,
        operator_journey_id: "operator_journey_id-2",
        datetime: new Date("2024-03-16"),
        statelessAmount: 500,
        statefulAmount: 500,
        status: IncentiveStatusEnum.Draft,
        state: IncentiveStateEnum.Regular,
        meta: [],
      },

      // create
      {
        _id: undefined,
        policy_id: 0,
        operator_id: 1,
        operator_journey_id: "operator_journey_id-3",
        datetime: new Date("2024-03-16"),
        statelessAmount: 100,
        statefulAmount: 100,
        status: IncentiveStatusEnum.Draft,
        state: IncentiveStateEnum.Regular,
        meta: [],
      },
    ];

    await repository?.createOrUpdateMany(incentives);

    type IncentiveRow = { operator_journey_id: string; result: number; state: string };
    const incentiveResults = await db.connection.query<IncentiveRow>(sql`
      SELECT * FROM ${raw(repository!.incentivesTable)} WHERE policy_id = ${0}
    `);

    assertEquals(incentiveResults.length, 3);
    assertEquals(
      incentiveResults.find((i) => i.operator_journey_id === "operator_journey_id-1")?.result,
      0,
    );
    assertEquals(
      incentiveResults.find((i) => i.operator_journey_id === "operator_journey_id-1")?.state,
      "null",
    );
    assertEquals(
      incentiveResults.find((i) => i.operator_journey_id === "operator_journey_id-2")?.result,
      500,
    );
    assertEquals(
      incentiveResults.find((i) => i.operator_journey_id === "operator_journey_id-3")?.result,
      100,
    );
  });

  it("Should update many incentives amount", async () => {
    type IncentiveRow = { state: string };
    const incentives = await db.connection.query<Record<string, unknown>>(sql`
      SELECT * FROM ${raw(repository!.incentivesTable)} WHERE policy_id = ${0}
    `);

    const data = incentives.map((i) => ({ ...i, statefulAmount: 0 }));
    await repository?.updateStatefulAmount(data as any);

    const incentiveResults = await db.connection.query<IncentiveRow>(sql`
      SELECT * FROM ${raw(repository!.incentivesTable)} WHERE policy_id = ${0}
    `);

    assertEquals(incentiveResults.length, 3);
    assertEquals(
      incentiveResults.filter((i) => i.state === "null").length,
      3,
    );
  });
});
