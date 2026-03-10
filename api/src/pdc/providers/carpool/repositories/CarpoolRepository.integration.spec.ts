import { assertObjectMatch } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { LegacyDbContext, makeLegacyDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { Id } from "../interfaces/index.ts";
import { insertableCarpool, updatableCarpool } from "../mocks/database/carpool.ts";
import { CarpoolRepository } from "./CarpoolRepository.ts";

describe("Carpool Repository", () => {
  let repository: CarpoolRepository;
  let db: LegacyDbContext;

  const { before, after } = makeLegacyDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new CarpoolRepository(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  async function getCarpool(id: Id) {
    const result = await db.connection.getClient().query({
      text: `
        SELECT *,
          json_build_object(
            'lat', ST_Y(start_position::geometry),
            'lon', ST_X(start_position::geometry)
          ) AS start_position,
          json_build_object(
            'lat', ST_Y(end_position::geometry),
            'lon', ST_X(end_position::geometry)
          ) AS end_position
        FROM ${repository.table}
        WHERE _id = $1
      `,
      values: [id],
    });

    const incentiveResult = await db.connection.getClient().query({
      text: `SELECT idx, siret, amount FROM ${repository.incentiveTable} WHERE carpool_id = $1`,
      values: [id],
    });

    return {
      ...result.rows.pop(),
      incentives: incentiveResult.rows.map(({ idx, siret, amount }) => ({
        index: idx,
        siret,
        amount,
      })),
    };
  }

  it("Should create carpool", async () => {
    const data = { ...insertableCarpool };

    const carpool = await repository.register(data);
    const result = await getCarpool(carpool._id);

    assertObjectMatch({ ...result, conflict: false }, { ...carpool, ...data });
  });

  it("Should do nothing on duplicate carpool", async () => {
    const data = { ...insertableCarpool };

    const carpool = await repository.register(data);
    const result = await getCarpool(carpool._id);

    assertObjectMatch({ ...result, conflict: true }, { ...carpool, ...data });
  });

  it("Should update acquisition", async () => {
    const insertData = {
      ...insertableCarpool,
      operator_journey_id: "journey_2",
    };

    const carpool = await repository.register(insertData);

    const updateData = { ...updatableCarpool };
    await repository.update(
      insertData.operator_id,
      insertData.operator_journey_id,
      updateData,
    );

    const result = await getCarpool(carpool._id);
    assertObjectMatch({ ...result, conflict: false }, { ...carpool, ...insertData, ...updateData });
  });
});
