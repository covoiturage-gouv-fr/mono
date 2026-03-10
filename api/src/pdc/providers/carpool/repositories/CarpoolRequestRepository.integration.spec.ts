import { assertObjectMatch } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import sql, { raw } from "@/lib/pg/sql.ts";
import { LegacyDbContext, makeLegacyDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { Id } from "../interfaces/index.ts";
import { insertableCarpool } from "../mocks/database/carpool.ts";
import { insertableCarpoolCancelRequest, insertableCarpoolCreateRequest } from "../mocks/database/request.ts";
import { CarpoolRepository } from "./CarpoolRepository.ts";
import { CarpoolRequestRepository } from "./CarpoolRequestRepository.ts";

describe("CarpoolRequestRepository", () => {
  let repository: CarpoolRequestRepository;
  let carpoolRepository: CarpoolRepository;
  let db: LegacyDbContext;
  let carpool_id: Id;
  const { before, after } = makeLegacyDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new CarpoolRequestRepository(db.connection);
    carpoolRepository = new CarpoolRepository(db.connection);
    const carpool = await carpoolRepository.register(insertableCarpool);
    carpool_id = carpool._id;
  });

  afterAll(async () => {
    await after(db);
  });

  it("Should save create request", async () => {
    const data = {
      ...insertableCarpoolCreateRequest,
      carpool_id: carpool_id,
    };

    await repository.save(data);
    const result = await db.connection.getClient().query(sql`
    SELECT carpool_id, operator_id, operator_journey_id, api_version, payload FROM ${raw(repository.table)}
    WHERE carpool_id = ${carpool_id}
  `);

    assertObjectMatch(result.rows.pop(), data);
  });

  it("Should save cancel request", async () => {
    const data = {
      ...insertableCarpoolCancelRequest,
      carpool_id: carpool_id,
    };

    await repository.save(data);
    const result = await db.connection.getClient().query(sql`
    SELECT carpool_id, operator_id, operator_journey_id, api_version, cancel_code, cancel_message FROM ${
      raw(
        repository.table,
      )
    }
    WHERE carpool_id = ${carpool_id} ORDER BY created_at DESC
  `);
    assertObjectMatch(result.rows.pop(), data);
  });
});
