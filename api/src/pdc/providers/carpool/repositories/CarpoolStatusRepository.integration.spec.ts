import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import sql, { raw } from "@/lib/pg/sql.ts";
import {
  CarpoolAcquisitionStatusEnum,
  CarpoolAnomalyStatusEnum,
  CarpoolFraudStatusEnum,
} from "@/pdc/providers/carpool/interfaces/common.ts";
import { LegacyDbContext, makeLegacyDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { Id } from "../interfaces/index.ts";
import { insertableCarpool } from "../mocks/database/carpool.ts";
import { insertableAcquisitionStatus } from "../mocks/database/status.ts";
import { CarpoolRepository } from "./CarpoolRepository.ts";
import { CarpoolStatusRepository } from "./CarpoolStatusRepository.ts";

describe("CarpoolStatusRepository", () => {
  let repository: CarpoolStatusRepository;
  let carpoolRepository: CarpoolRepository;
  let db: LegacyDbContext;
  let carpool_id: Id;

  const { before, after } = makeLegacyDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new CarpoolStatusRepository(db.connection);
    carpoolRepository = new CarpoolRepository(db.connection);
    const carpool = await carpoolRepository.register(insertableCarpool);
    carpool_id = carpool._id;
  });

  afterAll(async () => {
    await after(db);
  });

  it("Should create acquisition status", async () => {
    const data = {
      ...insertableAcquisitionStatus,
      carpool_id: carpool_id,
    };

    await repository.saveAcquisitionStatus(data);
    const result = await db.connection.getClient().query(sql`
      SELECT * FROM ${raw(repository.table)}
      WHERE carpool_id = ${carpool_id}
    `);

    assertEquals(result.rows.pop()?.acquisition_status, data.status);
  });

  it("Should create acquisition term violation error", async () => {
    await repository.setTermsViolationErrorLabels(carpool_id, [
      "test",
      "test2",
    ]);
    const result = await db.connection.getClient().query(sql`
      SELECT * FROM ${raw(repository.termsViolationErrorLabelTable)}
      WHERE carpool_id = ${carpool_id}
    `);
    assertEquals(result.rows.pop()?.labels, ["test", "test2"]);
    await repository.setTermsViolationErrorLabels(carpool_id, []);
    const result2 = await db.connection.getClient().query(sql`
      SELECT * FROM ${raw(repository.termsViolationErrorLabelTable)}
      WHERE carpool_id = ${carpool_id}
    `);
    assertEquals(result2.rows.pop()?.labels, []);
  });

  it("Should get status", async () => {
    const result = await repository.getStatusByOperatorJourneyId(
      insertableCarpool.operator_id,
      insertableCarpool.operator_journey_id,
    );
    assertEquals(result, {
      acquisition_status: CarpoolAcquisitionStatusEnum.Canceled,
      anomaly_status: CarpoolAnomalyStatusEnum.Pending,
      fraud_status: CarpoolFraudStatusEnum.Pending,
      created_at: result?.created_at,
      legacy_id: result?.legacy_id,
    });
  });

  it("Should list operator journey_id", async () => {
    const start = new Date(insertableCarpool.start_datetime.valueOf() - 1000);
    const end = new Date(insertableCarpool.start_datetime.valueOf() + 1000);
    const result = await repository.getOperatorJourneyIdByStatus({
      operator_id: insertableCarpool.operator_id,
      start,
      end,
      limit: 1,
      offset: 0,
      status: [
        {
          acquisition_status: CarpoolAcquisitionStatusEnum.Canceled,
        },
      ],
    });
    assertEquals(result, [{
      operator_journey_id: insertableCarpool.operator_journey_id,
    }]);
  });
});
