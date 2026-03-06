import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import sql, { raw } from "@/lib/pg/sql.ts";
import { LegacyDbContext, makeLegacyDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { insertableCarpool } from "../mocks/database/carpool.ts";
import { CarpoolLabelRepository } from "./CarpoolLabelRepository.ts";
import { CarpoolRepository } from "./CarpoolRepository.ts";

describe("Carpool Label Repository", () => {
  let repository: CarpoolRepository;
  let labelRepository: CarpoolLabelRepository;
  let db: LegacyDbContext;
  let carpool_id: number;

  // anomaly
  const anomaly_label_1 = "temporal_overlap_anomaly";

  // terms
  const terms_label_1 = "distance_too_short";

  // frauds
  const fraud_label_1_V3 = "interoperator_fraud";

  const fraud_label_1_V3_1 = "interoperator_overlap_trip";
  const fraud_label_2_V3_1 = "interoperator_too_many_trips_by_day";

  const { before, after } = makeLegacyDbBeforeAfter();

  const insertTermLabels = async (carpool_id: number) => {
    await db.connection.getClient().query(
      sql`INSERT INTO ${raw(labelRepository.termsTable)} (carpool_id, labels) VALUES (${carpool_id}, ${[
        terms_label_1,
      ]})`,
    );
  };

  const insertFraudLabels = async (carpool_id: number) => {
    await db.connection.getClient().query(
      sql`INSERT INTO ${
        raw(labelRepository.fraudTable)
      } (carpool_id, label) VALUES (${carpool_id}, ${fraud_label_1_V3_1})`,
    );
    await db.connection.getClient().query(
      sql`INSERT INTO ${
        raw(labelRepository.fraudTable)
      } (carpool_id, label) VALUES (${carpool_id},${fraud_label_2_V3_1})`,
    );
  };

  const insertAnomalyLabel = async (carpool_id: number) => {
    await db.connection.getClient().query(
      sql`INSERT INTO ${
        raw(labelRepository.anomalyTable)
      } (carpool_id, label) VALUES (${carpool_id}, ${anomaly_label_1})`,
    );
  };

  beforeAll(async () => {
    db = await before();
    repository = new CarpoolRepository(db.connection);
    labelRepository = new CarpoolLabelRepository(db.connection);
    const { _id: carpool_id } = await repository.register(insertableCarpool);
    await insertAnomalyLabel(carpool_id);
    await insertFraudLabels(carpool_id);
    await insertTermLabels(carpool_id);
  });

  afterAll(async () => {
    await after(db);
  });

  // anomaly
  it("Should read carpool anomaly label", async () => {
    const result = await labelRepository.findAnomalyByOperatorJourneyId(
      insertableCarpool.operator_id,
      insertableCarpool.operator_journey_id,
    );
    assertEquals(result, [{
      label: anomaly_label_1,
      metas: {
        conflicting_operator_journey_id: null,
        overlap_duration_ratio: null,
      },
    }]);
  });

  // terms
  it("Should read carpool terms label", async () => {
    const result = await labelRepository.findTermsByOperatorJourneyId(
      insertableCarpool.operator_id,
      insertableCarpool.operator_journey_id,
    );
    assertEquals(result, [{
      label: terms_label_1,
    }]);
  });

  // fraud V3
  it("Should read carpool fraud label and returns empty array if none for api v3", async () => {
    // Arrange
    await repository.register({ ...insertableCarpool, operator_journey_id: "operator_journey_id-4" });

    // Act
    const result = await labelRepository.findFraudByOperatorJourneyId(
      "v3",
      insertableCarpool.operator_id,
      "operator_journey_id-4",
    );

    // Assert
    assertEquals(result, []);
  });

  // fraud V3.1
  it("Should read carpool fraud label and returns 2 labels for api v3.1", async () => {
    const result = await labelRepository.findFraudByOperatorJourneyId(
      "v3.1",
      insertableCarpool.operator_id,
      insertableCarpool.operator_journey_id,
    );
    assertEquals(result, [
      { label: fraud_label_1_V3_1 },
      { label: fraud_label_2_V3_1 },
    ]);
  });

  it("Should read carpool fraud label and returns empty array if none for api v3", async () => {
    // Arrange
    await repository.register({ ...insertableCarpool, operator_journey_id: "operator_journey_id-4" });

    // Act
    const result = await labelRepository.findFraudByOperatorJourneyId(
      "v3.1",
      insertableCarpool.operator_id,
      "operator_journey_id-4",
    );

    // Assert
    assertEquals(result, []);
  });
});
