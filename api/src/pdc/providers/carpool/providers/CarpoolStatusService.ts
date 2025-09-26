import { provider } from "@/ilos/common/Decorators.ts";
import { LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { NotFoundException } from "../../../../ilos/common/index.ts";
import { castToStatusEnum } from "../helpers/castStatus.ts";
import { CarpoolLabel, CarpoolStatus } from "../interfaces/database/label.ts";
import { CarpoolLabelRepository } from "../repositories/CarpoolLabelRepository.ts";
import { CarpoolStatusRepository } from "../repositories/CarpoolStatusRepository.ts";

type FindByOperatorJourneyIdResult = {
  created_at: Date;
  status: CarpoolStatus;
  anomaly: Array<CarpoolLabel<unknown>>;
  fraud: Array<CarpoolLabel<unknown>>;
  terms: Array<CarpoolLabel<unknown>>;
  legacy_id: number;
};

type FindByParams = {
  operator_id: number;
  status: Array<Partial<CarpoolStatus>>;
  start: Date;
  end: Date;
  limit: number;
  offset: number;
};

type FindByResult = Array<{ operator_journey_id: string }>;

@provider()
export class CarpoolStatusService {
  constructor(
    protected connection: LegacyPostgresConnection,
    protected statusRepository: CarpoolStatusRepository,
    protected labelRepository: CarpoolLabelRepository,
  ) {}

  async findByOperatorJourneyId(
    operator_id: number,
    operator_journey_id: string,
    api_version: string,
  ): Promise<FindByOperatorJourneyIdResult> {
    const statusResult = await this.statusRepository.getStatusByOperatorJourneyId(operator_id, operator_journey_id);
    if (!statusResult) {
      throw new NotFoundException();
    }

    const { created_at, legacy_id, ...status } = statusResult;

    const anomaly = await this.labelRepository.findAnomalyByOperatorJourneyId(
      operator_id,
      operator_journey_id,
    );

    const fraud = await this.labelRepository.findFraudByOperatorJourneyId(
      api_version,
      operator_id,
      operator_journey_id,
    );

    const terms = await this.labelRepository.findTermsByOperatorJourneyId(
      operator_id,
      operator_journey_id,
    );

    return {
      created_at,
      status,
      anomaly,
      fraud,
      terms,
      legacy_id,
    };
  }

  castToStatusResult(operator_journey_id: string, statusResult: FindByOperatorJourneyIdResult) {
    const status = castToStatusEnum(statusResult.status);

    return {
      operator_journey_id,
      status,
      created_at: statusResult.created_at,
      fraud_error_labels: statusResult.fraud.map((f) =>
        (!!f.label && f.label == "interoperator_overlap_trip") ? "interoperator_overlap" : f.label
      ),
      anomaly_error_details: statusResult.anomaly as any,
      terms_violation_details: statusResult.terms.map((f) => f.label),
      journey_id: statusResult.legacy_id,
    };
  }

  async findBy(data: FindByParams): Promise<FindByResult> {
    return this.statusRepository.getOperatorJourneyIdByStatus(data);
  }
}
