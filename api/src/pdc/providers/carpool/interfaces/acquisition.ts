import {
  ApiVersion,
  CancelCode,
  CancelMessage,
  Id,
  TermsViolationErrorDetails,
  TermsViolationErrorLabels,
  Uuid,
} from "./common.ts";
import { InsertableCarpool, UpdatableCarpool } from "./database/carpool.ts";

interface ApiVersionObject {
  api_version: ApiVersion;
}

export type RegisterRequest = InsertableCarpool & ApiVersionObject;
export type RegisterResponse = {
  created_at: Date;
  terms_violation_error_labels: TermsViolationErrorLabels;
  terms_violation_error_details: TermsViolationErrorDetails;
};

export interface PatchRequest extends UpdatableCarpool, ApiVersionObject {
  operator_id: Id;
  operator_journey_id: Uuid;
}

export interface CancelRequest extends ApiVersionObject {
  operator_id: Id;
  operator_journey_id: Uuid;
  cancel_code: CancelCode;
  cancel_message: CancelMessage;
}

export type ProcessGeoParams = {
  batchSize: number;
  from: Date;
  to: Date;
  failedOnly: boolean;
};

export type ProcessGeoResults = number;
