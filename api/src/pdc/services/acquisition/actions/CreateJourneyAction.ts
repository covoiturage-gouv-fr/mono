import {
  ContextType,
  handler,
  InvalidRequestException,
  ParseErrorException,
  UnprocessableRequestException,
  ValidatorInterfaceResolver,
} from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { get } from "@/lib/object/index.ts";
import { CarpoolAcquisitionService } from "@/pdc/providers/carpool/index.ts";
import { RegisterRequest, RegisterResponse } from "@/pdc/providers/carpool/interfaces/acquisition.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { handlerConfig, ParamsInterface, PayloadV3, ResultInterface } from "../contracts/create.contract.ts";
import { v3alias } from "../contracts/create.schema.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      operator: "operator.acquisition.create",
    }),
  ],
  apiRoute: {
    path: "/journeys",
    method: "POST",
    successHttpCode: 201,
    rateLimiter: {
      key: "rl-acquisition",
      limit: 20_000,
      windowMinute: 1,
    },
    rpcAnswerOnSuccess: true,
    rpcAnswerOnFailure: true,
  },
})
export class CreateJourneyAction extends AbstractAction {
  constructor(
    private validator: ValidatorInterfaceResolver,
    private acquisitionService: CarpoolAcquisitionService,
  ) {
    super();
  }

  protected override async handle(params: ParamsInterface, context: ContextType): Promise<ResultInterface> {
    try {
      await this.validateParams(params);
      const request = this.convertPayloadToRequest(context, params);
      const results = await this.acquisitionService.registerRequest(request);
      this.validateResults(context, results);

      return {
        operator_journey_id: params.operator_journey_id,
        created_at: results.created_at,
      };
    } catch (e) {
      logger.error(e.message, {
        operator_journey_id: params.operator_journey_id,
        operator_id: this.getOperatorId(context),
      });

      throw e;
    }
  }

  protected async validateParams(journey: ParamsInterface): Promise<void> {
    await this.validator.validate(journey, v3alias);

    const now = new Date();
    const start = get(journey, "start.datetime") as Date;
    const end = get(journey, "end.datetime") as Date;

    if (end > now || start > end) {
      throw new ParseErrorException("Journeys cannot happen in the future");
    }

    if (
      !journey.driver?.identity?.phone &&
      !journey.driver?.identity?.phone_trunc
    ) {
      throw new InvalidRequestException(`driver.identity should have a phone or phone_trunc`);
    }
    if (
      !journey.passenger?.identity?.phone &&
      !journey.passenger?.identity?.phone_trunc
    ) {
      throw new InvalidRequestException(`passenger.identity should have a phone or phone_trunc`);
    }
  }

  protected validateResults(context: ContextType, result: RegisterResponse): void {
    if (result.terms_violation_error_labels.length) {
      throw new UnprocessableRequestException({
        terms_violation_labels: result.terms_violation_error_labels,
        terms_violation_error_details: result.terms_violation_error_details,
      });
    }
  }

  protected getOperatorId(context: ContextType): RegisterRequest["operator_id"] {
    return get(context, "call.user.operator_id") as RegisterRequest["operator_id"];
  }

  protected convertPayloadToRequest(context: ContextType, payload: PayloadV3): RegisterRequest {
    const request: RegisterRequest = {
      api_version: context.call?.api_version_range || "3",
      operator_id: this.getOperatorId(context),
      operator_journey_id: payload.operator_journey_id,
      operator_trip_id: payload.operator_trip_id,
      operator_class: payload.operator_class,
      start_datetime: payload.start.datetime,
      start_position: {
        lat: payload.start.lat,
        lon: payload.start.lon,
      },
      end_datetime: payload.end.datetime,
      end_position: {
        lat: payload.end.lat,
        lon: payload.end.lon,
      },
      distance: payload.distance,
      licence_plate: payload.licence_plate || null,
      driver_identity_key: payload.driver.identity.identity_key,
      driver_operator_user_id: payload.driver.identity.operator_user_id,
      driver_phone: payload.driver.identity.phone || null,
      driver_phone_trunc: payload.driver.identity.phone_trunc || null,
      driver_travelpass_name: payload.driver.identity.travel_pass?.name || null,
      driver_travelpass_user_id: payload.driver.identity.travel_pass?.user_id || null,
      driver_revenue: payload.driver.revenue,
      passenger_identity_key: payload.passenger.identity.identity_key,
      passenger_operator_user_id: payload.passenger.identity.operator_user_id,
      passenger_phone: payload.passenger.identity.phone || null,
      passenger_phone_trunc: payload.passenger.identity.phone_trunc || null,
      passenger_travelpass_name: payload.passenger.identity.travel_pass?.name || null,
      passenger_travelpass_user_id: payload.passenger.identity.travel_pass?.user_id || null,
      passenger_over_18: payload.passenger.identity.over_18 ?? true,
      passenger_seats: payload.passenger.seats ?? 1,
      passenger_contribution: payload.passenger.contribution,
      passenger_payments: payload.passenger.payments || null,
      incentives: payload.incentives,
    };

    return request;
  }
}
