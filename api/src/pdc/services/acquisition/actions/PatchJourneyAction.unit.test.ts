import { ContextType } from "@/ilos/common/index.ts";
import { CarpoolAcquisitionService } from "@/pdc/providers/carpool/index.ts";
import { PatchRequest } from "@/pdc/providers/carpool/interfaces/acquisition.ts";
import { OperatorClass, Position } from "@/pdc/providers/carpool/interfaces/common.ts";
import { PatchJourneyAction } from "@/pdc/services/acquisition/actions/PatchJourneyAction.ts";
import { ParamsInterface } from "@/pdc/services/acquisition/contracts/patch.contract.ts";
import { assertEquals } from "dep:assert";
import { beforeAll, describe, it } from "dep:testing-bdd";

describe("PatchJourneyAction", () => {
  class TestPatchJourneyAction extends PatchJourneyAction {
    public testConvertPayloadToRequest(context: ContextType, params: ParamsInterface): PatchRequest {
      return this.convertPayloadToRequest(context, params);
    }

    public testWrapDatetime(params: ParamsInterface, type: "start" | "end"): { [key: string]: Date | Position } {
      return this.wrapDatetime(params, type);
    }

    public testWrapApiVersion(context: ContextType): { api_version: string } {
      return this.wrapApiVersion(context);
    }

    public testWrapOperatorId(context: ContextType): { operator_id: number } {
      return this.wrapOperatorId(context);
    }

    public testWrapOperatorTripId(params: ParamsInterface): { operator_trip_id?: string } {
      return this.wrapOperatorTripId(params);
    }

    public testWrapOperatorClass(params: ParamsInterface): { operator_class?: OperatorClass } {
      return this.wrapOperatorClass(params);
    }
  }

  // ---------------------------------------------------------------------------
  // SETUP
  // ---------------------------------------------------------------------------
  const context = {
    call: {
      user: {
        operator_id: 1,
      },
      api_version_range: "3",
    },
  } as ContextType;

  const params: ParamsInterface = {
    operator_journey_id: "fad0445a-d839-47cc-bc6e-81c2001a760d",
    operator_trip_id: "f6258505-2fbf-4950-aeac-e914da0fe3c6",
    operator_class: OperatorClass.C,
    incentives: [],
    start: { lat: 0, lon: 0, datetime: new Date("2024-01-01T09:00:00+0100") },
    end: { lat: 0, lon: 0, datetime: new Date("2024-01-01T10:00:00+0100") },
    distance: 10_000,
  };

  let action: TestPatchJourneyAction;

  beforeAll(() => {
    const acquisitionService = {} as CarpoolAcquisitionService;
    action = new TestPatchJourneyAction(acquisitionService);
  });

  // ---------------------------------------------------------------------------
  // TESTS
  // ---------------------------------------------------------------------------
  it("should convert payload to request", () => {
    const request: PatchRequest = action.testConvertPayloadToRequest(context, params);

    assertEquals(request, {
      operator_journey_id: params.operator_journey_id,
      operator_trip_id: params.operator_trip_id,
      operator_class: params.operator_class,
      incentives: params.incentives,
      start_datetime: params.start.datetime,
      start_position: { lat: params.start.lat, lon: params.start.lon },
      end_datetime: params.end.datetime,
      end_position: { lat: params.end.lat, lon: params.end.lon },
      distance: params.distance,
      operator_id: 1,
      api_version: "3",
    });
  });

  it("should wrap datetime", () => {
    const start = action.testWrapDatetime(params, "start");
    assertEquals(start, {
      start_datetime: params.start.datetime,
      start_position: { lat: params.start.lat, lon: params.start.lon },
    });
  });

  it("should fail on wrong datetime key", () => {
    try {
      // @ts-expect-error unsupported datetime key
      action.testWrapDatetime(params, "nawak");
    } catch (e) {
      assertEquals(e.message, "Missing 'nawak' parameter");
    }
  });

  it("should wrap api version", () => {
    const apiVersion = action.testWrapApiVersion(context);
    assertEquals(apiVersion, { api_version: "3" });
  });

  it("should wrap operator id", () => {
    const operatorId = action.testWrapOperatorId(context);
    assertEquals(operatorId, { operator_id: 1 });
  });

  it("should fail on missing operator id", () => {
    try {
      // @ts-expect-error missing operator id
      action.testWrapOperatorId({ call: { user: {} } });
    } catch (e) {
      assertEquals(e.message, "Missing 'operator_id' in context");
    }
  });

  it("should wrap operator class", () => {
    const operatorClass = action.testWrapOperatorClass(params);
    assertEquals(operatorClass, { operator_class: OperatorClass.C });
  });

  it("should fail on invalid operator class", () => {
    try {
      // @ts-expect-error invalid operator class
      action.testWrapOperatorClass({ operator_class: "nawak" });
    } catch (e) {
      assertEquals(e.message, "Invalid 'operator_class' parameter");
    }
  });

  it("should wrap operator trip id", () => {
    const operatorTripId = action.testWrapOperatorTripId(params);
    assertEquals(operatorTripId, { operator_trip_id: params.operator_trip_id });
  });
});
