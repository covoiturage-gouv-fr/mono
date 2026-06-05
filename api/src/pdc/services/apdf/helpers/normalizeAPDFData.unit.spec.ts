import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { ExcelCampaignConfig } from "../interfaces/ExcelTypes.ts";
import { APDFTripInterface } from "../interfaces/APDFTripInterface.ts";
import { normalize } from "./normalizeAPDFData.helper.ts";

describe("normalizeAPDFData", () => {
  const config: ExcelCampaignConfig = {
    tz: "Europe/Paris",
    booster_dates: new Set<string>(),
    extras: {},
  };

  function fakeTrip(overrides: Partial<APDFTripInterface> = {}): APDFTripInterface {
    return {
      distance: 1000,
      driver_operator_user_id: "driver",
      duration: 600,
      end_datetime: "2024-01-15T08:30:00+01:00",
      end_epci: "end_epci",
      end_insee: "75056",
      end_location: "Paris",
      incentive_type: "normale",
      operator_class: "C",
      operator_journey_id: "ojid",
      operator_trip_id: "otid",
      operator: "Operator",
      passenger_contribution: 180,
      passenger_operator_user_id: "passenger",
      rpc_incentive: 200,
      rpc_journey_id: "rjid",
      start_datetime: "2024-01-15T08:00:00+01:00",
      start_epci: "start_epci",
      start_insee: "75056",
      start_location: "Paris",
      ...overrides,
    };
  }

  it("converts passenger_contribution from cents to euros", () => {
    const result = normalize(fakeTrip({ passenger_contribution: 180 }), config);
    assertEquals(result.passenger_contribution, 1.8);
  });

  it("keeps null passenger_contribution as null", () => {
    const result = normalize(fakeTrip({ passenger_contribution: null }), config);
    assertEquals(result.passenger_contribution, null);
  });
});
