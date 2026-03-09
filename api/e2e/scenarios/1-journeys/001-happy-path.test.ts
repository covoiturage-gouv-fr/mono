import { expect } from "dep:expect";
import { beforeEach, describe, it } from "dep:testing-bdd";
import { OPERATOR_EMAIL, OPERATOR_PASSWORD } from "../../config.ts";
import { API } from "../../lib/API.ts";
import { createOperatorJourney } from "../../lib/journey.ts";
import { regex_datetime } from "../../lib/regex.ts";
import { CreateJourneyResponse } from "../../lib/types.ts";

describe("Journey creation", () => {
  const http = new API();

  beforeEach(async () => {
    const operator = await http.callback<{ operator_id: number }>(OPERATOR_EMAIL, OPERATOR_PASSWORD);
    const { access_key, secret_key } = await http.createCredentials(operator.operator_id);
    if (!http.token) await http.authenticate(access_key, secret_key);
  });

  it("should get a valid operator journey from faker", async () => {
    const journey = await createOperatorJourney();
    expect(journey.operator_journey_id).toBeDefined();
    expect(journey.operator_trip_id).toBeDefined();
    expect(journey.operator_class).toMatch(/^[ABC]$/);
    expect(journey.start).toBeDefined();
    expect(journey.start.datetime).toBeDefined();
    expect(new Date(journey.end.datetime).getTime()).toBeGreaterThan(new Date(journey.start.datetime).getTime());
    expect(journey.distance).toBeGreaterThan(0);
    expect(journey.driver).toBeDefined();
    expect(journey.driver.identity).toBeDefined();
    expect(journey.driver.identity.identity_key.length).toEqual(64);
    expect(journey.driver.identity.phone_trunc.length).toEqual(10);
    expect(journey.passenger).toBeDefined();
    expect(journey.passenger.identity).toBeDefined();
    expect(journey.passenger.identity.identity_key.length).toEqual(64);
    expect(journey.passenger.identity.phone_trunc.length).toEqual(10);
  });

  it("should POST a valid operator journey to API v3", async () => {
    const journey = await createOperatorJourney();
    const response = await http.post<CreateJourneyResponse>("/v3/journeys", JSON.stringify(journey));
    expect(response.status).toEqual(201);
    expect(response.body).toBeDefined();

    if ("result" in response.body) {
      expect(response.body.result).toBeDefined();
      expect(response.body.result.data).toBeDefined();
      expect(response.body.result.data.operator_journey_id).toEqual(journey.operator_journey_id);
      expect(response.body.result.data.created_at).toMatch(regex_datetime);
      expect(response.headers.get("set-cookie")).toBeNull();
    } else if ("error" in response.body) {
      console.error(response.body.error);
      throw new Error(response.body.error?.message || "Unknown error");
    }
  });
});
