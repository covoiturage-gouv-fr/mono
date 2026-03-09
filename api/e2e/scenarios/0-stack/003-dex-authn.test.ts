import { expect } from "dep:expect";
import { beforeEach, describe, it } from "dep:testing-bdd";
import { OPERATOR_EMAIL, OPERATOR_PASSWORD, SUPPORTED_VERSIONS, UNSUPPORTED_VERSIONS } from "../../config.ts";
import { API } from "../../lib/API.ts";
import { createOperatorJourney } from "../../lib/journey.ts";
import { regex_datetime, regex_uuidV4 } from "../../lib/regex.ts";
import { CreateJourneyResponse } from "../../lib/types.ts";

describe("DEX Authentication", () => {
  const http = new API();

  beforeEach(async () => {
    const operator = await http.callback<{ operator_id: number }>(OPERATOR_EMAIL, OPERATOR_PASSWORD);
    const { access_key, secret_key } = await http.createCredentials(operator.operator_id);
    await http.authenticate(access_key, secret_key);
  });

  it("should be authenticated", async () => {
    // cookie-only authentication
    http.clearAccessToken();
    const response = await http.get("/auth/me");
    expect(response.status).toEqual(200);
    expect(response.body).toBeDefined();
    expect(response.body).toMatchObject({
      operator_id: 1,
      role: "operator.admin",
    });
  });

  for (const version of SUPPORTED_VERSIONS) {
    it(`should be authenticated with ${version} API`, async () => {
      const journey = await createOperatorJourney();
      const response = await http.post<CreateJourneyResponse>(`/${version}/journeys`, journey);
      expect(response.body).toBeDefined();

      if ("result" in response.body) {
        expect(response.body.result).toBeDefined();
        expect(response.body.result.data).toBeDefined();
        expect(response.body.result.data.operator_journey_id).toMatch(regex_uuidV4);
        expect(response.body.result.data.created_at).toMatch(regex_datetime);
      } else if ("error" in response.body) {
        console.error(response.body.error);
        throw new Error(response.body.error?.message || "Unknown error");
      }
    });
  }

  for (const version of UNSUPPORTED_VERSIONS) {
    it(`should not be authenticated with ${version} API`, async () => {
      const response = await http.post(`/${version}/journeys`, await createOperatorJourney());
      expect(response.ok).toBe(false);
      expect(response.status).toEqual(404);
    });
  }
});
