import { expect } from "dep:expect";
import { describe, it } from "dep:testing-bdd";
import { OPERATOR_EMAIL, OPERATOR_PASSWORD } from "../../config.ts";
import { API } from "../../lib/API.ts";
import { regex_jwt } from "../../lib/regex.ts";

/**
 * Based on registered user email/password in auth.users table
 * Make sure creating credentials works for the operator.
 *
 * Credentials = access_key + secret_key
 */
describe("Credentials Authentication", () => {
  it("should authenticate an operator", async () => {
    const http = new API();
    const operator = await http.callback(OPERATOR_EMAIL, OPERATOR_PASSWORD);

    expect(operator).toMatchObject({
      operator_id: 1,
      role: "operator.admin",
      email: OPERATOR_EMAIL,
    });
  });

  it("should create credentials and authenticate with them", async () => {
    const http = new API();
    const operator = await http.callback<{ operator_id: number }>(OPERATOR_EMAIL, OPERATOR_PASSWORD);
    const credentials = await http.createCredentials(operator.operator_id);
    expect(credentials).toMatchObject({
      access_key: expect.any(String),
      secret_key: expect.any(String),
    });

    // Verify that the credentials can be used to authenticate
    await http.authenticate(credentials.access_key, credentials.secret_key);
    expect(http.token).toBeDefined();
    expect(http.token).toMatch(regex_jwt);
    http.clearAccessToken();

    // Read credentials
    const readCredentials = await http.readCredentials(operator.operator_id);
    expect(readCredentials).toBeDefined();
    expect(readCredentials.length).toBeGreaterThan(0);
    expect(readCredentials[0]).toMatchObject({
      operator_id: operator.operator_id,
      role: "operator.application",
      token_id: expect.any(String),
    });
  });

  it("should create, read and delete credentials", async () => {
    const http = new API();
    const operator = await http.callback<{ operator_id: number }>(OPERATOR_EMAIL, OPERATOR_PASSWORD);
    const credentials = await http.createCredentials(operator.operator_id);
    expect(credentials).toMatchObject({
      access_key: expect.any(String),
      secret_key: expect.any(String),
    });

    // Read credentials
    const list = await http.readCredentials(operator.operator_id);
    expect(list).toBeDefined();
    expect(list.length).toBeGreaterThan(0);
    expect(list[0]).toMatchObject({
      token_id: expect.any(String),
      operator_id: operator.operator_id,
      role: "operator.application",
    });

    // Delete credentials
    const { operator_id, token_id } = list[0];
    await http.deleteCredentials(operator_id, token_id!);

    // Verify deletion
    const updatedCredentials = await http.readCredentials(operator.operator_id);
    expect(updatedCredentials.filter((i) => i.token_id === token_id).length).toBe(0);
  });
});
