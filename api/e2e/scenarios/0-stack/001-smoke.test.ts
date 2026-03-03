import { expect } from "dep:expect";
import { beforeEach, describe, it } from "dep:testing-bdd";
import { OPERATOR_EMAIL, OPERATOR_PASSWORD } from "../../config.ts";
import { API } from "../../lib/API.ts";

describe("Unauthenticated smoke test", () => {
  const http = new API();

  it("should be up and running", async () => {
    const response = await http.get("/health");
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ status: "ok" });
  });

  it("should return 401 for unauthenticated user", async () => {
    const response = await http.get<{ error: { message: string } }>("/auth/me");
    expect(response.status).toBe(401);
    expect(response.body).toBeDefined();
    expect(response.body).toEqual({
      id: 1,
      jsonrpc: "2.0",
      error: {
        code: 401,
        data: "Error",
        message: "Unauthorized Error",
      },
    });
  });
});

describe("Authenticated smoke test", () => {
  const http = new API();

  beforeEach(async () => {
    const operator = await http.callback<{ operator_id: number }>(OPERATOR_EMAIL, OPERATOR_PASSWORD);
    const { access_key, secret_key } = await http.createCredentials(operator.operator_id);
    await http.authenticate(access_key, secret_key);
  });

  it("should get a JWT token", async () => {
    expect(http.token).toBeDefined();
    expect(http.token).toMatch(/^ey[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+$/);
    expect(http.token!.split(".").length).toBe(3);
  });

  it("should get the user's profile", async () => {
    // remove the token otherwise the session
    // will not be loaded from the cookie.
    http.clearAccessToken();
    const response = await http.get("/auth/me");
    expect(response.status).toBe(200);

    // informations must match the ones in the dex config-e2e.yaml file
    expect(response.body).toMatchObject({
      operator_id: 1,
      role: "operator.admin",
      email: OPERATOR_EMAIL,
    });
  });
});
