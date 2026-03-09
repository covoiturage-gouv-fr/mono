import { ForbiddenException, NotFoundException, UnauthorizedException } from "@/ilos/common/index.ts";
import { expect } from "dep:expect";
import { describe, it } from "dep:testing-bdd";
import {
  ADMIN_EMAIL,
  ADMIN_PASSWORD,
  OPERATOR_EMAIL,
  OPERATOR_PASSWORD,
  TERRITORY_EMAIL,
  TERRITORY_PASSWORD,
} from "../../config.ts";
import { API } from "../../lib/API.ts";
import { regex_jwt } from "../../lib/regex.ts";

describe("Credentials Creation", () => {
  it("should fail to create credentials for another operator", async () => {
    const http = new API();
    await http.callback<{ operator_id: number }>(OPERATOR_EMAIL, OPERATOR_PASSWORD);
    await expect(http.createCredentials(999)).rejects.toThrow(ForbiddenException);
  });

  it("should fail a territory trying to create credentials", async () => {
    const http = new API();
    await http.callback<{ operator_id: number }>(TERRITORY_EMAIL, TERRITORY_PASSWORD);
    await expect(http.createCredentials(1)).rejects.toThrow(ForbiddenException);
  });

  it("should fail a super admin creating for a missing operator", async () => {
    const http = new API();
    await http.callback<{ operator_id: number }>(ADMIN_EMAIL, ADMIN_PASSWORD);
    await expect(http.createCredentials(2)).rejects.toThrow(NotFoundException);
  });

  it("should succeed to create credentials as a super admin", async () => {
    const operator_id = 1;
    const http = new API();
    await http.callback<{ operator_id: number }>(ADMIN_EMAIL, ADMIN_PASSWORD);
    const credentials = await http.createCredentials(operator_id);
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
    const readCredentials = await http.readCredentials(operator_id);
    expect(readCredentials).toBeDefined();
    expect(readCredentials.length).toBeGreaterThan(0);
    expect(readCredentials[0]).toMatchObject({
      operator_id: operator_id,
      role: "operator.application",
      token_id: expect.any(String),
    });
  });

  it("should fail on empty body", async () => {
    const http = new API();
    await http.callback<{ operator_id: number }>(OPERATOR_EMAIL, OPERATOR_PASSWORD);

    // @ts-expect-error type error to simulate empty body
    await expect(http.createCredentials(undefined)).rejects.toThrow(ForbiddenException);
  });

  it("should fail on wrong payload", async () => {
    const http = new API();
    await http.callback<{ operator_id: number }>(OPERATOR_EMAIL, OPERATOR_PASSWORD);

    // @ts-expect-error type error to simulate wrong payload
    await expect(http.createCredentials("wrong")).rejects.toThrow(ForbiddenException);
  });
});

describe("Credentials Reading", () => {
  it("should fail to read credentials for another operator", async () => {
    const http = new API();
    await http.callback<{ operator_id: number }>(OPERATOR_EMAIL, OPERATOR_PASSWORD);
    await expect(http.readCredentials(999)).rejects.toThrow(ForbiddenException);
  });

  it("should fail a territory trying to read credentials", async () => {
    const http = new API();
    await http.callback<{ operator_id: number }>(TERRITORY_EMAIL, TERRITORY_PASSWORD);
    await expect(http.readCredentials(1)).rejects.toThrow(ForbiddenException);
  });

  it("should fail a super admin reading for a missing operator", async () => {
    const http = new API();
    await http.callback<{ operator_id: number }>(ADMIN_EMAIL, ADMIN_PASSWORD);
    await expect(http.readCredentials(2)).rejects.toThrow(NotFoundException);
  });

  it("should succeed to read credentials as a super admin", async () => {
    const operator_id = 1;
    const http = new API();
    await http.callback<{ operator_id: number }>(ADMIN_EMAIL, ADMIN_PASSWORD);
    const credentials = await http.readCredentials(operator_id);
    expect(credentials).toBeDefined();
    expect(credentials.length).toBeGreaterThan(0);
    expect(credentials[0]).toMatchObject({
      operator_id: operator_id,
      role: "operator.application",
      token_id: expect.any(String),
    });
  });
});

describe("Credentials Deletion", () => {
  it("should fail to delete credentials for another operator", async () => {
    const http = new API();
    await http.callback<{ operator_id: number }>(OPERATOR_EMAIL, OPERATOR_PASSWORD);
    await expect(http.deleteCredentials(999, "token_id")).rejects.toThrow(ForbiddenException);
  });

  it("should fail a territory trying to delete credentials", async () => {
    const http = new API();
    await http.callback<{ operator_id: number }>(TERRITORY_EMAIL, TERRITORY_PASSWORD);
    await expect(http.deleteCredentials(1, "token_id")).rejects.toThrow(ForbiddenException);
  });

  it("should fail a super admin deleting for a missing operator", async () => {
    const http = new API();
    await http.callback<{ operator_id: number }>(ADMIN_EMAIL, ADMIN_PASSWORD);
    await expect(http.deleteCredentials(2, "token_id")).rejects.toThrow(NotFoundException);
  });

  it("should succeed to delete credentials as a super admin", async () => {
    const operator_id = 1;
    const http = new API();
    await http.callback<{ operator_id: number }>(ADMIN_EMAIL, ADMIN_PASSWORD);
    const created = await http.createCredentials(operator_id);
    expect(created).toBeDefined();

    // delete all credentials for the operator
    for (const credentials of await http.readCredentials(operator_id)) {
      await http.deleteCredentials(operator_id, String(credentials.token_id));
    }

    expect(await http.readCredentials(operator_id)).toEqual([]);

    // Verify that the credentials are no longer valid
    http.clearAccessToken();
    http.clearSessionCookie();
    await expect(http.authenticate(created.access_key, created.secret_key)).rejects.toThrow(UnauthorizedException);
  });
});
