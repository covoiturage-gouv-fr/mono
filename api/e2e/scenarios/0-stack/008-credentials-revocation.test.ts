import { expect } from "dep:expect";
import { describe, it } from "dep:testing-bdd";
import { ADMIN_EMAIL, ADMIN_PASSWORD, OPERATOR_EMAIL, OPERATOR_PASSWORD } from "../../config.ts";
import { API } from "../../lib/API.ts";
import { faker } from "../../lib/faker.ts";

/**
 * Dex tokens stay valid until `exp` once signed: deleting a key (or its
 * operator) must refuse the tokens it already issued, right away.
 */
const journeys = "/v3.3/journeys?status=ok";

async function issueToken(access_key: string, secret_key: string): Promise<API> {
  const app = new API();
  await app.authenticate(access_key, secret_key);
  return app;
}

describe("Credentials revocation", () => {
  it("refuses a token as soon as its key is deleted", async () => {
    const http = new API();
    const { operator_id } = await http.callback<{ operator_id: number }>(OPERATOR_EMAIL, OPERATOR_PASSWORD);
    const { access_key, secret_key } = await http.createCredentials(operator_id);
    const app = await issueToken(access_key, secret_key);
    expect((await app.get(journeys)).status).toBe(200);

    await http.deleteCredentials(operator_id, access_key);

    expect((await app.get(journeys)).status).toBe(401);
  });

  it("deletes the keys of a deleted operator and refuses their tokens", async () => {
    const admin = new API();
    await admin.callback(ADMIN_EMAIL, ADMIN_PASSWORD);
    const created = await admin.post<{ id: number }>("/v3/dashboard/operator", {
      name: `revocation ${faker.string.alphanumeric(8)}`,
      siret: faker.extra.siret(),
    });
    expect(created.status).toBe(200);
    const operator_id = created.body.id;

    const { access_key, secret_key } = await admin.createCredentials(operator_id);
    const app = await issueToken(access_key, secret_key);
    expect((await app.get(journeys)).status).toBe(200);

    expect((await admin.delete(`/v3/dashboard/operator/${operator_id}`)).status).toBe(200);

    expect((await app.get(journeys)).status).toBe(401);
    await expect(issueToken(access_key, secret_key)).rejects.toThrow();
    expect((await admin.post("/v3/auth/credentials", { operator_id })).status).toBe(404);
  });
});
