import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";

import { UserScope } from "./providers/UserScopeRepository.ts";
import { contextRoute } from "./AuthRouter.ts";

// Stub du repository : n'accorde que les couples (userId, territoryId) déclarés.
class StubUserScopeRepository {
  public calls: Array<[number, number]> = [];
  constructor(private granted: Array<[number, number]> = []) {}
  userHasTerritory(userId: number, territoryId: number): Promise<boolean> {
    this.calls.push([userId, territoryId]);
    return Promise.resolve(this.granted.some(([u, t]) => u === userId && t === territoryId));
  }
}

// Réponse express minimale : capture statut + corps.
function mockRes() {
  return {
    statusCode: 200,
    body: undefined as unknown,
    status(code: number) {
      this.statusCode = code;
      return this;
    },
    json(payload: unknown) {
      this.body = payload;
      return this;
    },
  };
}

// Session express minimale : save() résout immédiatement.
function mockSession(user: Record<string, unknown> | undefined) {
  return {
    user,
    save(cb: (err?: Error) => void) {
      cb();
    },
  };
}

// deno-lint-ignore no-explicit-any
async function invoke(repo: StubUserScopeRepository, req: any) {
  const res = mockRes();
  let nextErr: unknown = undefined;
  await contextRoute(repo as never)(req, res, (err: unknown) => (nextErr = err));
  if (nextErr) throw nextErr;
  return res;
}

const territoryScopes: UserScope[] = [
  { territory_id: 200, label: "AOM Deux Cents" },
  { territory_id: 201, label: "AOM Deux Cent Un" },
];

describe("POST /auth/context (contextRoute)", () => {
  it("401 when no session user", async () => {
    const repo = new StubUserScopeRepository();
    const res = await invoke(repo, { session: mockSession(undefined), body: { territory_id: 200 } });
    assertEquals(res.statusCode, 401);
  });

  it("400 when territory_id is missing or not a positive integer", async () => {
    const repo = new StubUserScopeRepository([[7, 200]]);
    const user = { _id: 7, territory_id: 200, scopes: territoryScopes };
    for (const body of [{}, { territory_id: 0 }, { territory_id: -3 }, { territory_id: "200" }]) {
      const res = await invoke(repo, { session: mockSession({ ...user }), body });
      assertEquals(res.statusCode, 400);
    }
  });

  it("403 when the territory is not granted in DB", async () => {
    const repo = new StubUserScopeRepository([[7, 200]]);
    const user = { _id: 7, territory_id: 200, scopes: territoryScopes };
    const res = await invoke(repo, { session: mockSession(user), body: { territory_id: 999 } });
    assertEquals(res.statusCode, 403);
    // La revalidation interroge bien la DB sur le territory_id demandé.
    assertEquals(repo.calls, [[7, 999]]);
  });

  it("403 on op/territory id collision (never treats an operator id as a territory)", async () => {
    const repo = new StubUserScopeRepository();
    const user = { _id: 9, operator_id: 42, scopes: [{ operator_id: 42, label: "Op" }] as UserScope[] };
    const res = await invoke(repo, { session: mockSession(user), body: { territory_id: 42 } });
    assertEquals(res.statusCode, 403);
    // Refus avant tout appel DB : l'id opérateur n'est jamais revalidé comme territoire.
    assertEquals(repo.calls, []);
  });

  it("refuses the switch for an operator-type user", async () => {
    const repo = new StubUserScopeRepository([[9, 200]]);
    const user = { _id: 9, operator_id: 5, scopes: [{ operator_id: 5, label: "Op" }] as UserScope[] };
    const res = await invoke(repo, { session: mockSession(user), body: { territory_id: 200 } });
    assertEquals(res.statusCode, 403);
    assertEquals(repo.calls, []);
  });

  it("200 and rewrites the full active tuple (operator_id=null) when granted", async () => {
    const repo = new StubUserScopeRepository([[7, 201]]);
    const user: Record<string, unknown> = { _id: 7, territory_id: 200, operator_id: undefined, scopes: territoryScopes };
    const res = await invoke(repo, { session: mockSession(user), body: { territory_id: 201 } });
    assertEquals(res.statusCode, 200);
    assertEquals(res.body, { territory_id: 201, label: "AOM Deux Cent Un" });
    // Tuple actif réécrit : territoire ciblé, opérateur purgé.
    assertEquals(user.territory_id, 201);
    assertEquals(user.operator_id, null);
  });
});
