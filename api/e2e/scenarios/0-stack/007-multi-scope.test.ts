import { expect } from "dep:expect";
import { describe, it } from "dep:testing-bdd";
import {
  ADMIN_EMAIL,
  ADMIN_PASSWORD,
  MULTI_EMAIL,
  MULTI_PASSWORD,
  OPERATOR_EMAIL,
  OPERATOR_PASSWORD,
  TERRITORY_EMAIL,
  TERRITORY_IDFM_ID,
  TERRITORY_LYON_ID,
  TERRITORY_PASSWORD,
} from "../../config.ts";
import { API } from "../../lib/API.ts";

/**
 * Multi-périmètre (GEN-686) : un compte territoire porte plusieurs SIRET,
 * bascule de contexte et administration des périmètres depuis le dashboard.
 */

type SessionUser = {
  _id: number;
  role: string;
  territory_id: number | null;
  operator_id: number | null;
  scopes: Array<{ territory_id?: number; operator_id?: number; label: string; siret?: string }>;
};

type UserRow = {
  id: number;
  email: string;
  role: string;
  territory_id: number | null;
  operator_id: number | null;
  login_siren?: string | null;
  scopes_count: number;
  scopes: Array<{ territory_id: number; is_default: boolean }>;
};

const me = async (http: API): Promise<SessionUser> => (await http.get<SessionUser>("/auth/me")).body;

const findUser = async (http: API, email: string): Promise<UserRow> => {
  const params = new URLSearchParams({ search: email });
  const res = await http.get<{ data: UserRow[] }>("/v3/dashboard/users", params);
  const row = res.body.data.find((u) => u.email === email);
  if (!row) throw new Error(`user ${email} introuvable dans la liste`);
  return row;
};

describe("Multi-périmètre : session et bascule de contexte", () => {
  it("expose les périmètres du compte et son défaut", async () => {
    const http = new API();
    await http.callback(MULTI_EMAIL, MULTI_PASSWORD);

    const user = await me(http);
    expect(user.role).toBe("territory.admin");
    expect(user.scopes.map((s) => s.territory_id).sort()).toEqual([TERRITORY_IDFM_ID, TERRITORY_LYON_ID]);
    // Le contexte actif est un des périmètres accordés (lequel dépend du défaut en base).
    expect(user.scopes.some((s) => s.territory_id === user.territory_id)).toBe(true);
    // Le libellé et le SIRET viennent des tables territoire/entreprise, pas d'un identifiant brut.
    expect(user.scopes.every((s) => typeof s.label === "string" && s.label.length > 0)).toBe(true);
    expect(new Set(user.scopes.map((s) => s.siret)).size).toBe(2);
  });

  it("bascule vers un périmètre accordé et la session suit", async () => {
    const http = new API();
    await http.callback(MULTI_EMAIL, MULTI_PASSWORD);

    // Cible = l'autre périmètre accordé, quel que soit le défaut en base.
    const before = await me(http);
    const target = before.territory_id === TERRITORY_LYON_ID ? TERRITORY_IDFM_ID : TERRITORY_LYON_ID;

    const res = await http.post<{ territory_id: number; label: string }>("/auth/context", {
      territory_id: target,
    });
    expect(res.status).toBe(200);
    expect(res.body.territory_id).toBe(target);
    expect(res.body.label.length > 0).toBe(true);

    const user = await me(http);
    expect(user.territory_id).toBe(target);
    expect(user.operator_id).toBe(null);
  });

  it("refuse un périmètre non accordé (403)", async () => {
    const http = new API();
    await http.callback(TERRITORY_EMAIL, TERRITORY_PASSWORD);

    const res = await http.post("/auth/context", { territory_id: TERRITORY_LYON_ID });
    expect(res.status).toBe(403);

    // La session reste sur le périmètre d'origine après un refus.
    expect((await me(http)).territory_id).toBe(TERRITORY_IDFM_ID);
  });

  it("refuse la bascule à un compte opérateur (403)", async () => {
    const http = new API();
    await http.callback(OPERATOR_EMAIL, OPERATOR_PASSWORD);

    const res = await http.post("/auth/context", { territory_id: TERRITORY_IDFM_ID });
    expect(res.status).toBe(403);
  });

  it("refuse un territory_id invalide (400)", async () => {
    const http = new API();
    await http.callback(MULTI_EMAIL, MULTI_PASSWORD);

    expect((await http.post("/auth/context", { territory_id: 0 })).status).toBe(400);
    expect((await http.post("/auth/context", { territory_id: "1" })).status).toBe(400);
    expect((await http.post("/auth/context", {})).status).toBe(400);
  });

  it("refuse la bascule sans session (401)", async () => {
    const http = new API();
    const res = await http.post("/auth/context", { territory_id: TERRITORY_IDFM_ID });
    expect(res.status).toBe(401);
  });
});

describe("Multi-périmètre : administration des utilisateurs", () => {
  it("la liste expose les périmètres et leur nombre", async () => {
    const http = new API();
    await http.callback(ADMIN_EMAIL, ADMIN_PASSWORD);

    const row = await findUser(http, MULTI_EMAIL);
    expect(row.scopes_count).toBe(2);
    expect(row.scopes.filter((s) => s.is_default).length).toBe(1);
  });

  it("registry.admin crée un compte multi-périmètre, puis change son défaut", async () => {
    const http = new API();
    await http.callback(ADMIN_EMAIL, ADMIN_PASSWORD);
    const email = `e2e-multi-${Date.now()}@example.com`;

    const created = await http.post("/v3/dashboard/user", {
      firstname: "E2E",
      lastname: "Multi",
      email,
      role: "territory.admin",
      login_siren: "130025265",
      scopes: [
        { territory_id: TERRITORY_IDFM_ID, is_default: true },
        { territory_id: TERRITORY_LYON_ID },
      ],
    });
    expect(created.status).toBe(200);

    let row = await findUser(http, email);
    expect(row.scopes_count).toBe(2);
    expect(row.territory_id).toBe(TERRITORY_IDFM_ID);
    expect(row.login_siren).toBe("130025265");

    // La ligne renvoyée par la liste doit pouvoir être réémise telle quelle par le formulaire.
    const updated = await http.put("/v3/dashboard/user", {
      id: row.id,
      firstname: "E2E",
      lastname: "Multi",
      email,
      role: "territory.admin",
      operator_id: null,
      territory_id: row.territory_id,
      login_siren: row.login_siren,
      scopes: [
        { territory_id: TERRITORY_IDFM_ID, is_default: false },
        { territory_id: TERRITORY_LYON_ID, is_default: true },
      ],
    });
    expect(updated.status).toBe(200);

    row = await findUser(http, email);
    expect(row.territory_id).toBe(TERRITORY_LYON_ID);
    expect(row.scopes_count).toBe(2);

    // Nettoyage : suppression complète du compte.
    expect((await http.delete(`/v3/dashboard/user/${row.id}`)).status).toBe(200);
  });

  it("un admin de territoire modifie un compte sans amputer ses autres périmètres", async () => {
    const admin = new API();
    await admin.callback(ADMIN_EMAIL, ADMIN_PASSWORD);
    const email = `e2e-keep-${Date.now()}@example.com`;
    await admin.post("/v3/dashboard/user", {
      firstname: "E2E",
      lastname: "Keep",
      email,
      role: "territory.user",
      scopes: [
        { territory_id: TERRITORY_IDFM_ID, is_default: true },
        { territory_id: TERRITORY_LYON_ID },
      ],
    });
    const row = await findUser(admin, email);

    const territoryAdmin = new API();
    await territoryAdmin.callback(TERRITORY_EMAIL, TERRITORY_PASSWORD);
    const updated = await territoryAdmin.put("/v3/dashboard/user", {
      id: row.id,
      firstname: "E2E",
      lastname: "Renommé",
      email,
      role: "territory.user",
      operator_id: null,
      territory_id: TERRITORY_IDFM_ID,
    });
    expect(updated.status).toBe(200);

    expect((await findUser(admin, email)).scopes_count).toBe(2);

    await admin.delete(`/v3/dashboard/user/${row.id}`);
  });

  it("un admin de territoire ne peut pas octroyer un périmètre étranger ni un login_siren", async () => {
    const admin = new API();
    await admin.callback(ADMIN_EMAIL, ADMIN_PASSWORD);
    const email = `e2e-guard-${Date.now()}@example.com`;
    await admin.post("/v3/dashboard/user", {
      firstname: "E2E",
      lastname: "Guard",
      email,
      role: "territory.user",
      scopes: [{ territory_id: TERRITORY_IDFM_ID, is_default: true }],
    });
    const row = await findUser(admin, email);

    const territoryAdmin = new API();
    await territoryAdmin.callback(TERRITORY_EMAIL, TERRITORY_PASSWORD);
    const body = {
      id: row.id,
      firstname: "E2E",
      lastname: "Guard",
      email,
      role: "territory.user",
      operator_id: null,
      territory_id: TERRITORY_IDFM_ID,
    };

    expect(
      (await territoryAdmin.put("/v3/dashboard/user", {
        ...body,
        scopes: [{ territory_id: TERRITORY_LYON_ID }],
      })).status,
    ).toBe(403);

    expect((await territoryAdmin.put("/v3/dashboard/user", { ...body, login_siren: "130025265" })).status).toBe(403);

    await admin.delete(`/v3/dashboard/user/${row.id}`);
  });

  it("retirer un compte d'un territoire libère le périmètre sans supprimer le compte", async () => {
    const admin = new API();
    await admin.callback(ADMIN_EMAIL, ADMIN_PASSWORD);
    const email = `e2e-release-${Date.now()}@example.com`;
    await admin.post("/v3/dashboard/user", {
      firstname: "E2E",
      lastname: "Release",
      email,
      role: "territory.user",
      scopes: [
        { territory_id: TERRITORY_IDFM_ID, is_default: true },
        { territory_id: TERRITORY_LYON_ID },
      ],
    });
    const row = await findUser(admin, email);

    const territoryAdmin = new API();
    await territoryAdmin.callback(TERRITORY_EMAIL, TERRITORY_PASSWORD);
    const released = await territoryAdmin.delete<{ outcome: string }>(`/v3/dashboard/user/${row.id}`);
    expect(released.status).toBe(200);
    expect(released.body.outcome).toBe("scope_released");

    const after = await findUser(admin, email);
    expect(after.scopes_count).toBe(1);
    expect(after.scopes[0].territory_id).toBe(TERRITORY_LYON_ID);

    await admin.delete(`/v3/dashboard/user/${after.id}`);
  });
});
