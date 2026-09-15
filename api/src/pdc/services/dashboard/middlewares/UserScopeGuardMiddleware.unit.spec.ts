import { assertEquals, assertRejects } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { ContextType, ForbiddenException } from "@/ilos/common/index.ts";
import { roleRank, UserScopeGuardMiddleware } from "./UserScopeGuardMiddleware.ts";

describe("UserScopeGuardMiddleware", () => {
  const middleware = new UserScopeGuardMiddleware();
  const next = async () => "next() called";

  const ctx = (user: Record<string, unknown>): ContextType => ({
    call: { user },
    channel: { service: "proxy" },
  });

  const registryAdmin = { role: "registry.admin", permissions: ["registry.user.manageScopes", "registry.user.create"] };
  const territoryAdmin = { role: "territory.admin", permissions: ["territory.user.create"], territory_id: 1 };

  it("roleRank: registry.admin outranks territory.admin outranks territory.user", () => {
    assertEquals(roleRank("registry.admin") > roleRank("territory.admin"), true);
    assertEquals(roleRank("territory.admin") > roleRank("territory.user"), true);
    assertEquals(roleRank("registry.user") > roleRank("territory.admin"), true);
    assertEquals(roleRank("unknown"), 0);
  });

  it("403 when territory.admin assigns role registry.admin (hierarchy)", async () => {
    await assertRejects(
      () => middleware.process({ role: "registry.admin" }, ctx(territoryAdmin), next, undefined),
      ForbiddenException,
    );
  });

  it("403 when territory.admin assigns role registry.user (hierarchy)", async () => {
    await assertRejects(
      () => middleware.process({ role: "registry.user" }, ctx(territoryAdmin), next, undefined),
      ForbiddenException,
    );
  });

  it("OK when territory.admin assigns a peer territory.admin", async () => {
    assertEquals(
      await middleware.process({ role: "territory.admin" }, ctx(territoryAdmin), next, undefined),
      "next() called",
    );
  });

  it("OK when registry.admin creates a registry.admin peer", async () => {
    assertEquals(
      await middleware.process({ role: "registry.admin" }, ctx(registryAdmin), next, undefined),
      "next() called",
    );
  });

  it("403 when territory.admin provides login_siren (privileged field)", async () => {
    await assertRejects(
      () =>
        middleware.process({ role: "territory.admin", login_siren: "123456789" }, ctx(territoryAdmin), next, undefined),
      ForbiddenException,
    );
  });

  it("403 when territory.admin grants a foreign territory", async () => {
    await assertRejects(
      () =>
        middleware.process(
          { role: "territory.admin", scopes: [{ territory_id: 1 }, { territory_id: 2 }] },
          ctx(territoryAdmin),
          next,
          undefined,
        ),
      ForbiddenException,
    );
  });

  // territory_id vient du corps de la requête : tolérer « son propre territoire » laisserait
  // un appelant remplacer tout le pivot d'un compte multi-territoire par ce seul périmètre.
  it("403 when territory.admin re-submits even his own territory", async () => {
    await assertRejects(
      () =>
        middleware.process(
          { role: "territory.user", scopes: [{ territory_id: 1, is_default: true }] },
          ctx(territoryAdmin),
          next,
          undefined,
        ),
      ForbiddenException,
    );
  });

  it("OK when territory.admin sends no scopes at all (formulaire courant)", async () => {
    assertEquals(
      await middleware.process(
        { role: "territory.user", territory_id: 1 },
        ctx(territoryAdmin),
        next,
        undefined,
      ),
      "next() called",
    );
  });

  it("OK when registry.admin provides login_siren + scopes", async () => {
    assertEquals(
      await middleware.process(
        { role: "registry.admin", login_siren: "123456789", scopes: [{ territory_id: 1 }, { territory_id: 2 }] },
        ctx(registryAdmin),
        next,
        undefined,
      ),
      "next() called",
    );
  });
});
