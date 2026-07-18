import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { getPermissions } from "./permissions.ts";

describe("permissions", () => {
  it("grants export.process to datalake.worker", () => {
    assertEquals(getPermissions("datalake.worker").includes("datalake.export.process"), true);
  });

  it("datalake.worker is least-privilege: only export.process, no common.* inheritance", () => {
    const perms = getPermissions("datalake.worker");
    assertEquals(perms, ["datalake.export.process"]);
    assertEquals(perms.some((p) => p.startsWith("common.")), false);
  });

  it("human roles still inherit the common permission set", () => {
    const perms = getPermissions("operator.admin");
    assertEquals(perms.some((p) => p.startsWith("common.")), true);
  });

  it("grants registry.user.manageScopes to registry.admin only", () => {
    assertEquals(getPermissions("registry.admin").includes("registry.user.manageScopes"), true);
  });

  it("denies manageScopes to territory.admin and operator.admin", () => {
    assertEquals(getPermissions("territory.admin").some((p) => p.endsWith("user.manageScopes")), false);
    assertEquals(getPermissions("operator.admin").some((p) => p.endsWith("user.manageScopes")), false);
  });
});
