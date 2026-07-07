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
});
