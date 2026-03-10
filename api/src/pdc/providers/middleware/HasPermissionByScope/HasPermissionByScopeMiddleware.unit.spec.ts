import { assertEquals, assertRejects } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { ContextType } from "@/ilos/common/index.ts";

import {
  HasPermissionByScopeMiddleware,
  HasPermissionByScopeMiddlewareParams,
} from "./HasPermissionByScopeMiddleware.ts";

describe("has permission by scope middleware", () => {
  const mockSuperAdmin = {
    _id: "1ab",
    email: "john.schmidt@example.com",
    firstname: "john",
    lastname: "schmidt",
    phone: "0624857425",
    group: "registry",
    role: "admin",
    permissions: ["registry.user.create"],
  };

  const mockTerritoryAdmin = {
    _id: 2,
    email: "territory.admin@example.com",
    firstname: "john",
    lastname: "schmidt",
    phone: "0624857425",
    group: "territories",
    role: "admin",
    territory_id: 42,
    permissions: ["territory.trip.stats"],
    authorizedZoneCodes: { _id: [42, 43, 44, 45] },
  };

  const mockCreateUserParameters = {
    email: "edouard.nelson@example.com",
    firstname: "edouard",
    lastname: "nelson",
    phone: "+33622222233",
    role: "admin",
    territory_id: 42,
  };

  const middlewareConfigUserCreate: HasPermissionByScopeMiddlewareParams = [
    "registry.user.create",
    [
      ["territory.users.add", "call.user.territory_id", "territory_id"],
      ["operator.users.add", "call.user.operator_id", "operator_id"],
    ],
  ];

  const middlewareConfigTripStats: HasPermissionByScopeMiddlewareParams = [
    "registry.trip.stats",
    [
      [
        "territory.trip.stats",
        "call.user.authorizedZoneCodes._id",
        "territory_id",
      ],
      ["operator.trip.stats", "call.user.operator_id", "operator_id"],
    ],
  ];

  const mockAllStatsParams = {
    date: { start: new Date("2020-01-01T00:00:00+0100") },
    tz: "Europe/Paris",
  };

  const mockTownStatsParams = {
    date: { start: new Date("2020-01-01T00:00:00+0100") },
    territory_id: [43],
    tz: "Europe/Paris",
  };

  const middleware = new HasPermissionByScopeMiddleware();
  const contextFactory = (params: any): ContextType => {
    return {
      call: {
        user: {
          ...mockSuperAdmin,
          ...params,
        },
      },
      channel: {
        service: "",
      },
    };
  };

  it("Middleware Scopetoself: has permission to create user", async () => {
    const result = await middleware.process(
      mockCreateUserParameters,
      contextFactory({ permissions: ["registry.user.create"] }),
      async () => "next() called",
      middlewareConfigUserCreate,
    );

    assertEquals(result, "next() called");
  });

  it("Middleware Scopetoself: has permission to create territory user", async () => {
    const result = await middleware.process(
      mockCreateUserParameters,
      contextFactory({
        permissions: ["territory.users.add"],
        territory_id: mockCreateUserParameters.territory_id,
      }),
      async () => "next() called",
      middlewareConfigUserCreate,
    );

    assertEquals(result, "next() called");
  });

  it("Middleware Scopetoself: territory admin - has no permission to create territory user", async () => {
    await assertRejects(async () =>
      middleware.process(
        mockCreateUserParameters,
        contextFactory({
          permissions: [],
          territory: mockCreateUserParameters.territory_id,
        }),
        async () => {},
        middlewareConfigUserCreate,
      )
    );
  });

  it("Middleware Scopetoself: registry admin - wrong territory", async () => {
    await assertRejects(async () =>
      middleware.process(
        mockCreateUserParameters,
        contextFactory({
          permissions: ["territory.users.add"],
          territory: 0,
        }),
        async () => {},
        middlewareConfigUserCreate,
      )
    );
  });

  it("Middleware Scopetoself: super-admin can trip.stats", async () => {
    const result = await middleware.process(
      mockAllStatsParams,
      contextFactory({ permissions: ["registry.trip.stats"] }),
      async () => "next() called",
      middlewareConfigTripStats,
    );

    assertEquals(result, "next() called");
  });

  it("Middleware Scopetoself: super-admin can trip.stats with town filter", async () => {
    const result = await middleware.process(
      mockTownStatsParams,
      contextFactory({ permissions: ["registry.trip.stats"] }),
      async () => "next() called",
      middlewareConfigTripStats,
    );

    assertEquals(result, "next() called");
  });

  it("Middleware Scopetoself: territory-admin can trip.stats", async () => {
    // mock territory_id being added by copy.from_context middleware
    const params = {
      ...mockAllStatsParams,
      territory_id: mockTerritoryAdmin.territory_id,
    };

    const context = contextFactory(mockTerritoryAdmin);

    const result = await middleware.process(
      params,
      context,
      async () => "next() called",
      middlewareConfigTripStats,
    );

    assertEquals(result, "next() called");
  });

  it("Middleware Scopetoself: territory-admin can trip.stats w/ town filter", async () => {
    // mock territory_id being added by copy.from_context middleware
    const params = mockTownStatsParams;
    const context = contextFactory(mockTerritoryAdmin);

    const result = await middleware.process(
      params,
      context,
      async () => "next() called",
      middlewareConfigTripStats,
    );

    assertEquals(result, "next() called");
  });
});
