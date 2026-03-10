import { assertEquals, assertRejects } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { ContextType, ForbiddenException } from "@/ilos/common/index.ts";
import { LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { env_or_default } from "@/lib/env/index.ts";
import { TerritorySelectorsInterface } from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";
import { ScopeToGroupMiddleware } from "./ScopeToGroupMiddleware.ts";

describe("ScopeToGroupMiddleware", () => {
  let connection: LegacyPostgresConnection;
  let middleware: ScopeToGroupMiddleware;

  const connectionString = env_or_default(
    "APP_POSTGRES_URL",
    "postgresql://postgres:postgres@localhost:5432/local",
  );

  const contextFactory = (params: Record<string, unknown>): ContextType => {
    return {
      call: {
        user: {
          ...mockConnectedUser,
          ...params,
        },
      },
      channel: {
        service: "",
      },
    };
  };

  const territory_id = 1;
  const geo_selector = {
    aom: ["217500016"],
    com: ["91471", "91477", "91377"],
  };

  const mockConnectedUser = {
    permissions: ["trip.list"],
  };

  const mockTripParameters = {
    geo_selector: {
      com: ["91477"],
    },
  };

  const middlewareConfig = {
    registry: "trip.list",
    territory: "territory.trip.list",
    operator: "operator.trip.list",
  };

  beforeAll(async () => {
    connection = new LegacyPostgresConnection({ connectionString });
    await connection.up();
    middleware = new ScopeToGroupMiddleware(connection);
  });

  afterAll(async () => {
    await connection.down();
  });

  it("Middleware Scopetogroup: has global permission", async () => {
    const result = await middleware.process(
      mockTripParameters,
      contextFactory({ permissions: ["trip.list"] }),
      () => "next() called",
      middlewareConfig,
    );

    assertEquals(result, "next() called");
  });

  it("Middleware Scopetogroup: has territory permission", async () => {
    const result = await middleware.process(
      mockTripParameters,
      contextFactory({
        permissions: ["territory.trip.list"],
        territory_id: territory_id,
        authorizedZoneCodes: geo_selector,
      }),
      () => "next() called",
      middlewareConfig,
    );

    assertEquals(result, "next() called");
  });

  it("Middleware Scopetogroup: has territory permission autoscope", async () => {
    const result = await middleware.process(
      {},
      contextFactory({
        permissions: ["territory.trip.list"],
        territory_id: 1,
        authorizedZoneCodes: geo_selector,
      }),
      (params: { geo_selector: TerritorySelectorsInterface }) => params.geo_selector,
      middlewareConfig,
    );
    assertEquals(result, { com: geo_selector.com });
  });

  it("Middleware Scopetogroup: has territory permission unnest selector", async () => {
    const result = await middleware.process(
      { aom: geo_selector.aom },
      contextFactory({
        permissions: ["territory.trip.list"],
        territory_id: 1,
        authorizedZoneCodes: geo_selector,
      }),
      (params: { geo_selector: TerritorySelectorsInterface }) => params.geo_selector,
      middlewareConfig,
    );
    assertEquals(result, { com: geo_selector.com });
  });

  it("Middleware Scopetogroup: has territory permission and search on authorized", async () => {
    const result = await middleware.process(
      mockTripParameters,
      contextFactory({
        permissions: ["territory.trip.list"],
        territory_id: 1,
        authorizedZoneCodes: geo_selector,
      }),
      () => "next() called",
      middlewareConfig,
    );

    assertEquals(result, "next() called");
  });

  it("Middleware Scopetogroup: has territory permission and search on unauthorized", async () => {
    await assertRejects(
      () =>
        middleware.process(
          mockTripParameters,
          contextFactory({
            permissions: ["territory.trip.list"],
            territory_id: 1,
            authorizedZoneCodes: { com: ["91377"] },
          }),
          () => "next() called",
          middlewareConfig,
        ),
      ForbiddenException,
    );
  });

  it("Middleware Scopetogroup: has operator permission", async () => {
    const result = await middleware.process(
      { operator_id: 2 },
      contextFactory({
        permissions: ["operator.trip.list"],
        operator_id: 2,
      }),
      () => "next() called",
      middlewareConfig,
    );

    assertEquals(result, "next() called");
  });

  it("Middleware Scopetogroup: has operator permission autoscope", async () => {
    const result = await middleware.process(
      {},
      contextFactory({
        permissions: ["operator.trip.list"],
        operator_id: 2,
      }),
      (params: { operator_id: number }) => params.operator_id,
      middlewareConfig,
    );

    assertEquals(result.length, 1);
    assertEquals(result[0], 2);
  });

  it("Middleware Scopetogroup: has operator permission and search on unauthorized", async () => {
    await assertRejects(
      () =>
        middleware.process(
          { operator_id: [1] },
          contextFactory({
            permissions: ["operator.trip.list"],
            operator_id: 2,
          }),
          () => "next() called",
          middlewareConfig,
        ),
      ForbiddenException,
    );
  });

  it("Middleware Scopetogroup: has no permission", async () => {
    await assertRejects(
      () =>
        middleware.process(
          {},
          contextFactory({
            permissions: [],
          }),
          () => "next() called",
          middlewareConfig,
        ),
      ForbiddenException,
    );
  });
});
