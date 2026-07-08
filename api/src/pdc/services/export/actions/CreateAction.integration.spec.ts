/**
 * Test the CreateActionV3 classes with proper payloads
 * Middlewares:
 * - permission
 * - castToArray ?
 * - timezone
 * - copyFromContext(created_by, operator_id, territory_id)
 * - payload Validation
 *
 * Features:
 * - recipients
 * - creator as recipient
 * - store: check response
 * - store: check existence in DB
 */
import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { ContextType } from "@/ilos/common/index.ts";
import { DenoPostgresConnection, LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { set } from "@/lib/object/index.ts";
import sql from "@/lib/pg/sql.ts";
import { User, users } from "@/pdc/providers/migration/seeds/users.ts";
import {
  AJVParamsInterface,
  assertHandler,
  DenoDbContext,
  KernelContext,
  makeDenoDbBeforeAfter,
  makeKernelBeforeAfter,
} from "@/pdc/providers/test/index.ts";
import { ExportServiceProvider as ExportSP } from "@/pdc/services/export/ExportServiceProvider.ts";
import { Export, ExportStatus, ExportTarget } from "@/pdc/services/export/models/Export.ts";
import { ExportParams } from "@/pdc/services/export/models/ExportParams.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/create.contract.ts";

const { before: kernelBefore, after: kernelAfter } = makeKernelBeforeAfter(ExportSP);
const { before: denoDbBefore, after: denoDbAfter } = makeDenoDbBeforeAfter();

/**
 * Simple Export fetcher to get all records and cast their values
 */
type FullExport = Export;
function fetcher(db: DenoDbContext) {
  return async (): Promise<FullExport[]> => {
    const rows = await db.connection.query<any>(sql`
      SELECT *
      FROM export.exports
      ORDER BY _id ASC
    `);

    return rows.length ? rows.map((r) => ({ ...r, params: ExportParams.fromJSON(r.params) })) : [];
  };
}

describe("CreateAction V3", () => {
  // ---------------------------------------------------------------------------
  // SETUP
  // ---------------------------------------------------------------------------

  let db: DenoDbContext;
  let kc: KernelContext;
  let fetchExports: () => Promise<FullExport[]>;

  const defaultContext: ContextType = {
    call: { user: { permissions: ["common.export.create"] } },
    channel: { service: "proxy" },
  };

  type UserWithId = User & { _id: number };
  const adminUser: UserWithId = { _id: 1, ...users[0] };
  const maxiCovoitAdmin: UserWithId = { _id: 4, ...users[3] };

  /**
   * - boot up postgresql connection
   * - create the kernel
   * - stop the existing kernel connection to replace it with the test one
   * - setup the db macro with the connection
   */
  beforeAll(async () => {
    // Native Deno connection
    db = await denoDbBefore();
    kc = await kernelBefore();
    await kc.kernel.getContainer().get(DenoPostgresConnection).down();
    kc.kernel
      .getContainer()
      .rebind(DenoPostgresConnection)
      .toConstantValue(db.connection);

    // @deprecated
    // replace the legacy connection with the test one for non-migrated repositories
    const { connectionString } = db.connection;
    await kc.kernel.getContainer().get(LegacyPostgresConnection).down();
    kc.kernel
      .getContainer()
      .rebind(LegacyPostgresConnection)
      .toConstantValue(new LegacyPostgresConnection({ connectionString }));

    fetchExports = fetcher(db);
  });

  afterAll(async () => {
    await kernelAfter(kc);
    await denoDbAfter(db);
  });

  // ---------------------------------------------------------------------------
  // TESTS
  // ---------------------------------------------------------------------------

  it("should create an export for admin", async () => {
    const start_at = "2024-01-01T00:00:00+0100";
    const end_at = "2024-01-02T00:00:00+0100";

    const params: AJVParamsInterface<ParamsInterface, "start_at" | "end_at"> = {
      tz: "Europe/Paris",
      start_at,
      end_at,
      created_by: adminUser._id,
    };

    // UUID is omitted as we have no way to predict it
    const expected: Omit<ResultInterface, "uuid"> = {
      // uuid: "uuid",
      target: ExportTarget.TERRITORY,
      status: ExportStatus.PENDING,
      start_at: new Date(start_at),
      end_at: new Date(end_at),
    };

    await assertHandler(
      kc,
      defaultContext,
      handlerConfig,
      params,
      async (response: ResultInterface) => {
        // assert the response
        const { uuid: _uuid, ...actual } = response;
        assertEquals(actual, expected);

        // assert the database record
        const last = (await fetchExports()).pop();
        assertEquals(last?.target, ExportTarget.TERRITORY);
        assertEquals(last?.status, ExportStatus.PENDING);
        assertEquals(last?.params.get().start_at, new Date(params.start_at));
        assertEquals(last?.error, null);
        assertEquals(last?.created_by, adminUser._id);
      },
    );
  });

  it("should create a super-admin export for a territory", async () => {
    const params: AJVParamsInterface<
      ParamsInterface,
      "start_at" | "end_at"
    > = {
      tz: "Europe/Paris",
      start_at: "2024-01-01T00:00:00+0100",
      end_at: "2024-01-02T00:00:00+0100",
      created_by: adminUser._id,
    };

    // TODO fix by inejcting all operator_id

    await assertHandler(
      kc,
      defaultContext,
      handlerConfig,
      params,
      (response: ResultInterface) => {
        assertEquals(response.target, ExportTarget.TERRITORY);
      },
    );
  });

  it("should create a territory export for admin", async () => {
    const params: AJVParamsInterface<ParamsInterface, "start_at" | "end_at"> = {
      tz: "Europe/Paris",
      start_at: "2024-01-01T00:00:00+0100",
      end_at: "2024-01-02T00:00:00+0100",
      created_by: adminUser._id,
    };

    await assertHandler(
      kc,
      set(defaultContext, "call.user.territory_id", 1),
      handlerConfig,
      params,
      async (response: ResultInterface) => {
        // assert response
        assertEquals(response.target, ExportTarget.TERRITORY);

        // assert database record
        const last = (await fetchExports()).pop();
        assertEquals(last?.target, ExportTarget.TERRITORY);

        // TODO
        // assert territory_id has been injected and wrapped in an array ?

        // TODO: resolve the geo_selector from the territory_id
        //       and inject it in the CreateActionV3
        // assertEquals(last?.params.get().geo_selector, { aom: ["TODO"] });
      },
    );
  });

  it("should create an operator export as operator", async () => {
    const params: AJVParamsInterface<ParamsInterface, "start_at" | "end_at"> = {
      tz: "Europe/Paris",
      start_at: "2024-01-01T00:00:00+0100",
      end_at: "2024-01-02T00:00:00+0100",
      created_by: maxiCovoitAdmin._id,
    };

    await assertHandler(
      kc,
      set(defaultContext, "call.user.operator_id", 1),
      handlerConfig,
      params,
      async (response: ResultInterface) => {
        // assert response
        assertEquals(response.target, ExportTarget.OPERATOR);

        // assert database record
        const last = (await fetchExports()).pop();
        assertEquals(last?.target, ExportTarget.OPERATOR);

        // assert that operator_id has been replaced by the operator's id
        // from the context (2).
        assertEquals(last?.params.get().operator_id, [1]);
      },
    );
  });
});
