import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { ContextType } from "@/ilos/common/index.ts";
import { DenoPostgresConnection, LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql from "@/lib/pg/sql.ts";
import {
  assertHandler,
  DenoDbContext,
  KernelContext,
  makeDenoDbBeforeAfter,
  makeKernelBeforeAfter,
} from "@/pdc/providers/test/index.ts";
import { ExportServiceProvider as ExportSP } from "@/pdc/services/export/ExportServiceProvider.ts";
import { NotificationService } from "@/pdc/services/export/services/NotificationService.ts";
import { handlerConfig, ResultInterface } from "../contracts/fail.contract.ts";

const { before: kernelBefore, after: kernelAfter } = makeKernelBeforeAfter(ExportSP);
const { before: denoDbBefore, after: denoDbAfter } = makeDenoDbBeforeAfter();

describe("FailAction", () => {
  let db: DenoDbContext;
  let kc: KernelContext;

  const errorCalls: unknown[] = [];
  const supportCalls: unknown[] = [];

  const workerContext: ContextType = {
    call: { user: { permissions: ["datalake.export.process"] } },
    channel: { service: "proxy" },
  };

  const params = {
    start_at: "2024-01-01T00:00:00+0100",
    end_at: "2024-02-01T00:00:00+0100",
    operator_id: [1],
    geo_selector: null,
  };

  async function seedRunning(): Promise<string> {
    const rows = await db.connection.query<{ uuid: string }>(sql`
      INSERT INTO export.exports (created_by, target, status, params)
      VALUES (1, 'operator', 'running', ${JSON.stringify(params)}::json)
      RETURNING uuid
    `);
    return rows[0].uuid;
  }

  async function fetchByUuid(uuid: string) {
    const rows = await db.connection.query<{ status: string; error: { message: string } | null }>(sql`
      SELECT status, error FROM export.exports WHERE uuid = ${uuid}
    `);
    return rows[0];
  }

  function forceBind(token: unknown, value: unknown) {
    // deno-lint-ignore no-explicit-any
    const c = kc.kernel.getContainer() as any;
    if (c.isBound(token)) c.rebind(token).toConstantValue(value);
    else c.bind(token).toConstantValue(value);
  }

  beforeAll(async () => {
    db = await denoDbBefore();
    kc = await kernelBefore();
    await kc.kernel.getContainer().get(DenoPostgresConnection).down();
    kc.kernel.getContainer().rebind(DenoPostgresConnection).toConstantValue(db.connection);

    const { connectionString } = db.connection;
    await kc.kernel.getContainer().get(LegacyPostgresConnection).down();
    kc.kernel
      .getContainer()
      .rebind(LegacyPostgresConnection)
      .toConstantValue(new LegacyPostgresConnection({ connectionString }));

    forceBind(NotificationService, {
      success: async () => {},
      error: async (exp: unknown) => {
        errorCalls.push(exp);
      },
      support: async (exp: unknown) => {
        supportCalls.push(exp);
      },
    });
  });

  afterAll(async () => {
    await kernelAfter(kc);
    await denoDbAfter(db);
  });

  it("marks a running export failure, stores the error and notifies creator + support once", async () => {
    const uuid = await seedRunning();

    await assertHandler(
      kc,
      workerContext,
      handlerConfig,
      { uuid, message: "boom" },
      async (response: ResultInterface) => {
        assertEquals(response.status, "failure");

        const row = await fetchByUuid(uuid);
        assertEquals(row.status, "failure");
        assertEquals(row.error?.message, "boom");

        assertEquals(errorCalls.length, 1);
        assertEquals(supportCalls.length, 1);
      },
    );

    // idempotent: a second fail does not re-notify
    await assertHandler(
      kc,
      workerContext,
      handlerConfig,
      { uuid, message: "boom again" },
      async (response: ResultInterface) => {
        assertEquals(response.status, "failure");
        assertEquals(errorCalls.length, 1);
        assertEquals(supportCalls.length, 1);
      },
    );
  });

  it("still marks failure when notification throws (best-effort)", async () => {
    // rebind notification to throw — the row must still end up 'failure'
    forceBind(NotificationService, {
      success: async () => {},
      error: async () => {
        throw new Error("mail down");
      },
      support: async () => {},
    });

    const uuid = await seedRunning();

    await assertHandler(
      kc,
      workerContext,
      handlerConfig,
      { uuid, message: "boom" },
      async (response: ResultInterface) => {
        assertEquals(response.status, "failure");
        const row = await fetchByUuid(uuid);
        assertEquals(row.status, "failure");
        assertEquals(row.error?.message, "boom");
      },
    );
  });
});
