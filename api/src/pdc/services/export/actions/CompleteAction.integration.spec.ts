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
import { Export } from "@/pdc/services/export/models/Export.ts";
import { NotificationService } from "@/pdc/services/export/services/NotificationService.ts";
import { StorageService } from "@/pdc/services/export/services/StorageService.ts";
import { handlerConfig, ResultInterface } from "../contracts/complete.contract.ts";

const { before: kernelBefore, after: kernelAfter } = makeKernelBeforeAfter(ExportSP);
const { before: denoDbBefore, after: denoDbAfter } = makeDenoDbBeforeAfter();

describe("CompleteAction", () => {
  let db: DenoDbContext;
  let kc: KernelContext;

  const successCalls: Array<{ exp: Export; url: string }> = [];

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
    const rows = await db.connection.query<{ status: string; filename: string; file_size: number }>(sql`
      SELECT status, filename, file_size FROM export.exports WHERE uuid = ${uuid}
    `);
    return rows[0];
  }

  function forceBind(token: unknown, value: unknown) {
    const container = kc.kernel.getContainer();
    // deno-lint-ignore no-explicit-any
    const c = container as any;
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

    // spy the notification + storage so we exercise the transition, not real S3/email
    forceBind(NotificationService, {
      success: async (exp: Export, url: string) => {
        successCalls.push({ exp, url });
      },
      error: async () => {},
      support: async () => {},
    });
    forceBind(StorageService, {
      getPublicUrl: async (filename: string) => `https://example.test/${filename}`,
    });
  });

  afterAll(async () => {
    await kernelAfter(kc);
    await denoDbAfter(db);
  });

  it("marks a running export success, stores filename/size, emails the creator once", async () => {
    const uuid = await seedRunning();

    await assertHandler(
      kc,
      workerContext,
      handlerConfig,
      { uuid, file_size: 4096 },
      async (response: ResultInterface) => {
        assertEquals(response.status, "success");

        const row = await fetchByUuid(uuid);
        assertEquals(row.status, "success");
        assertEquals(row.filename, `${uuid}.csv.zip`);
        assertEquals(Number(row.file_size), 4096);

        assertEquals(successCalls.length, 1);
        assertEquals(successCalls[0].url, `https://example.test/${uuid}.csv.zip`);
      },
    );

    // idempotent: a second complete does not re-email
    await assertHandler(
      kc,
      workerContext,
      handlerConfig,
      { uuid, file_size: 4096 },
      async (response: ResultInterface) => {
        assertEquals(response.status, "success");
        assertEquals(successCalls.length, 1);
      },
    );
  });

  it("does not resurrect a reaped (failure) export and does not email", async () => {
    const rows = await db.connection.query<{ uuid: string }>(sql`
      INSERT INTO export.exports (created_by, target, status, params)
      VALUES (1, 'operator', 'failure', ${JSON.stringify(params)}::json)
      RETURNING uuid
    `);
    const uuid = rows[0].uuid;
    const emailsBefore = successCalls.length;

    await assertHandler(
      kc,
      workerContext,
      handlerConfig,
      { uuid, file_size: 4096 },
      async (response: ResultInterface) => {
        assertEquals(response.status, "failure"); // stays failure, not resurrected
        const row = await fetchByUuid(uuid);
        assertEquals(row.status, "failure");
        assertEquals(successCalls.length, emailsBefore); // no download-link email
      },
    );
  });
});
