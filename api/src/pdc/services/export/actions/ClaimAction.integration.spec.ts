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
import { handlerConfig, ResultInterface } from "../contracts/claim.contract.ts";

const { before: kernelBefore, after: kernelAfter } = makeKernelBeforeAfter(ExportSP);
const { before: denoDbBefore, after: denoDbAfter } = makeDenoDbBeforeAfter();

describe("ClaimAction", () => {
  let db: DenoDbContext;
  let kc: KernelContext;

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

  async function seedPending(target: string): Promise<string> {
    const rows = await db.connection.query<{ uuid: string }>(sql`
      INSERT INTO export.exports (created_by, target, status, params)
      VALUES (1, ${target}, 'pending', ${JSON.stringify(params)}::json)
      RETURNING uuid
    `);
    return rows[0].uuid;
  }

  async function fetchByUuid(uuid: string): Promise<{ status: string } | undefined> {
    const rows = await db.connection.query<{ status: string }>(sql`
      SELECT status FROM export.exports WHERE uuid = ${uuid}
    `);
    return rows[0];
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
  });

  afterAll(async () => {
    await kernelAfter(kc);
    await denoDbAfter(db);
  });

  it("claims the oldest pending export of the given target and marks it running", async () => {
    const uuid = await seedPending("operator");

    await assertHandler(
      kc,
      workerContext,
      handlerConfig,
      { targets: ["operator"] },
      async (response: ResultInterface) => {
        assertEquals(response !== null, true);
        assertEquals(response!.target, "operator");
        assertEquals(response!.uuid, uuid);

        const row = await fetchByUuid(response!.uuid);
        assertEquals(row?.status, "running");
      },
    );
  });

  it("returns null when no pending export matches", async () => {
    await assertHandler(
      kc,
      workerContext,
      handlerConfig,
      { targets: ["territory"] },
      async (response: ResultInterface) => {
        assertEquals(response, null);
      },
    );
  });
});
