import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";
import { ExportLog, ExportLogEvent } from "../models/ExportLog.ts";

export abstract class LogRepositoryInterfaceResolver {
  public async add(
    _export_id: number,
    _type: ExportLogEvent,
    _message: string,
  ): Promise<void> {
    throw new Error("Not implemented");
  }
  public async list(_export_id: number): Promise<ExportLog[]> {
    throw new Error("Not implemented");
  }
}

@provider({
  identifier: LogRepositoryInterfaceResolver,
})
export class LogRepository {
  protected readonly table = "export.logs";

  constructor(protected connection: DenoPostgresConnection) {}

  public async add(
    export_id: number,
    type: ExportLogEvent,
    message: string,
  ): Promise<void> {
    await this.connection.query(sql`
      INSERT INTO ${raw(this.table)} (export_id, type, message)
      VALUES (${export_id}, ${type}, ${message})
    `);
  }

  public async list(export_id: number): Promise<ExportLog[]> {
    const rows = await this.connection.query(sql`
      SELECT *
      FROM ${raw(this.table)}
      WHERE export_id = ${export_id}
      ORDER BY created_at DESC
    `);

    return rows.map(ExportLog.fromJSON);
  }
}
