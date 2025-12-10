import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { logger } from "@/lib/logger/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";
import { staleDelay } from "../config/export.ts";
import { Export, ExportStatus } from "../models/Export.ts";
import { ExportRecipient } from "../models/ExportRecipient.ts";
import { LogServiceInterfaceResolver } from "../services/LogService.ts";

export type ExportCreateData =
  & Pick<Export, "created_by" | "target" | "params">
  & { recipients: ExportRecipient[] };
export type ExportUpdateData = Partial<
  Pick<Export, "status" | "progress" | "download_url" | "filename" | "file_size" | "error" | "stats">
>;
export type ExportProgress = (progress: number) => Promise<void>;

export abstract class ExportRepositoryInterfaceResolver {
  /**
   * Create an new export in the database
   *
   * The export is created with a `pending` status.
   * Recipients are added to the export if passed in the data and
   * the creator is added as a recipient if not already in the list.
   *
   * @param {ExportCreateData} _data
   * @returns {Promise<Export>}
   */
  public async create(_data: ExportCreateData): Promise<Export> {
    throw new Error("Not implemented");
  }

  /**
   * Get an export by its id
   *
   * @param {number} id
   * @returns {Promise<Export>}
   */
  public async get(id: number): Promise<Export>;

  /**
   * Get an export by its UUID
   *
   * @param {string} id
   * @returns {Promise<Export>}
   */
  public async get(id: string): Promise<Export>;

  // Method overloading implementation
  public async get(_id: number | string): Promise<Export> {
    throw new Error("Not implemented");
  }

  /**
   * Update an export by its id
   *
   * State information can be updated with this method, not the configuration
   * of the initial export.
   *
   * @param {number} _id
   * @param {ExportUpdateData} _data
   * @returns {Promise<void>}
   */
  public async update(_id: number, _data: ExportUpdateData): Promise<void> {
    throw new Error("Not implemented");
  }

  /**
   * Hard delete an export by its id
   *
   * @param {number} _id
   * @returns {Promise<void>}
   */
  public async delete(_id: number): Promise<void> {
    throw new Error("Not implemented");
  }

  /**
   * List all exports
   *
   * @todo add pagination
   * @todo add filters (user_id, status, ...)
   *
   * @param _filters
   * @returns {Promise<Export[]>}
   */
  public async list(_filters?: { created_by?: number; days?: number }): Promise<Export[]> {
    throw new Error("Not implemented");
  }

  /**
   * Set the status of an export
   *
   * @param {number} _id
   * @param {ExportStatus} _status
   * @returns {Promise<void>}
   */
  public async status(_id: number, _status: ExportStatus): Promise<void> {
    throw new Error("Not implemented");
  }

  /**
   * Set the error context of an export
   *
   * @param {number} _id
   * @param {string | Error} _error
   * @returns {Promise<void>}
   */
  public async error(_id: number, _error: string): Promise<void> {
    throw new Error("Not implemented");
  }

  /**
   * Progress callback
   *
   * to be injected in the carpool repository to be able
   * to update the `progress` field of the export as the export is running
   *
   * @param {number} _id
   * @returns {ExportProgress}
   */
  public async progress(_id: number): Promise<ExportProgress> {
    throw new Error("Not implemented");
  }

  /**
   * Pick pending exports
   *
   * Exports are picked in a FIFO manner (older first).
   *
   * @returns {Promise<Export | null>}
   */
  public async pickPending(): Promise<Export | null> {
    throw new Error("Not implemented");
  }

  /**
   * Get the recipients of an export
   *
   * @param _id Export _id
   */
  public async recipients(_id: number): Promise<ExportRecipient[]> {
    throw new Error("Not implemented");
  }

  /**
   * Add a recipient to an export
   *
   * Recipients will receive notifications when the export is done.
   *
   * @param {number} _export_id
   * @param {ExportRecipient} _recipient
   */
  public async addRecipient(
    _export_id: number,
    _recipient: ExportRecipient,
  ): Promise<void> {
    throw new Error("Not implemented");
  }

  /**
   * Fail stale exports
   *
   * This method is called by the process command to fail exports that are
   * stuck in the `running` status for too long.
   */
  public async failStaleExports(): Promise<void> {
    throw new Error("Not implemented");
  }
}

@provider({
  identifier: ExportRepositoryInterfaceResolver,
})
export class ExportRepository {
  protected readonly exportsTable = "export.exports";
  protected readonly recipientsTable = "export.recipients";

  constructor(
    protected connection: DenoPostgresConnection,
    protected logger: LogServiceInterfaceResolver,
  ) {}

  public async create(data: ExportCreateData): Promise<Export> {
    const { created_by, target, params, recipients } = data;

    const rows = await this.connection.query(sql`
        INSERT INTO ${raw(this.exportsTable)}
        (created_by, target, params)
        VALUES (${created_by}, ${target}, ${JSON.stringify(params.get())}::json)
        RETURNING *
      `);
    const exp = Export.fromJSON(rows[0]);

    // add recipients
    for (const recipient of recipients) {
      await this.addRecipient(exp._id, recipient);
    }

    // log the creation event
    await this.logger.created(exp._id);

    return exp;
  }

  public async get(id: number | string): Promise<Export> {
    const field = typeof id === "number" ? "_id" : "uuid";
    const rows = await this.connection.query(sql`
      SELECT *
      FROM ${raw(this.exportsTable)}
      WHERE ${raw(field)} = ${id}
    `);
    return Export.fromJSON(rows[0]);
  }

  public async update(id: number, data: ExportUpdateData): Promise<void> {
    await this.connection.query(sql`
      UPDATE ${raw(this.exportsTable)}
      SET ${Object.keys(data).map((key) => `${raw(key)} = ${data[key as keyof ExportUpdateData]}`).join(", ")}
      WHERE _id = ${id}
    `);
  }

  public async delete(id: number): Promise<void> {
    await this.connection.query(sql`DELETE FROM ${raw(this.exportsTable)} WHERE _id = ${id}`);
  }

  public async list(filters?: { created_by?: number; days?: number }): Promise<Export[]> {
    const rows = await this.connection.query(sql`
      SELECT *
      FROM ${raw(this.exportsTable)}
      WHERE true
      ${filters?.created_by ? sql`AND created_by = ${filters.created_by}` : sql``}
      ${filters?.days ? sql`AND created_at >= NOW() - INTERVAL '${filters.days} days'` : sql``}
      ORDER BY created_at DESC
    `);

    return rows.map(Export.fromJSON);
  }

  public async status(id: number, status: ExportStatus): Promise<void> {
    // update the export status
    await this.connection.query(sql`
      UPDATE ${raw(this.exportsTable)}
      SET status = ${status}::text
      WHERE _id = ${id}
    `);

    // log depending on the status
    switch (status) {
      case ExportStatus.RUNNING:
        await this.logger.running(id);
        break;
      case ExportStatus.SUCCESS:
        await this.logger.success(id);
        break;
      case ExportStatus.FAILURE:
        await this.logger.failure(id);
        break;
    }
  }

  public async error(id: number, error: string | Error): Promise<void> {
    // cast the Error
    const { message, stack } = error instanceof Error && "message" in error ? error : new Error(error);

    // log error event
    await this.logger.failure(id, message);

    // update the export status
    await this.connection.query(sql`
      UPDATE ${raw(this.exportsTable)}
      SET status = ${ExportStatus.FAILURE},
          error = ${JSON.stringify({ message, stack })}::json
      WHERE _id = ${id}
    `);
  }

  public async progress(id: number): Promise<ExportProgress> {
    return async (progress: number): Promise<void> => {
      logger.info(`Export #${id} progress: ${progress}%`);

      await this.connection.query(sql`
        UPDATE ${raw(this.exportsTable)}
        SET progress = ${progress}::int
        WHERE _id = ${id}
      `);
    };
  }

  public async pickPending(): Promise<Export | null> {
    const rows = await this.connection.query(sql`
        SELECT * FROM ${raw(this.exportsTable)}
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT 1
      `);
    return rows.length ? Export.fromJSON(rows[0]) : null;
  }

  public async recipients(id: number): Promise<ExportRecipient[]> {
    const rows = await this.connection.query<ExportRecipient>(sql`
      SELECT *
      FROM ${raw(this.recipientsTable)}
      WHERE export_id = ${id}
    `);

    return rows.map(ExportRecipient.fromJSON);
  }

  public async addRecipient(
    export_id: number,
    recipient: ExportRecipient,
  ): Promise<void> {
    if (!export_id) throw new Error("Export _id is required");
    if (!recipient.email) throw new Error("Recipient email is required");

    await this.connection.query(sql`
      INSERT INTO ${raw(this.recipientsTable)}
      (export_id, email, fullname, message)
      VALUES (
        ${export_id},
        ${recipient.email},
        ${recipient.fullname},
        ${recipient.message}
      )
    `);
  }

  public async failStaleExports(): Promise<void> {
    const query = sql`
      UPDATE ${raw(this.exportsTable)}
      SET status = ${ExportStatus.FAILURE}
      WHERE status = ${ExportStatus.RUNNING}
        AND created_at < NOW() - ${staleDelay}::interval
    `;

    await this.connection.query(query);
  }
}
