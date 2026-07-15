import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import { staleDelay } from "../config/export.ts";
import { Export, ExportStatus } from "../models/Export.ts";
import { LogServiceInterfaceResolver } from "../services/LogService.ts";

export type ExportCreateData = Pick<Export, "created_by" | "target" | "params">;
export type ExportUpdateData = Partial<
  Pick<Export, "status" | "filename" | "file_size" | "error">
>;

export abstract class ExportRepositoryInterfaceResolver {
  /**
   * Create an new export in the database
   *
   * The export is created with a `pending` status.
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
  public async get(id: number, _userId?: number): Promise<Export>;

  /**
   * Get an export by its UUID
   *
   * @param {string} id
   * @returns {Promise<Export>}
   */
  public async get(id: string, _userId?: number): Promise<Export>;

  // Method overloading implementation
  public async get(_id: number | string, _userId?: number): Promise<Export> {
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
   * Atomically claim the oldest pending export of the given targets
   *
   * Flips the row from `pending` to `running` in a single statement,
   * using `FOR UPDATE SKIP LOCKED` so concurrent workers never claim the
   * same row. Returns the claimed export or `null` when the queue is empty.
   *
   * @param {string[]} _targets
   * @returns {Promise<Export | null>}
   */
  public async claim(_targets: string[]): Promise<Export | null> {
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

  constructor(
    protected connection: DenoPostgresConnection,
    protected logger: LogServiceInterfaceResolver,
  ) {}

  public async create(data: ExportCreateData): Promise<Export> {
    const { created_by, target, params } = data;

    const rows = await this.connection.query(sql`
        INSERT INTO ${raw(this.exportsTable)}
        (created_by, target, params)
        VALUES (${created_by}, ${target}, ${JSON.stringify(params.get())}::json)
        RETURNING *
      `);
    const exp = Export.fromJSON(rows[0]);

    // log the creation event
    await this.logger.created(exp._id);

    return exp;
  }

  public async get(id: number | string, userId?: number): Promise<Export | null> {
    const field = typeof id === "number" ? "_id" : "uuid";
    const rows = await this.connection.query(sql`
      SELECT *
      FROM ${raw(this.exportsTable)}
      WHERE ${raw(field)} = ${id}
      ${userId ? sql`AND created_by = ${userId}` : sql``}
    `);
    return rows.length ? Export.fromJSON(rows[0]) : null;
  }

  private static readonly UPDATABLE_COLUMNS: ReadonlySet<string> = new Set([
    "status", "filename", "file_size", "error",
  ]);

  public async update(id: number, data: ExportUpdateData): Promise<void> {
    const props = Object.keys(data)
      .filter((key) => ExportRepository.UPDATABLE_COLUMNS.has(key))
      .map((key) => sql`${raw(key)} = ${data[key as keyof ExportUpdateData]}`);

    if (!props.length) return;

    const query = sql`
      UPDATE ${raw(this.exportsTable)}
      SET ${join(props)}
      WHERE _id = ${id}
    `;
    await this.connection.query(query);
  }

  public async delete(id: number): Promise<void> {
    await this.connection.query(sql`DELETE FROM ${raw(this.exportsTable)} WHERE _id = ${id}`);
  }

  public async list(filters?: { created_by?: number; days?: number }): Promise<Export[]> {
    const rows = await this.connection.query(sql`
      SELECT *
      FROM ${raw(this.exportsTable)}
      WHERE true
      ${filters?.days ? sql`AND created_at >= NOW() - ${filters.days} * interval '1 days'` : sql``}
      ${filters?.created_by ? sql`AND created_by = ${filters.created_by}` : sql``}
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

  public async pickPending(): Promise<Export | null> {
    const rows = await this.connection.query(sql`
        SELECT * FROM ${raw(this.exportsTable)}
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT 1
      `);
    return rows.length ? Export.fromJSON(rows[0]) : null;
  }

  public async claim(targets: string[]): Promise<Export | null> {
    const rows = await this.connection.query(sql`
      UPDATE ${raw(this.exportsTable)}
      SET status = ${ExportStatus.RUNNING}
      WHERE _id = (
        SELECT _id FROM ${raw(this.exportsTable)}
        WHERE status = 'pending'
          AND target = ANY(${targets})
        ORDER BY created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING *
    `);
    return rows.length ? Export.fromJSON(rows[0]) : null;
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
