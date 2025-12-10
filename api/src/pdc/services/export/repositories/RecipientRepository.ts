import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";

export type Recipient = {
  _id: number;
  scrambled_at: Date;
  export_id: number;
  email: string;
  fullname: string;
  message: string;
};

export type CreateRecipientData = Pick<
  Recipient,
  "export_id" | "email" | "fullname" | "message"
>;

export abstract class RecipientRepositoryInterfaceResolver {
  public async create(_data: CreateRecipientData): Promise<number> {
    throw new Error("Not implemented");
  }
  public async anonymize(_export_id: number): Promise<void> {
    throw new Error("Not implemented");
  }
}

@provider({
  identifier: RecipientRepositoryInterfaceResolver,
})
export class RecipientRepository {
  protected readonly table = "export.recipients";

  constructor(protected connection: DenoPostgresConnection) {}

  public async create(data: CreateRecipientData): Promise<number> {
    const rows = await this.connection.query<{ _id: number }>(sql`
      INSERT INTO ${raw(this.table)}
        (export_id, email, fullname, message)
        VALUES (
          ${data.export_id},
          ${data.email},
          ${data.fullname},
          ${data.message}
        )
      RETURNING _id`);
    return rows[0]._id;
  }

  public async anonymize(export_id: number): Promise<void> {
    await this.connection.query(sql`
        UPDATE ${raw(this.table)}
        SET
          scrambled_at = NOW(),
          email = NULL,
          fullname = NULL,
          message = NULL
        WHERE export_id = ${export_id}`);
  }
}
