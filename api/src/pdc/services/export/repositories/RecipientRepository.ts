import { provider } from "@/ilos/common/index.ts";
import { LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";

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

  constructor(protected connection: LegacyPostgresConnection) {}

  public async create(data: CreateRecipientData): Promise<number> {
    const { rows } = await this.connection.getClient().query<any>({
      text: `
        INSERT INTO ${this.table}
        (export_id, email, fullname, message)
        VALUES ($1, $2, $3, $4)
        RETURNING _id`,
      values: [data.export_id, data.email, data.fullname, data.message],
    });
    return rows[0]._id;
  }

  public async anonymize(export_id: number): Promise<void> {
    await this.connection.getClient().query<any>({
      text: `
        UPDATE ${this.table}
        SET
          scrambled_at = NOW(),
          email = NULL,
          fullname = NULL,
          message = NULL
        WHERE export_id = $1`,
      values: [export_id],
    });
  }
}
