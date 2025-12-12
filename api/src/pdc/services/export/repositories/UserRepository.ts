import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";

export interface UserInterface {
  _id: number;
  email: string;
  fullname: string;
}

export abstract class UserRepositoryInterfaceResolver {
  public async find(_id: number): Promise<UserInterface | null> {
    throw new Error("Not implemented");
  }
}

@provider({
  identifier: UserRepositoryInterfaceResolver,
})
export class UserRepository {
  protected readonly table = "auth.users";

  constructor(protected connection: DenoPostgresConnection) {}

  public async find(_id: number): Promise<UserInterface | null> {
    const rows = await this.connection.query<UserInterface>(sql`
      SELECT _id, email, CONCAT(firstname, ' ', lastname) as fullname
      FROM ${raw(this.table)}
      WHERE _id = ${_id}  
    `);

    return rows.length ? rows[0] : null;
  }
}
