import { NotFoundException, provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";
import { logger } from "@/lib/logger/index.ts";
import { InseeCoderInterface, PointInterface } from "../interfaces/index.ts";

@provider()
export class LocalGeoProvider implements InseeCoderInterface {
  protected fn = "geo.get_latest_by_point";
  protected fb = "geo.get_closest_country";
  protected fbclose = "geo.get_closest_com";

  constructor(protected connection: DenoPostgresConnection) {}

  async positionToInsee(geo: PointInterface): Promise<string> {
    const { lat, lon } = geo;

    try {
      const inCom = await this.connection.query<{ arr: string }>(sql`
        SELECT arr
        FROM ${raw(this.fn)}(${lon}::float, ${lat}::float)
        WHERE arr NOT IN ('XXXXX','99100')
      `);
      if (inCom.length > 0) {
        return inCom[0].arr;
      }

      const outFr = await this.connection.query<{ arr: string }>(sql`
        SELECT arr
        FROM ${raw(this.fb)}(${lon}::float, ${lat}::float)
        WHERE com IS NULL
      `);
      if (outFr.length > 0) {
        return outFr[0].arr;
      }

      const closeCom = await this.connection.query<{ arr: string }>(sql`
        SELECT arr
        FROM ${raw(this.fbclose)}(${lon}::float, ${lat}::float, 1000)
      `);
      if (closeCom.length === 0) {
        throw new NotFoundException();
      }

      return closeCom[0].arr;
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      logger.error(`[LocalGeoProvider] (${lon},${lat}) ${message}`);
      throw e;
    }
  }
}
