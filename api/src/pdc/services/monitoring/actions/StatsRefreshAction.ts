import { ConfigInterfaceResolver, handler } from "@/ilos/common/index.ts";
import { LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { env_or_false } from "@/lib/env/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { internalOnlyMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/statsrefresh.contract.ts";
import { alias } from "../contracts/statsrefresh.schema.ts";
import { filterTables } from "../helpers/filterTables.helper.ts";
import { todayFrequencies } from "../helpers/todayFrequencies.helper.ts";
import { MatviewItem } from "../interfaces/StatsRefreshInterfaces.ts";

@handler({
  ...handlerConfig,
  // internalOnlyMiddlewares(<service>) est une LISTE BLANCHE : avec "proxy" elle autorisait
  // précisément le canal HTTP, donc tout compte connecté pouvait déclencher un REFRESH
  // MATERIALIZED VIEW en boucle via /rpc. Sans argument, le canal proxy est refusé.
  middlewares: [...internalOnlyMiddlewares(), ["validate", alias]],
})
export class StatsRefreshAction extends AbstractAction {
  constructor(
    protected pg: LegacyPostgresConnection,
    protected config: ConfigInterfaceResolver,
  ) {
    super();
  }

  public async handle({ schema }: ParamsInterface): Promise<ResultInterface> {
    if (env_or_false("APP_DISABLE_STATS_REFRESH")) {
      return;
    }
    const cn = await this.pg.getClient().connect();
    try {
      const views = await cn.query<MatviewItem>({
        text: "SELECT matviewname FROM pg_matviews WHERE schemaname = $1",
        values: [schema],
      });

      if (!views.rowCount) {
        logger.info("No materialized views to refresh");
        return;
      }

      // get the frequencies that can run today
      const frequencies = todayFrequencies();

      // Filter the tables based on today's frequencies
      // Unprefixed tables will be run every day
      // Refreshing can be skipped by add 'schema.matview' to the config.refresh.skip list
      const tables = filterTables(this.config, frequencies, schema, views.rows);

      logger.info(
        `[monitoring:stats:refresh] Refresh materialised views: ${tables.sort().join(", ")}`,
      );

      for (const table of tables) {
        try {
          const bench = new Date().getTime();
          await cn.query(`REFRESH MATERIALIZED VIEW stats.${table}`);
          const ms = (new Date().getTime() - bench) / 1000;
          logger.info(
            `[monitoring:stats:refresh] (stats.${table}) refreshed in ${ms} seconds`,
          );
        } catch (e) {
          logger.error(
            `[monitoring:stats:refresh] (stats.${table}) ${e.message}`,
          );
        }
      }
    } catch (e) {
      logger.error(`[monitoring:stats:refresh] ${e.message}`);
    } finally {
      cn.release();
    }
  }
}
