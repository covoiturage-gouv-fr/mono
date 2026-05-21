import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";

import {
  DataSetInterface,
  ParamsInterface as StatsParamsInterface,
  ResultInterface as StatsResultInterface,
} from "../contracts/stats.contract.ts";

type StatsResponseRow = {
  day: string | null;
  type: "public" | "limited" | null;
  total: number;
};

export interface HonorRepositoryInterface {
  stats(params: StatsParamsInterface): Promise<StatsResultInterface>;
  save(type: string, employer: string): Promise<void>;
}

export abstract class HonorRepositoryInterfaceResolver implements HonorRepositoryInterface {
  async stats(params: StatsParamsInterface): Promise<StatsResultInterface> {
    throw new Error("Method not implemented.");
  }

  async save(type: string, employer: string): Promise<void> {
    throw new Error("Method not implemented.");
  }
}

@provider({
  identifier: HonorRepositoryInterfaceResolver,
})
export class HonorRepositoryProvider implements HonorRepositoryInterface {
  private readonly table = "honor.tracking";

  constructor(protected pgConnection: DenoPostgresConnection) {}

  async stats(_params: StatsParamsInterface): Promise<StatsResultInterface> {
    // substring from 11 for days
    // substring from 8 for months
    // TODO switch from days to months when we have enough data (starts 10/2020)
    const rows = await this.pgConnection.query<StatsResponseRow>(sql`
      SELECT
        to_char(journey_start_datetime::date, 'yyyy-mm-dd') as day,
        type,
        SUM(1)::int as total
      FROM ${raw(this.table)}
      GROUP BY day, type
      ORDER BY day, type
    `);

    return this.statsConvert(rows);
  }

  async save(type: string, employer: string): Promise<void> {
    await this.pgConnection.query(sql`
      INSERT INTO ${raw(this.table)} (type, employer) VALUES (${type}, ${employer})
    `);
  }

  /**
   * Convert the group by PostgreSQL response to a time series compatible with
   * ChartJS library
   */
  private statsConvert(rows: StatsResponseRow[]): StatsResultInterface {
    const count: { total: number; public: number; limited: number } = {
      total: 0,
      public: 0,
      limited: 0,
    };
    const labels: string[] = [];
    const datasets: DataSetInterface[] = [
      {
        label: "Public",
        data: [],
      },
      {
        label: "Limited",
        data: [],
      },
    ];

    rows.forEach((row: StatsResponseRow) => {
      // create a new day in labels and init counts to 0
      if (labels.indexOf(row.day) === -1) {
        labels.push(row.day);
        datasets[0].data[labels.length - 1] = 0;
        datasets[1].data[labels.length - 1] = 0;
      }

      // set the public/limited values if exists
      const idx = labels.indexOf(row.day);
      datasets[row.type === "public" ? 0 : 1].data[idx] = row.total;
      count[row.type] += row.total;
    });

    // set grand total
    count.total = count.public + count.limited;

    return { labels, datasets, count };
  }
}
