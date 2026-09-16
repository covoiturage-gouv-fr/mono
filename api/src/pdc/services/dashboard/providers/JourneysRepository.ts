import { NotFoundException, provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import {
  JourneysIncentiveByDayParamsInterface,
  JourneysIncentiveByDayResultInterface,
  JourneysIncentiveByMonthParamsInterface,
  JourneysIncentiveByMonthResultInterface,
  JourneysOperatorsByDayParamsInterface,
  JourneysOperatorsByDayResultInterface,
  JourneysOperatorsByMonthParamsInterface,
  JourneysOperatorsByMonthResultInterface,
  JourneysRepositoryInterface,
  JourneysRepositoryInterfaceResolver,
} from "../interfaces/JourneysRepositoryInterface.ts";
import { CallerScope } from "../interfaces/JourneysRepositoryInterface.ts";

@provider({
  identifier: JourneysRepositoryInterfaceResolver,
})
export class JourneysRepository implements JourneysRepositoryInterface {
  private readonly tableByMonth = "dashboard_stats.campaigns_by_month";
  private readonly tableByDay = "dashboard_stats.campaigns_by_day";
  private readonly tableOperators = "operator.operators";
  private readonly tablePolicies = "policy.policies";

  constructor(private pgConnection: DenoPostgresConnection) {}

  /**
   * Cloisonne une statistique de campagne au périmètre de l'appelant.
   *
   * `campaign_id` vient du corps et la seule garde était `common.observatory.stats`, détenue par
   * tout compte : n'importe qui lisait les montants d'incitation, opérateur par opérateur, de la
   * campagne d'un autre territoire. Un territoire ne voit que ses campagnes ; un opérateur ne
   * voit que ses propres lignes (filtre ci-dessous) ; un compte RPC n'est pas restreint.
   */
  private async assertCampaignInScope(params: { campaign_id: number } & CallerScope): Promise<void> {
    if (!params.territory_id) return;

    const rows = await this.pgConnection.query<{ one: number }>(sql`
      SELECT 1 AS one
      FROM ${raw(this.tablePolicies)}
      WHERE _id = ${params.campaign_id} AND territory_id = ${params.territory_id}
      LIMIT 1
    `);
    if (!rows.length) throw new NotFoundException();
  }

  // Un appelant opérateur ne lit que ses propres chiffres, jamais le détail de ses concurrents.
  private operatorFilter(params: CallerScope) {
    return params.operator_id ? [sql`operator_id = ${params.operator_id}`] : [];
  }

  async getIncentiveByDay(
    params: JourneysIncentiveByDayParamsInterface & CallerScope,
  ): Promise<JourneysIncentiveByDayResultInterface[]> {
    await this.assertCampaignInScope(params);
    const date = params.date ? new Date(params.date) : new Date();
    const filters = [
      sql`campaign_id = ${params.campaign_id}`,
      sql`start_date <= ${date.toISOString().split("T")[0]}`,
      sql`start_date >= ${new Date(date.setMonth(date.getMonth() - 2)).toISOString().split("T")[0]}`,
      ...this.operatorFilter(params),
    ];
    const query = sql`
      SELECT 
        to_char(start_date, 'YYYY-MM-DD') AS start_date,
        campaign_id,
        sum(journeys) as journeys,
        sum(incented_journeys) as incented_journeys,
        sum(incentive_amount) as incentive_amount
      FROM ${raw(this.tableByDay)}
      WHERE ${join(filters, ` AND `)}
      GROUP BY 1,2
      ORDER BY 1
    `;
    const rows = await this.pgConnection.query<JourneysIncentiveByDayResultInterface>(query);
    return rows;
  }

  async getIncentiveByMonth(
    params: JourneysIncentiveByMonthParamsInterface & CallerScope,
  ): Promise<JourneysIncentiveByMonthResultInterface[]> {
    await this.assertCampaignInScope(params);
    const filters = [
      sql`campaign_id = ${params.campaign_id}`,
      ...this.operatorFilter(params),
    ];
    if (params.year) {
      filters.push(sql`year = ${params.year}`);
    }

    const query = sql`
      SELECT 
        year,
        month,
        campaign_id,
        sum(journeys) as journeys,
        sum(incented_journeys) as incented_journeys,
        sum(incentive_amount) as incentive_amount
      FROM ${raw(this.tableByMonth)}
      WHERE ${join(filters, " AND ")}
      GROUP BY 1,2,3
      ORDER BY 1,2
    `;
    const rows = await this.pgConnection.query<JourneysIncentiveByMonthResultInterface>(query);
    return rows;
  }

  async getOperatorsByDay(
    params: JourneysOperatorsByDayParamsInterface & CallerScope,
  ): Promise<JourneysOperatorsByDayResultInterface[]> {
    await this.assertCampaignInScope(params);
    const date = params.date ? new Date(params.date) : new Date();
    const filters = [
      sql`campaign_id = ${params.campaign_id}`,
      sql`start_date <= ${date.toISOString().split("T")[0]}`,
      sql`start_date >= ${new Date(date.setMonth(date.getMonth() - 2)).toISOString().split("T")[0]}`,
      ...this.operatorFilter(params),
    ];

    const query = sql`
      SELECT 
        to_char(a.start_date, 'YYYY-MM-DD') AS start_date,
        a.campaign_id,
        a.operator_id,
        b.name as operator_name,
        a.journeys,
        a.incented_journeys,
        a.incentive_amount
      FROM ${raw(this.tableByDay)} AS a
      LEFT JOIN ${raw(this.tableOperators)} AS b ON b._id = a.operator_id
      WHERE ${join(filters, " AND ")}
      ORDER BY 1,2,3
    `;
    const rows = await this.pgConnection.query<JourneysOperatorsByDayResultInterface>(query);
    return rows;
  }

  async getOperatorsByMonth(
    params: JourneysOperatorsByMonthParamsInterface & CallerScope,
  ): Promise<JourneysOperatorsByMonthResultInterface[]> {
    await this.assertCampaignInScope(params);
    const filters = [
      sql`campaign_id = ${params.campaign_id}`,
      ...this.operatorFilter(params),
    ];
    if (params.year) {
      filters.push(sql`year = ${params.year}`);
    }
    const query = sql`
      SELECT 
        a.year,
        a.month,
        a.campaign_id,
        a.operator_id,
        b.name as operator_name,
        a.journeys,
        a.incented_journeys,
        a.incentive_amount
      FROM ${raw(this.tableByMonth)} AS a
      LEFT JOIN ${raw(this.tableOperators)} AS b ON b._id = a.operator_id
      WHERE ${join(filters, " AND ")}
      ORDER BY 1,2,3,4
    `;
    const rows = await this.pgConnection.query<JourneysOperatorsByMonthResultInterface>(query);
    return rows;
  }
}
