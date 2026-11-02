import { PostgresError } from "dep:postgres";
import { ConflictException, NotFoundException, provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import type {
  CreateCampaignCondition,
  UpdateCampaignCondition,
} from "@/pdc/services/dashboard/dto/CampaignConditions.ts";
import {
  TerritoryGroupShape,
  toCampaignTerritory,
} from "@/pdc/services/dashboard/helpers/campaignConditionTerritory.ts";
import {
  CampaignConditionAuthor,
  CampaignConditionRow,
  CampaignConditionsFilter,
  CampaignConditionsRepositoryInterface,
  CampaignConditionsRepositoryInterfaceResolver,
  ResolvedTerritory,
} from "@/pdc/services/dashboard/interfaces/CampaignConditionsRepositoryInterface.ts";

const COLUMNS = [
  "territory_type",
  "territory_code",
  "territory_label",
  "operator_ids",
  "premiere_campagne",
  "date_debut",
  "date_fin",
  "budget_incitations",
  "conducteur_montant_min_par_passager",
  "conducteur_montant_max_par_passager",
  "conducteur_montant_max_par_mois",
  "conducteur_trajets_max_par_mois",
  "passager_gratuite",
  "passager_eligible_gratuite",
  "passager_reduction_ticket",
  "passager_eligibilite_reduction",
  "passager_montant_ticket",
  "passager_trajets_max_par_mois",
  "zone_sens_des_trajets",
  "zone_exclusion",
  "zone_exclusion_liste",
  "autre_exclusion",
  "trajet_longueur_min",
  "trajet_longueur_max",
  "trajet_classe_preuve",
  "lien_page_collectivite",
  "autres_informations",
] as const satisfies readonly (keyof CreateCampaignCondition)[];

const UNIQUE_VIOLATION = "23505";

@provider({
  identifier: CampaignConditionsRepositoryInterfaceResolver,
})
export class CampaignConditionsRepository implements CampaignConditionsRepositoryInterface {
  private readonly table = raw("policy.campaign_conditions");

  constructor(private pgConnection: DenoPostgresConnection) {}

  async list(params: CampaignConditionsFilter): Promise<CampaignConditionRow[]> {
    const filters = [sql`deleted_at IS NULL`];
    if (params.operator_id !== undefined) filters.push(sql`operator_ids @> ARRAY[${params.operator_id}]::int[]`);
    if (params.id !== undefined) filters.push(sql`_id = ${params.id}`);
    if (params.territory) {
      const { id, resolved } = params.territory;
      filters.push(
        resolved
          ? sql`(created_by_territory_id = ${id} OR (territory_type = ${resolved.territory_type} AND territory_code = ${resolved.territory_code}))`
          : sql`created_by_territory_id = ${id}`,
      );
    }
    return this.pgConnection.query<CampaignConditionRow>(sql`
      SELECT
        _id AS id,
        ${raw(COLUMNS.filter((c) => !c.startsWith("date_")).join(", "))},
        date_debut::text AS date_debut,
        date_fin::text AS date_fin,
        created_by_operator_id,
        created_by_territory_id,
        status,
        validated_at,
        created_at,
        updated_at
      FROM ${this.table}
      WHERE ${join(filters, " AND ")}
      ORDER BY date_debut DESC, _id DESC
    `);
  }

  async find(id: number): Promise<CampaignConditionRow | null> {
    const [row] = await this.list({ id });
    return row ?? null;
  }

  async create(data: CreateCampaignCondition, author: CampaignConditionAuthor): Promise<{ id: number }> {
    const rows = await this.unique(() =>
      this.pgConnection.query<{ id: number }>(sql`
        INSERT INTO ${this.table} (${raw(COLUMNS.join(", "))}, created_by_operator_id, created_by_territory_id)
        VALUES (${join(COLUMNS.map((c) => sql`${data[c]}`))}, ${author.operator_id}, ${author.territory_id})
        RETURNING _id AS id
      `)
    );
    return rows[0];
  }

  // Toute modification repasse la campagne en brouillon : elle doit être revalidée.
  async update(data: UpdateCampaignCondition): Promise<void> {
    const assignments = COLUMNS.map((c) => sql`${raw(c)} = ${data[c]}`);
    const rows = await this.unique(() =>
      this.pgConnection.query(sql`
        UPDATE ${this.table}
        SET ${join(assignments)}, status = 'draft', validated_at = NULL, validated_by = NULL, updated_at = now()
        WHERE _id = ${data.id} AND deleted_at IS NULL
        RETURNING _id
      `)
    );
    if (rows.length !== 1) throw new NotFoundException(`campaign condition not found: (${data.id})`);
  }

  async delete(id: number): Promise<void> {
    const rows = await this.pgConnection.query(sql`
      UPDATE ${this.table} SET deleted_at = now() WHERE _id = ${id} AND deleted_at IS NULL RETURNING _id
    `);
    if (rows.length !== 1) throw new NotFoundException(`campaign condition not found: (${id})`);
  }

  async validate(id: number, userId: number | null): Promise<void> {
    const rows = await this.pgConnection.query(sql`
      UPDATE ${this.table}
      SET status = 'validated', validated_at = now(), validated_by = ${userId}, updated_at = now()
      WHERE _id = ${id} AND deleted_at IS NULL
      RETURNING _id
    `);
    if (rows.length !== 1) throw new NotFoundException(`campaign condition not found: (${id})`);
  }

  async resolveTerritory(territoryId: number): Promise<ResolvedTerritory | null> {
    const [row] = await this.pgConnection.query<TerritoryGroupShape & { found: boolean }>(sql`
      SELECT
        true AS found,
        tg.name,
        EXISTS (SELECT 1 FROM territory.territory_perimeters p WHERE p.territory_id = tg._id) AS has_versions,
        coalesce(
          (SELECT json_agg(json_build_object('type', s.selector_type, 'value', s.selector_value))
           FROM territory.territory_group_selector s WHERE s.territory_group_id = tg._id),
          '[]'::json
        ) AS selectors
      FROM territory.territory_group tg
      WHERE tg._id = ${territoryId} AND tg.deleted_at IS NULL
    `);
    return row ? toCampaignTerritory(territoryId, row) : null;
  }

  private async unique<T>(run: () => Promise<T>): Promise<T> {
    try {
      return await run();
    } catch (e) {
      if (e instanceof PostgresError && e.fields.code === UNIQUE_VIOLATION) {
        throw new ConflictException("Une campagne existe déjà pour ce territoire à cette date de début");
      }
      throw e;
    }
  }
}
