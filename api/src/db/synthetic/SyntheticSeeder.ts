import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql from "@/lib/pg/sql.ts";
import { parseTerritoryCodes } from "@/pdc/services/territory/helpers/perimeters.ts";
import { PerimeterRepositoryProvider } from "@/pdc/services/territory/providers/PerimeterRepositoryProvider.ts";
import { Commune, generateCarpools } from "./carpools.ts";
import { profileErrors } from "./profile.ts";
import { SyntheticCampaign, SyntheticProfile, SyntheticTerritory, SyntheticUser } from "./types.ts";

export interface SeedReport {
  operators: Record<string, number>;
  territories: Record<string, number>;
  policies: { _id: number; territory: string; handler: string; status: string }[];
  users: number;
  carpools: { generated: number; inserted: number };
}

// même début de validité que `territory:perimeter create`
const PERIMETER_FROM = new Date("2019-01-01T00:00:00Z");
const BATCH_SIZE = 500;

/**
 * Écrit en base le jeu de données d'un profil. Rejouable : chaque objet est retrouvé par sa clé
 * naturelle (SIRET, nom du territoire, e-mail, handler, operator_journey_id) avant d'être créé.
 */
export class SyntheticSeeder {
  protected perimeters: PerimeterRepositoryProvider;

  constructor(
    protected connection: DenoPostgresConnection,
    protected profile: SyntheticProfile,
    protected referenceDate: Date,
  ) {
    this.perimeters = new PerimeterRepositoryProvider(connection);
  }

  async run(): Promise<SeedReport> {
    const errors = profileErrors(this.profile);
    if (errors.length) {
      throw new Error(`Profil '${this.profile.name}' incohérent :\n- ${errors.join("\n- ")}`);
    }
    await this.assertGeo();
    await this.assertOwnOperators();
    await this.syncSequences();

    const operators: Record<string, number> = {};
    for (const operator of this.profile.operators) {
      await this.seedCompany(operator.siret, operator.legal_name, "4939B");
      operators[operator.key] = await this.seedOperator(operator);
    }

    const territories: Record<string, number> = {};
    const communes: Record<string, Commune[]> = {};
    for (const territory of this.profile.territories) {
      await this.seedCompany(territory.siret, territory.legal_name, "8411Z");
      territories[territory.key] = await this.seedTerritory(territory);
      communes[territory.key] = await this.findCommunes(territory, territories[territory.key]);
    }

    for (const user of this.profile.users) {
      await this.seedUser(user, operators, territories);
    }

    const policies: SeedReport["policies"] = [];
    for (const territory of this.profile.territories) {
      for (const campaign of territory.campaigns) {
        policies.push({
          territory: territory.key,
          handler: campaign.handler,
          ...await this.seedPolicy(territories[territory.key], campaign),
        });
      }
    }

    const carpools = generateCarpools(this.profile, this.referenceDate, communes)
      .map((c) => ({ ...c, operator_id: operators[c.operator] }));
    let inserted = 0;
    for (let i = 0; i < carpools.length; i += BATCH_SIZE) {
      inserted += await this.insertCarpools(carpools.slice(i, i + BATCH_SIZE));
    }

    return {
      operators,
      territories,
      policies,
      users: this.profile.users.length,
      carpools: { generated: carpools.length, inserted },
    };
  }

  protected async assertGeo(): Promise<void> {
    const rows = await this.connection.query<{ count: number }>(sql`
      SELECT count(*)::int AS count FROM geo.perimeters
    `);
    if (!rows[0].count) {
      throw new Error(
        "geo.perimeters est vide : charger les périmètres avec geo-import avant le seeding (src/db/geo/RUNBOOK.md)",
      );
    }
  }

  // Un UUID du profil déjà porté par un autre SIRET = base qui contient de vrais opérateurs : on n'y écrit rien.
  protected async assertOwnOperators(): Promise<void> {
    for (const operator of this.profile.operators) {
      const rows = await this.connection.query<{ _id: number }>(sql`
        SELECT _id FROM operator.operators
        WHERE uuid = ${operator.uuid}::uuid AND siret IS DISTINCT FROM ${operator.siret} AND deleted_at IS NULL
        LIMIT 1
      `);
      if (rows.length) {
        throw new Error(
          `L'UUID de l'opérateur '${operator.key}' est déjà porté par un autre opérateur (_id ${
            rows[0]._id
          }) : base non synthétique, génération refusée`,
        );
      }
    }
  }

  // `just seed` insère des _id explicites sans avancer les séquences : le prochain INSERT entrerait en collision.
  // GREATEST : une séquence ne recule jamais, un _id supprimé n'est pas réattribué.
  protected async syncSequences(): Promise<void> {
    await this.connection.query(sql`
      SELECT
        setval('company.companies__id_seq', GREATEST(
          (SELECT max(_id) FROM company.companies), (SELECT last_value FROM company.companies__id_seq)
        )) AS companies,
        setval('operator.operators__id_seq', GREATEST(
          (SELECT max(_id) FROM operator.operators), (SELECT last_value FROM operator.operators__id_seq)
        )) AS operators,
        setval('territory.territory_group__id_seq', GREATEST(
          (SELECT max(_id) FROM territory.territory_group), (SELECT last_value FROM territory.territory_group__id_seq)
        )) AS territories
    `);
  }

  protected async seedCompany(siret: string, legal_name: string, naf: string): Promise<void> {
    await this.connection.query(sql`
      INSERT INTO company.companies
        (siret, siren, nic, legal_name, company_naf_code, establishment_naf_code, headquarter)
      VALUES (${siret}, ${siret.slice(0, 9)}, ${siret.slice(9)}, ${legal_name}, ${naf}, ${naf}, true)
      ON CONFLICT (siret) DO NOTHING
    `);
  }

  protected async seedOperator(operator: SyntheticProfile["operators"][number]): Promise<number> {
    // pas de contrainte d'unicité sur le SIRET : recherche puis création dans la même requête
    const rows = await this.connection.query<{ _id: number }>(sql`
      WITH existing AS (
        SELECT _id FROM operator.operators WHERE siret = ${operator.siret} AND deleted_at IS NULL ORDER BY _id LIMIT 1
      ), created AS (
        INSERT INTO operator.operators (name, legal_name, siret, company, address, bank, contacts, uuid)
        SELECT ${operator.name}, ${operator.legal_name}, ${operator.siret}, '{}', '{}', '{}', '{}', ${operator.uuid}::uuid
        WHERE NOT EXISTS (SELECT 1 FROM existing)
        RETURNING _id
      )
      SELECT _id FROM existing UNION ALL SELECT _id FROM created
    `);
    return rows[0]._id;
  }

  protected async seedTerritory(territory: SyntheticTerritory): Promise<number> {
    const existing = await this.perimeters.findTerritoryByName(territory.name);
    if (existing) return existing._id;

    const resolved = await this.perimeters.resolve(parseTerritoryCodes(territory.codes));
    if (resolved.unknown.length || !resolved.arr.length) {
      throw new Error(
        `Périmètres absents de geo.perimeters pour '${territory.name}' : ${resolved.unknown.join(" ")} ` +
          `(millésimes à charger avec geo-import)`,
      );
    }
    return await this.perimeters.createTerritory(territory.name, territory.siret, {
      arr: resolved.arr,
      valid_from: PERIMETER_FROM,
      valid_to: null,
    });
  }

  protected async findCommunes(territory: SyntheticTerritory, territory_id: number): Promise<Commune[]> {
    const communes = await this.connection.query<Commune>(sql`
      SELECT gp.arr, ST_X(gp.centroid)::float8 AS lon, ST_Y(gp.centroid)::float8 AS lat, gp.pop
      FROM geo.perimeters gp
      WHERE gp.year = geo.get_latest_millesime()
        AND gp.arr IN (SELECT arr FROM territory.get_arr(${territory_id}::int, ${this.referenceDate}::timestamptz))
      ORDER BY gp.arr
    `);
    if (!communes.length) {
      throw new Error(`Aucune commune du dernier millésime dans le périmètre de '${territory.name}'`);
    }
    return communes;
  }

  protected async seedUser(
    user: SyntheticUser,
    operators: Record<string, number>,
    territories: Record<string, number>,
  ): Promise<void> {
    const rows = await this.connection.query<{ _id: number }>(sql`
      INSERT INTO auth.users (email, firstname, lastname, status, role, login_siren)
      VALUES (
        ${user.email}, ${user.firstname}, ${user.lastname}, 'active', ${user.role}, ${user.login_siren ?? null}
      )
      ON CONFLICT (email) DO UPDATE SET
        firstname = excluded.firstname,
        lastname = excluded.lastname,
        status = excluded.status,
        role = excluded.role,
        login_siren = excluded.login_siren
      RETURNING _id
    `);

    const operator_id = user.operator ? operators[user.operator] : null;
    const territory_id = user.territory ? territories[user.territory] : null;
    if (operator_id === null && territory_id === null) return;

    await this.connection.query(sql`
      INSERT INTO auth.user_scopes (user_id, operator_id, territory_id, is_default)
      SELECT ${rows[0]._id}::int, ${operator_id}::int, ${territory_id}::int, true
      WHERE NOT EXISTS (SELECT 1 FROM auth.user_scopes WHERE user_id = ${rows[0]._id}::int)
    `);
  }

  protected async seedPolicy(
    territory_id: number,
    campaign: SyntheticCampaign,
  ): Promise<{ _id: number; status: string }> {
    // `campaign:apply` ne clôt que les campagnes actives dont la fin est passée à l'horloge réelle
    const status = new Date(campaign.end_date) <= this.referenceDate ? "finished" : "active";
    const rows = await this.connection.query<{ _id: number; status: string }>(sql`
      WITH existing AS (
        SELECT _id, status::varchar FROM policy.policies
        WHERE territory_id = ${territory_id}::int AND handler = ${campaign.handler} AND deleted_at IS NULL
        ORDER BY _id LIMIT 1
      ), created AS (
        INSERT INTO policy.policies (territory_id, start_date, end_date, name, unit, status, handler, max_amount)
        SELECT
          ${territory_id}::int,
          ${campaign.start_date}::timestamptz,
          ${campaign.end_date}::timestamptz,
          ${campaign.name},
          'euro',
          ${status}::policy.policy_status_enum,
          ${campaign.handler},
          ${campaign.max_amount}::bigint
        WHERE NOT EXISTS (SELECT 1 FROM existing)
        RETURNING _id, status::varchar
      )
      SELECT _id, status FROM existing UNION ALL SELECT _id, status FROM created
    `);
    return rows[0];
  }

  // Une seule requête par lot : un trajet n'existe jamais sans son statut, sa géographie et sa requête.
  protected async insertCarpools(batch: unknown[]): Promise<number> {
    const rows = await this.connection.query<{ inserted: number }>(sql`
      WITH data AS (
        SELECT * FROM jsonb_to_recordset(${JSON.stringify(batch)}::jsonb) AS t(
          operator_id int, operator_journey_id varchar, operator_trip_id varchar, operator_class char(1),
          uuid uuid, start_datetime timestamptz, end_datetime timestamptz, created_at timestamptz,
          start_lon float8, start_lat float8, start_geo_code varchar,
          end_lon float8, end_lat float8, end_geo_code varchar,
          distance int, driver jsonb, passenger jsonb,
          passenger_seats smallint, passenger_contribution int, driver_revenue int,
          acquisition_status varchar, fraud_status varchar, anomaly_status varchar
        )
      ), inserted AS (
        INSERT INTO carpool_v2.carpools (
          created_at, updated_at, operator_id, operator_journey_id, operator_trip_id, operator_class, uuid,
          start_datetime, start_position, end_datetime, end_position, distance,
          driver_identity_key, driver_operator_user_id, driver_phone, driver_phone_trunc, driver_revenue,
          passenger_identity_key, passenger_operator_user_id, passenger_phone, passenger_phone_trunc,
          passenger_over_18, passenger_seats, passenger_contribution, passenger_payments
        )
        SELECT
          created_at, created_at, operator_id, operator_journey_id, operator_trip_id, operator_class, uuid,
          start_datetime, ST_SetSRID(ST_Point(start_lon, start_lat), 4326),
          end_datetime, ST_SetSRID(ST_Point(end_lon, end_lat), 4326), distance,
          driver->>'identity_key', driver->>'operator_user_id', driver->>'phone', driver->>'phone_trunc',
          driver_revenue,
          passenger->>'identity_key', passenger->>'operator_user_id', passenger->>'phone',
          passenger->>'phone_trunc',
          true, passenger_seats, passenger_contribution, '[]'::jsonb
        FROM data
        ON CONFLICT (operator_id, operator_journey_id) DO NOTHING
        RETURNING _id, operator_id, operator_journey_id
      ), fresh AS (
        SELECT i._id, d.* FROM inserted i JOIN data d USING (operator_id, operator_journey_id)
      ), status AS (
        INSERT INTO carpool_v2.status (carpool_id, acquisition_status, fraud_status, anomaly_status)
        SELECT
          _id,
          acquisition_status::carpool_v2.carpool_acquisition_status_enum,
          fraud_status::carpool_v2.carpool_fraud_status_enum,
          anomaly_status::carpool_v2.carpool_anomaly_status_enum
        FROM fresh
      ), geo AS (
        INSERT INTO carpool_v2.geo (carpool_id, start_geo_code, end_geo_code, errors)
        SELECT _id, start_geo_code, end_geo_code, '[]'::jsonb FROM fresh
      ), requests AS (
        INSERT INTO carpool_v2.requests (carpool_id, operator_id, operator_journey_id, api_version, created_at)
        SELECT _id, operator_id, operator_journey_id, '3', created_at FROM fresh
      )
      SELECT count(*)::int AS inserted FROM inserted
    `);
    return rows[0].inserted;
  }
}
