import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { basePath, join } from "@/lib/path/index.ts";
import sql, { join as sqlJoin, raw } from "@/lib/pg/sql.ts";
import { Options as ParseOptions, parse } from "dep:csv-parse";
import { add as addDate } from "dep:date-fns";
import { createReadStream } from "dep:fs";
import { URL } from "dep:url";
import { FlashDBData } from "./FlashDBData.ts";
import { migrateSQL } from "./migrations.ts";
import { Carpool, carpoolsV2 } from "./seeds/carpools.ts";
import { companies, Company } from "./seeds/companies.ts";
import { Operator, operators } from "./seeds/operators.ts";
import { CreateTerritoryGroupInterface, territory_groups, TerritorySelectorsInterface } from "./seeds/territories.ts";
import { User, users } from "./seeds/users.ts";

export type DenoMigratorMigrateParams = {
  path: string;
  skip: boolean;
  flash: boolean;
  verbose: boolean;
  cache: {
    url: string;
    sha: string;
  };
};

export class DenoMigrator {
  // base connection to handle the creation of a specific database
  public baseConn: DenoPostgresConnection;
  public rootConnectionString: string;

  // specific connection to a test database
  public testConn: DenoPostgresConnection;
  public currentConnectionString: string;

  public readonly dbName: string;
  public readonly hasTmpDb: boolean;
  public verbose = false;

  constructor(dbUrlString: string, createTmpDb = true) {
    const dbUrl = new URL(dbUrlString);
    this.hasTmpDb = createTmpDb;
    this.dbName = createTmpDb
      ? `test_${Date.now().valueOf()}_${(Math.random() + 1).toString(36).substring(7)}`
      : dbUrl.pathname.replace("/", "");

    const currentConnection = new URL(dbUrlString);
    this.rootConnectionString = currentConnection.toString();
    if (createTmpDb) {
      currentConnection.pathname = `/${this.dbName}`;
    }

    this.currentConnectionString = currentConnection.toString();
    this.testConn = new DenoPostgresConnection(this.currentConnectionString);
    this.baseConn = createTmpDb ? new DenoPostgresConnection(this.rootConnectionString) : this.testConn;
  }

  async up() {
    this.testConn && await this.testConn.up();
    this.baseConn && await this.baseConn.up();
  }

  async down() {
    this.testConn && await this.testConn.down();
    this.baseConn && await this.baseConn.down();
  }

  async drop() {
    if (this.hasTmpDb) {
      // connection to the test database must be closed before dropping it
      await this.testConn.down();

      // drop the test database using the base connection
      await this.baseConn.raw(sql`DROP DATABASE ${raw(this.dbName)}`);
    }
  }

  async create() {
    if (this.hasTmpDb) {
      logger.debug(`[migrator] creating database ${this.dbName}`);
      await this.baseConn.up();
      await this.baseConn.raw(sql`CREATE DATABASE ${raw(this.dbName)}`);
    }
  }

  async migrate(params: Partial<DenoMigratorMigrateParams> = {}) {
    const { path, skip, flash, verbose, cache } = {
      path: join(basePath(), "db", "migrations"),
      skip: false,
      flash: true,
      verbose: true,
      ...params,
    };

    if (skip) {
      logger.warn("[migrator] skipping migrations");
      return;
    }

    verbose && logger.info("[migrator] Migrate database schema");
    await migrateSQL(this.currentConnectionString, path, verbose);

    if (flash) {
      if (!cache) {
        throw new Error("[migrator] cache configuration is required to flash data");
      }

      verbose && logger.info("[migrator] flash data");
      const flash = new FlashDBData({
        connectionString: this.currentConnectionString,
        cache,
        verbose,
      });

      await flash.missing() && await flash.exec();
    }
  }

  async seed() {
    logger.debug("[migrator] seeding...");

    await this.up();
    await this.testConn.query(sql`SET session_replication_role = 'replica'`);

    for (const company of companies) {
      this.verbose && logger.debug(`Seeding company ${company.legal_name}`);
      await this.seedCompany(company);
    }

    this.verbose && logger.debug(`Seeding geo`);
    await this.seedTerritory();

    for (const operator of operators) {
      this.verbose && logger.debug(`Seeding operator ${operator.name}`);
      await this.seedOperator(operator);
    }

    for (const user of users) {
      this.verbose && logger.debug(`Seeding user ${user.email}`);
      await this.seedUser(user);
    }

    for (const territory_group of territory_groups) {
      this.verbose && logger.debug(`Seeding territory group ${territory_group.name}`);
      await this.seedTerritoryGroup(territory_group);
    }

    for (const carpool of carpoolsV2) {
      this.verbose && logger.debug(`Seeding carpool ${carpool[0].acquisition_id}`);
      await this.seedCarpoolV2(carpool);
    }

    await this.testConn.query(sql`SET session_replication_role = 'origin'`);
    logger.debug("[migrator] seeding... done");
  }

  protected async seedFromCSV(filename: string, tablename: string, _csvOptions: ParseOptions = {}) {
    const cursor = (async function* <P>(f: string, options: ParseOptions = {}) {
      const filepath = join(new URL(".", import.meta.url).pathname, f);
      const parser = createReadStream(filepath).pipe(
        parse({ cast: (v: unknown) => (v === "" ? null : v), ...options }),
      );
      for await (const record of parser) {
        yield record;
      }
    })(filename);

    for await (const data of cursor) {
      await this.testConn.query(sql`
        INSERT INTO ${raw(tablename)}
        VALUES (${sqlJoin(data, ", ")})
        ON CONFLICT DO NOTHING
      `);
    }
  }

  async seedCarpoolV2([driverCarpool, passengerCarpool]: [Carpool, Carpool]) {
    const carpoolResults = await this.testConn.query<Carpool & { _id: number }>(sql`
      INSERT INTO carpool_v2.carpools (
        operator_id,
        operator_journey_id,
        operator_trip_id,
        operator_class,
        start_datetime,
        start_position,
        end_datetime,
        end_position,
        distance,
        licence_plate,
        driver_identity_key,
        driver_operator_user_id,
        driver_phone,
        driver_phone_trunc,
        driver_travelpass_name,
        driver_travelpass_user_id,
        driver_revenue,
        passenger_identity_key,
        passenger_operator_user_id,
        passenger_phone,
        passenger_phone_trunc,
        passenger_travelpass_name,
        passenger_travelpass_user_id,
        passenger_over_18,
        passenger_seats,
        passenger_contribution,
        passenger_payments,
        legacy_id
      ) VALUES(
        ${driverCarpool.operator_id},
        ${driverCarpool.operator_journey_id},
        ${driverCarpool.operator_trip_id},
        ${driverCarpool.operator_class},
        ${driverCarpool.datetime},
        ST_SetSRID(ST_Point(${driverCarpool.start_position.lon}, ${driverCarpool.start_position.lat}), 4326),
        '${raw(addDate(driverCarpool.datetime, { seconds: driverCarpool.duration }).toISOString())}',
        ST_SetSRID(ST_Point(${driverCarpool.end_position.lon}, ${driverCarpool.end_position.lat}), 4326),
        ${driverCarpool.distance},
        ${driverCarpool.licence_plate},
        ${driverCarpool.identity_key},
        ${driverCarpool.identity_operator_user_id},
        ${driverCarpool.identity_phone},
        ${driverCarpool.identity_phone_trunc},
        ${driverCarpool.identity_travelpass_name},
        ${driverCarpool.identity_travelpass_user_id},
        ${driverCarpool.cost},
        ${passengerCarpool.identity_key},
        ${passengerCarpool.identity_operator_user_id},
        ${passengerCarpool.identity_phone},
        ${passengerCarpool.identity_phone_trunc},
        ${passengerCarpool.identity_travelpass_name},
        ${passengerCarpool.identity_travelpass_user_id},
        ${passengerCarpool.identity_over_18},
        ${passengerCarpool.seats},
        ${passengerCarpool.cost},
        '${raw(JSON.stringify(passengerCarpool.payments))}',
        ${driverCarpool.acquisition_id}
      )
      ON CONFLICT (operator_id, operator_journey_id) DO NOTHING
      RETURNING _id, uuid, created_at, updated_at
    `);

    await this.testConn.query(sql`
      INSERT INTO carpool_v2.requests (
        carpool_id, operator_id, operator_journey_id, payload, api_version, cancel_code, cancel_message
      ) VALUES (
        ${carpoolResults[0]._id},
        ${driverCarpool.operator_id},
        ${driverCarpool.operator_journey_id},
        NULL,
        '3',
        NULL,
        NULL
      )
      RETURNING _id, created_at
    `);

    await this.testConn.query(sql`
      INSERT INTO carpool_v2.geo (
        carpool_id, start_geo_code, end_geo_code, errors
      ) VALUES (
        ${carpoolResults[0]._id},
        ${driverCarpool.start_geo_code},
        ${driverCarpool.end_geo_code},
        '[]'
      )
      ON CONFLICT (carpool_id)
      DO UPDATE
      SET
        start_geo_code = excluded.start_geo_code,
        end_geo_code = excluded.end_geo_code,
        errors = excluded.errors::jsonb
    `);

    await this.testConn.query(sql`
      INSERT INTO carpool_v2.status (
        carpool_id,
        acquisition_status
      ) VALUES (
        ${carpoolResults[0]._id},
        'processed'
      )
    `);
  }

  async seedCompany(company: Company) {
    await this.testConn.query(sql`
      INSERT INTO company.companies
        (_id, siret, siren, nic, legal_name, company_naf_code, establishment_naf_code, headquarter)
      VALUES (
        ${company._id}::int,
        ${company.siret}::varchar,
        ${company.siren}::varchar,
        ${company.nic}::varchar,
        ${company.legal_name}::varchar,
        ${company.company_naf_code}::varchar,
        ${company.establishment_naf_code}::varchar,
        ${company.headquarter}::boolean
      )
      ON CONFLICT DO NOTHING
    `);
  }

  async seedOperator(operator: Operator) {
    await this.testConn.query(sql`
      INSERT INTO operator.operators
        (_id, name, legal_name, siret, company, address, bank, contacts, uuid)
      VALUES (
        ${operator._id}::int,
        ${operator.name}::varchar,
        ${operator.legal_name}::varchar,
        ${operator.siret}::varchar,
        ${operator.company}::json,
        ${operator.address}::json,
        ${operator.bank}::json,
        ${operator.contacts}::json,
        ${operator.uuid}::uuid
      )
      ON CONFLICT DO NOTHING
    `);
  }

  async seedUser(user: User) {
    // DO UPDATE no-op : RETURNING doit renvoyer l'id même quand l'utilisateur existe déjà (re-seed).
    const rows = await this.testConn.query<{ _id: number }>(sql`
      INSERT INTO auth.users
        (email, firstname, lastname, password, status, role)
      VALUES (
        ${user.email}::varchar,
        ${user.firstname}::varchar,
        ${user.lastname}::varchar,
        ${user.password}::varchar,
        ${user.status}::auth.user_status_enum,
        ${user.role}::varchar
      )
      ON CONFLICT (email) DO UPDATE SET email = excluded.email
      RETURNING _id
    `);

    const operatorId = user.operator?._id ?? null;
    const territoryId = user.territory?._id ?? null;
    if (operatorId === null && territoryId === null) return;

    const userId = rows[0]?._id;
    if (!userId) {
      throw new Error(`[migrator] identifiant introuvable pour ${user.email} : périmètre non seedé`);
    }

    await this.testConn.query(sql`
      INSERT INTO auth.user_scopes (user_id, operator_id, territory_id, is_default)
      VALUES (${userId}::int, ${operatorId}::int, ${territoryId}::int, true)
      ON CONFLICT DO NOTHING
    `);

    for (const extra of user.extraTerritories ?? []) {
      await this.testConn.query(sql`
        INSERT INTO auth.user_scopes (user_id, territory_id, is_default)
        VALUES (${userId}::int, ${extra._id}::int, false)
        ON CONFLICT DO NOTHING
      `);
    }
  }

  async seedTerritoryGroup(territory_group: CreateTerritoryGroupInterface) {
    const results = await this.testConn.query<{
      _id: number;
      name: string;
      shortname: string;
      contacts: string;
      address: string;
      company_id: number;
    }>(sql`
      INSERT INTO territory.territory_group (
        _id,
        name,
        shortname,
        contacts,
        address,
        company_id
      ) VALUES (
        ${territory_group._id}::int,
        ${territory_group.name}::varchar,
        '',  
        ${territory_group.contacts}::json,
        ${territory_group.address}::json,
        ${territory_group.company_id}::int
      )
      ON CONFLICT DO NOTHING
      RETURNING *
    `);

    this.syncSelector(results[0]._id, territory_group.selector);
  }

  async syncSelector(groupId: number, selector: TerritorySelectorsInterface): Promise<void> {
    const values: [number[], string[], string[]] = [[], [], []];
    for (const [type, list] of Object.entries(selector)) {
      for (const value of list ?? []) { // optional chain + nullish coalesce
        values[0].push(groupId);
        values[1].push(type);
        values[2].push(String(value));
      }
    }

    await this.testConn.query(sql`
      DELETE FROM territory.territory_group_selector
      WHERE territory_group_id = ${groupId}
    `);

    await this.testConn.query(sql`
      INSERT INTO territory.territory_group_selector (
        territory_group_id,
        selector_type,
        selector_value
      ) 
      SELECT * FROM UNNEST(${values[0]}::int[], ${values[1]}::varchar[], ${values[2]}::varchar[])
    `);
  }

  async seedTerritory() {
    await this.seedFromCSV("./seeds/geo.csv", "geo.perimeters");
  }
}
