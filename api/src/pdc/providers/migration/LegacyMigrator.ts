import { LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { basePath, join } from "@/lib/path/index.ts";
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

export type MigratorMigrateParams = {
  path: string;
  skip: boolean;
  flash: boolean;
  verbose: boolean;
  cache: {
    url: string;
    sha: string;
  };
};

/**
 * @deprecated replaced by DenoMigrator
 */
export class LegacyMigrator {
  // base connection to handle the creation of a specific database
  public baseConn: LegacyPostgresConnection;
  public rootConnectionString: string;

  // specific connection to a test database
  public testConn: LegacyPostgresConnection;
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
    this.testConn = new LegacyPostgresConnection({
      connectionString: this.currentConnectionString,
    });

    this.baseConn = createTmpDb
      ? new LegacyPostgresConnection({ connectionString: this.rootConnectionString })
      : this.testConn;
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
      await this.baseConn.getClient().query(
        `DROP DATABASE ${this.dbName}`,
      );
    }
  }

  async create() {
    if (this.hasTmpDb) {
      logger.debug(`[migrator] creating database ${this.dbName}`);
      await this.baseConn.getClient().query(`CREATE DATABASE ${this.dbName}`);
    }
  }

  async migrate(params: Partial<MigratorMigrateParams> = {}) {
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
    await this.testConn.getClient().query(`SET session_replication_role = 'replica'`);

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

    await this.testConn.getClient().query(`SET session_replication_role = 'origin'`);
    logger.debug("[migrator] seeding... done");
  }

  protected async *dataFromCsv<P>(filename: string, options: ParseOptions = {}): AsyncIterator<P> {
    const filepath = join(new URL(".", import.meta.url).pathname, filename);
    const parser = createReadStream(filepath).pipe(
      parse({
        cast: (v: any) => (v === "" ? null : v),
        ...options,
      }),
    );

    for await (const record of parser) {
      yield record;
    }
  }

  protected async seedFromCsv(
    filename: string,
    tablename: string,
    _csvOptions: ParseOptions = {},
  ) {
    const cursor = this.dataFromCsv(filename);
    let done = false;
    do {
      const data = await cursor.next();
      if (data.value && Array.isArray(data.value)) {
        await this.testConn.getClient().query({
          text: `INSERT INTO ${tablename} VALUES (${data.value.map((_, i) => `$${i + 1}`).join(", ")})`,
          values: data.value,
        });
      }
      done = !!data.done;
    } while (!done);
  }

  async seedCarpoolV2([driverCarpool, passengerCarpool]: [Carpool, Carpool]) {
    const carpoolResult = await this.testConn.getClient().query({
      text: `INSERT INTO carpool_v2.carpools (
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
        $1,
        $2,
        $3,
        $4,
        $5,
        ST_SetSRID(ST_Point($6, $7), 4326),
        $8,
        ST_SetSRID(ST_Point($9, $10), 4326),
        $11,
        $12,
        $13,
        $14,
        $15,
        $16,
        $17,
        $18,
        $19,
        $20,
        $21,
        $22,
        $23,
        $24,
        $25,
        $26,
        $27,
        $28,
        $29,
        $30
      )
      ON CONFLICT (operator_id, operator_journey_id) DO NOTHING
      RETURNING _id, uuid, created_at, updated_at
    `,
      values: [
        driverCarpool.operator_id,
        driverCarpool.operator_journey_id,
        driverCarpool.operator_trip_id,
        driverCarpool.operator_class,
        driverCarpool.datetime,
        driverCarpool.start_position.lon,
        driverCarpool.start_position.lat,
        addDate(driverCarpool.datetime, { seconds: driverCarpool.duration }),
        driverCarpool.end_position.lon,
        driverCarpool.end_position.lat,
        driverCarpool.distance,
        driverCarpool.licence_plate,
        driverCarpool.identity_key,
        driverCarpool.identity_operator_user_id,
        driverCarpool.identity_phone,
        driverCarpool.identity_phone_trunc,
        driverCarpool.identity_travelpass_name,
        driverCarpool.identity_travelpass_user_id,
        driverCarpool.cost,
        passengerCarpool.identity_key,
        passengerCarpool.identity_operator_user_id,
        passengerCarpool.identity_phone,
        passengerCarpool.identity_phone_trunc,
        passengerCarpool.identity_travelpass_name,
        passengerCarpool.identity_travelpass_user_id,
        passengerCarpool.identity_over_18,
        passengerCarpool.seats,
        passengerCarpool.cost,
        JSON.stringify(passengerCarpool.payments),
        driverCarpool.acquisition_id,
      ],
    });

    await this.testConn.getClient().query({
      text: `
        INSERT INTO carpool_v2.requests (
          carpool_id, operator_id, operator_journey_id, payload, api_version, cancel_code, cancel_message
        ) VALUES (
          $1,
          $2,
          $3,
          $4,
          $5,
          $6,
          $7
        )
        RETURNING _id, created_at
      `,
      values: [
        carpoolResult.rows[0]._id,
        driverCarpool.operator_id,
        driverCarpool.operator_journey_id,
        null,
        "3",
        null,
        null,
      ],
    });

    await this.testConn.getClient().query({
      text: `
      INSERT INTO carpool_v2.geo (
        carpool_id, start_geo_code, end_geo_code, errors
      ) VALUES (
        $1,
        $2,
        $3,
        $4
      )
      ON CONFLICT (carpool_id)
      DO UPDATE
      SET
        start_geo_code = excluded.start_geo_code,
        end_geo_code = excluded.end_geo_code,
        errors = excluded.errors::jsonb
      `,
      values: [
        carpoolResult.rows[0]._id,
        driverCarpool.start_geo_code,
        driverCarpool.end_geo_code,
        JSON.stringify([]),
      ],
    });

    await this.testConn.getClient().query({
      text: `
      INSERT INTO carpool_v2.status (
        carpool_id, acquisition_status
      ) VALUES (
        $1,
        $2
      )
      `,
      values: [carpoolResult.rows[0]._id, "processed"],
    });
  }

  async seedCompany(company: Company) {
    await this.testConn.getClient().query({
      text: `
        INSERT INTO company.companies
          (_id, siret, siren, nic, legal_name, company_naf_code, establishment_naf_code, headquarter)
        VALUES (
          $1::int,
          $2::varchar,
          $3::varchar,
          $4::varchar,
          $5::varchar,
          $6::varchar,
          $7::varchar,
          $8::boolean
        )
        ON CONFLICT DO NOTHING
      `,
      values: [
        company._id,
        company.siret,
        company.siren,
        company.nic,
        company.legal_name,
        company.company_naf_code,
        company.establishment_naf_code,
        company.headquarter,
      ],
    });
  }

  async seedOperator(operator: Operator) {
    await this.testConn.getClient().query({
      text: `
        INSERT INTO operator.operators
          (_id, name, legal_name, siret, company, address, bank, contacts, uuid)
        VALUES (
          $1::int,
          $2::varchar,
          $3::varchar,
          $4::varchar,
          $5::json,
          $6::json,
          $7::json,
          $8::json,
          $9::uuid
        )
        ON CONFLICT DO NOTHING
      `,
      values: [
        operator._id,
        operator.name,
        operator.legal_name,
        operator.siret,
        operator.company,
        operator.address,
        operator.bank,
        operator.contacts,
        operator.uuid,
      ],
    });
  }

  async seedUser(user: User) {
    // DO UPDATE no-op : RETURNING doit renvoyer l'id même quand l'utilisateur existe déjà (re-seed).
    const inserted = await this.testConn.getClient().query({
      text: `
        INSERT INTO auth.users
          (email, firstname, lastname, password, status, role)
        VALUES (
          $1::varchar,
          $2::varchar,
          $3::varchar,
          $4::varchar,
          $5::auth.user_status_enum,
          $6::varchar
        )
        ON CONFLICT (email) DO UPDATE SET email = excluded.email
        RETURNING _id
      `,
      values: [
        user.email,
        user.firstname,
        user.lastname,
        user.password, // TODO: use cryptoprovider tcrypt password
        user.status,
        user.role,
      ],
    });

    const operatorId = user.operator?._id ?? null;
    const territoryId = user.territory?._id ?? null;
    if (operatorId === null && territoryId === null) return;

    const userId = inserted.rows[0]?._id;
    if (!userId) {
      throw new Error(`[migrator] identifiant introuvable pour ${user.email} : périmètre non seedé`);
    }

    await this.testConn.getClient().query({
      text: `
        INSERT INTO auth.user_scopes (user_id, operator_id, territory_id, is_default)
        VALUES ($1::int, $2::int, $3::int, true)
        ON CONFLICT DO NOTHING
      `,
      values: [userId, operatorId, territoryId],
    });

    for (const extra of user.extraTerritories ?? []) {
      await this.testConn.getClient().query({
        text: `
          INSERT INTO auth.user_scopes (user_id, territory_id, is_default)
          VALUES ($1::int, $2::int, false)
          ON CONFLICT DO NOTHING
        `,
        values: [userId, extra._id],
      });
    }
  }

  async seedTerritoryGroup(territory_group: CreateTerritoryGroupInterface) {
    const fields = [
      "_id",
      "name",
      "shortname",
      "contacts",
      "address",
      "company_id",
    ];

    const values: any[] = [
      territory_group._id,
      territory_group.name,
      "",
      territory_group.contacts,
      territory_group.address,
      territory_group.company_id,
    ];
    const query = {
      text: `
        INSERT INTO territory.territory_group (${fields.join(",")})
        VALUES (${fields.map((data, ind) => `$${ind + 1}`).join(",")})
        RETURNING *
      `,
      values,
    };
    const resultData = await this.testConn.getClient().query(query);
    this.syncSelector(resultData.rows[0]._id, territory_group.selector);
  }

  async syncSelector(
    groupId: number,
    selector: TerritorySelectorsInterface,
  ): Promise<void> {
    const values: [number[], string[], string[]] = [[], [], []];
    for (const [type, list] of Object.entries(selector)) {
      for (const value of list ?? []) { // optional chain + nullish coalesce
        values[0].push(groupId);
        values[1].push(type);
        values[2].push(String(value));
      }
    }

    await this.testConn.getClient().query({
      text: `
        DELETE FROM territory.territory_group_selector
        WHERE territory_group_id = $1
      `,
      values: [groupId],
    });

    await this.testConn.getClient().query({
      text: `
        INSERT INTO territory.territory_group_selector (
          territory_group_id,
          selector_type,
          selector_value
        ) 
        SELECT * FROM UNNEST($1::int[], $2::varchar[], $3::varchar[])`,
      values,
    });
  }

  async seedTerritory() {
    await this.seedFromCsv("./seeds/geo.csv", "geo.perimeters");
  }
}
