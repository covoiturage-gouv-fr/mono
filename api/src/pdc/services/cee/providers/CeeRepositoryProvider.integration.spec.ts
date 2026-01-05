import {
  afterAll,
  assert,
  assertEquals,
  assertNotEquals,
  assertObjectMatch,
  assertRejects,
  beforeAll,
  describe,
  it,
} from "@/dev_deps.ts";
import { ConflictException } from "@/ilos/common/index.ts";
import { CarpoolAcquisitionStatusEnum, CarpoolFraudStatusEnum } from "@/pdc/providers/carpool/interfaces/index.ts";
import { LegacyDbContext, makeLegacyDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { config } from "../config/index.ts";
import {
  CeeApplicationErrorEnum,
  CeeJourneyTypeEnum,
  LongCeeApplication,
  SearchCeeApplication,
  SearchJourney,
  ShortCeeApplication,
  ValidJourneyConstraint,
} from "../interfaces/index.ts";
import { CeeRepositoryProvider } from "./CeeRepositoryProvider.ts";

describe("CeeRepositoryProvider", () => {
  let db: LegacyDbContext;
  let repository: CeeRepositoryProvider;
  let applicationUuid: string;

  const { before, after } = makeLegacyDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new CeeRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("Should find a valid journey", async () => {
    const search: SearchJourney = {
      operator_id: 1,
      operator_journey_id: "operator_journey_id-1",
    };

    const constraint: ValidJourneyConstraint = {
      ...config.rules.validJourneyConstraint,
      start_date: new Date("2022-01-01"),
    };

    const result = await repository.searchForValidJourney(
      search,
      constraint,
    );

    assertNotEquals(result, undefined);
    assertEquals(result.phone_trunc, "+336000000");
  });

  it("Should throw error is no valid journey found", async () => {
    const search: SearchJourney = {
      operator_id: 1,
      operator_journey_id: "operator_journey_id-1",
    };

    const constraint: ValidJourneyConstraint = {
      ...config.rules.validJourneyConstraint,
      start_date: new Date("2022-01-01"),
      max_distance: 0,
    };

    const error = await assertRejects(
      async () => repository.searchForValidJourney(search, constraint),
    );

    assertNotEquals(error, undefined);
  });

  it("Should find a carpool and checks its registered state", async () => {
    // Search and make sure the journey is not registered
    const search: SearchJourney = {
      operator_id: 1,
      operator_journey_id: "operator_journey_id-3",
    };

    const constraint: ValidJourneyConstraint = {
      ...config.rules.validJourneyConstraint,
      start_date: new Date("2024-03-16"),
    };

    const resultBefore = await repository.searchForValidJourney(
      search,
      constraint,
    );

    assertNotEquals(resultBefore, undefined);
    assertEquals(resultBefore.phone_trunc, "+336000000");
    assertEquals(resultBefore.already_registered, false);

    // Register a journey
    const application: ShortCeeApplication = {
      operator_id: 1,
      operator_journey_id: "operator_journey_id-3",
      last_name_trunc: "ZZZ",
      phone_trunc: "+336000000",
      datetime: new Date("2024-03-16"),
      application_timestamp: new Date("2024-03-16"),
      driving_license: "driving_license_100",
    };

    await repository.registerShortApplication(
      application,
      config.rules.applicationCooldownConstraint,
    );

    // Search for the journey and check if it is already registered
    const resultAfter = await repository.searchForValidJourney(
      search,
      constraint,
    );

    assertNotEquals(resultAfter, undefined);
    assertEquals(resultAfter.phone_trunc, "+336000000");
    assertEquals(resultAfter.already_registered, true);
  });

  it("Should create short application", async () => {
    const application: ShortCeeApplication = {
      operator_id: 1,
      operator_journey_id: "operator_journey_id-1",
      last_name_trunc: "AAA",
      phone_trunc: "+3360000000000",
      datetime: new Date("2022-11-01"),
      application_timestamp: new Date("2022-11-01"),
      driving_license: "driving_license_1",
    };

    await repository.registerShortApplication(
      application,
      config.rules.applicationCooldownConstraint,
    );

    const applicationResults = await db.connection.getClient().query<any>({
      text: `
        SELECT ${Object.keys(application).join(",")}, journey_type
        FROM ${repository.ceeApplicationsTable}
        WHERE operator_id = $1 and last_name_trunc = $2
      `,
      values: [1, "AAA"],
    });

    assertEquals(applicationResults.rowCount, 1);
    assertObjectMatch(applicationResults.rows[0], {
      journey_type: "short",
      ...application,
    });
  });

  it("Should create long application", async () => {
    const application: LongCeeApplication = {
      operator_id: 1,
      last_name_trunc: "BBB",
      phone_trunc: "+3360000000000",
      datetime: new Date("2022-11-01"),
      application_timestamp: new Date("2022-11-01"),
      driving_license: "driving_license_1",
      identity_key: "test",
    };

    const r = await repository.registerLongApplication(
      application,
      config.rules.applicationCooldownConstraint,
    );

    applicationUuid = r.uuid;

    const applicationResults = await db.connection.getClient().query<any>({
      text: `
        SELECT ${Object.keys(application).join(",")}, journey_type
        FROM ${repository.ceeApplicationsTable}
        WHERE operator_id = $1 AND last_name_trunc = $2
      `,
      values: [1, "BBB"],
    });

    assertEquals(applicationResults.rowCount, 1);
    assertEquals(applicationResults.rows[0]?.journey_type, "long");
    assert(!!applicationResults.rows[0]?.identity_key);
  });

  it("Search should be equal to the new registration", async () => {
    const application: ShortCeeApplication = {
      operator_id: 1,
      operator_journey_id: "operator_journey_id-2",
      last_name_trunc: "BBA",
      phone_trunc: "+3361100000000",
      datetime: new Date("2022-11-01"),
      application_timestamp: new Date("2022-11-01"),
      driving_license: "driving_license_3",
      identity_key: "search_1",
    };

    const createResult = await repository.registerShortApplication(
      application,
      config.rules.applicationCooldownConstraint,
    );

    const searchResult = await repository.searchForShortApplication(
      {
        datetime: application.datetime,
        identity_key: application.identity_key,
      } as any,
      config.rules.applicationCooldownConstraint,
    );

    const {
      journey_id,
      operator_journey_id,
      acquisition_status,
      fraud_status,
      ...match
    } = searchResult || {};

    assertObjectMatch(createResult, match);
    assertEquals(operator_journey_id, "operator_journey_id-2");
    assertEquals(acquisition_status, CarpoolAcquisitionStatusEnum.Processed);
    assertEquals(fraud_status, CarpoolFraudStatusEnum.Pending);
  });

  it("Should raise error if conflict short application", async () => {
    const application: ShortCeeApplication = {
      operator_id: 1,
      operator_journey_id: "operator_journey_id-1",
      last_name_trunc: "AAA",
      phone_trunc: "+3360000000000",
      datetime: new Date("2022-11-02"),
      application_timestamp: new Date("2022-11-02"),
      driving_license: "driving_license_1",
    };

    await assertRejects(
      async () =>
        await repository.registerShortApplication(
          application,
          config.rules.applicationCooldownConstraint,
        ),
    );
  });

  it("Should raise error if conflict id key long application", async () => {
    // This is a duplicate of a previously inserted application
    // It should raise a conflict error
    const application: LongCeeApplication = {
      operator_id: 1,
      last_name_trunc: "CCC",
      phone_trunc: "+3360000000066",
      datetime: new Date("2022-11-02"),
      application_timestamp: new Date("2022-11-02"),
      driving_license: "driving_license_66",
      identity_key: "test",
    };

    await assertRejects(
      async () =>
        repository.registerLongApplication(
          application,
          config.rules.applicationCooldownConstraint,
        ),
      ConflictException,
    );

    // this is NOT a duplicate and it should be inserted
    const application2: LongCeeApplication = {
      operator_id: 1,
      last_name_trunc: "CCC",
      phone_trunc: "+3360000000066",
      datetime: new Date("2022-11-02"),
      application_timestamp: new Date("2022-11-02"),
      driving_license: "driving_license_66",
      identity_key: "test2",
    };

    const app2 = await repository.registerLongApplication(
      application2,
      config.rules.applicationCooldownConstraint,
    );

    assertObjectMatch(app2, {
      operator_id: 1,
      datetime: new Date("2022-11-02"),
      journey_type: CeeJourneyTypeEnum.Long,
    });
  });

  it("Should raise error if missing fields in short application", async () => {
    const application: any = {
      operator_id: 1,
      last_name_trunc: "AAA",
      phone_trunc: "+3360000000000",
      datetime: new Date("2022-11-02"),
      driving_license: "driving_license_1",
    };

    await assertRejects(
      async () =>
        await repository.registerShortApplication(
          application,
          config.rules.applicationCooldownConstraint,
        ),
    );
  });

  it("Should find short application with id if exists", async () => {
    const search: SearchCeeApplication = {
      last_name_trunc: "AAA",
      phone_trunc: "+3360000000000",
      datetime: new Date(),
    };

    const result = await repository.searchForShortApplication(
      search,
      config.rules.applicationCooldownConstraint,
    );
    assertNotEquals(result, undefined);
    assertEquals((result || {}).datetime, new Date("2022-11-01"));
  });

  it("Should find short application with driver license if exists", async () => {
    const search: SearchCeeApplication = {
      last_name_trunc: "BBB",
      phone_trunc: "+3360000000001",
      driving_license: "driving_license_1",
      datetime: new Date(),
    };

    const result = await repository.searchForShortApplication(
      search,
      config.rules.applicationCooldownConstraint,
    );
    assertNotEquals(result, undefined);
    assertEquals((result || {}).datetime, new Date("2022-11-01"));
  });

  it("Should not find long application with name match if id key is not null", async () => {
    const search: SearchCeeApplication = {
      last_name_trunc: "BBB",
      phone_trunc: "+3360000000000",
      driving_license: "driving_license_999",
      datetime: new Date(),
      identity_key: "test_2",
    };

    const result = await repository.searchForLongApplication(
      search,
      config.rules.applicationCooldownConstraint,
    );
    assertEquals(result, undefined);
  });

  it("Should find long application with id key if exists", async () => {
    const search: SearchCeeApplication = {
      last_name_trunc: "EEE",
      phone_trunc: "+336000000777",
      driving_license: "driving_license_777",
      datetime: new Date(),
      identity_key: "test",
    };

    const result = await repository.searchForLongApplication(
      search,
      config.rules.applicationCooldownConstraint,
    );
    assertNotEquals(result, undefined);
    assertEquals((result || {}).datetime, new Date("2022-11-01"));
  });

  it("Should not find short application if criterias dont match", async () => {
    const search: SearchCeeApplication = {
      last_name_trunc: "BBB",
      phone_trunc: "+3360000000001",
      driving_license: "driving_license_2",
      datetime: new Date(),
    };

    const result = await repository.searchForShortApplication(
      search,
      config.rules.applicationCooldownConstraint,
    );
    assertEquals(result, undefined);
  });

  it("Should match cooldown criteria", async () => {
    const app1 = {
      application_timestamp: new Date("2014-01-01T00:00:00.000Z"),
      operator_id: 1,
      last_name_trunc: "CCC",
      phone_trunc: "+336123456",
      datetime: new Date("2014-01-01T00:00:00.000Z"),
      journey_type: CeeJourneyTypeEnum.Long,
    };
    const app2 = {
      application_timestamp: new Date("2016-01-01T00:00:00.000Z"),
      operator_id: 1,
      last_name_trunc: "DDD",
      phone_trunc: "+336123457",
      datetime: new Date("2016-01-01T00:00:00.000Z"),
      journey_type: CeeJourneyTypeEnum.Long,
    };
    const data = [app1, app2];

    for (const application of data) {
      await repository.importApplication(application);
    }

    // should return undefined
    const result1 = await repository.searchForLongApplication(
      {
        last_name_trunc: "CCC",
        phone_trunc: "+336123456",
        datetime: new Date("2025-01-02T00:00:00.000Z"),
      },
      config.rules.applicationCooldownConstraint,
    );
    assertEquals(result1, undefined);

    // should be found
    const result2 = await repository.searchForLongApplication(
      {
        last_name_trunc: "DDD",
        phone_trunc: "+336123457",
        datetime: new Date("2025-01-02T00:00:00.000Z"),
      },
      config.rules.applicationCooldownConstraint,
    );
    assertObjectMatch(result2 as object, {
      operator_id: 1,
      journey_type: CeeJourneyTypeEnum.Long,
      datetime: new Date("2016-01-01T00:00:00.000Z"),
    });

    // should register an app
    const app1Reg = await repository.registerLongApplication(
      { ...app1, driving_license: "toto" },
      config.rules.applicationCooldownConstraint,
    );
    assert(app1Reg);

    // should throw an error
    await assertRejects(async () =>
      repository.registerLongApplication(
        { ...app2, driving_license: "toto2" },
        config.rules.applicationCooldownConstraint,
      )
    );
  });

  it("Should register application error", async () => {
    const uuidResult = await db.connection
      .getClient()
      .query<any>(
        `SELECT _id FROM ${repository.ceeApplicationsTable} LIMIT 1`,
      );
    const data1 = {
      operator_id: 1,
      error_type: CeeApplicationErrorEnum.Conflict,
      journey_type: CeeJourneyTypeEnum.Long,
      last_name_trunc: "TOT",
      operator_journey_id: "TOTO",
      identity_key: "tototo",
    };
    await repository.registerApplicationError(data1);

    const data2 = {
      operator_id: 1,
      error_type: CeeApplicationErrorEnum.Date,
      journey_type: CeeJourneyTypeEnum.Long,
      datetime: new Date().toISOString(),
      driving_license: "TOTO",
      application_id: uuidResult.rows[0]?._id,
    };
    await repository.registerApplicationError(data2);

    const errorResults = await db.connection.getClient().query<any>({
      text: `SELECT * FROM ${repository.errorTable} ORDER BY created_at`,
      values: [],
    });

    assertEquals(errorResults.rowCount, 2);
    assertObjectMatch(errorResults.rows[0], { ...data1 });
    assertObjectMatch(errorResults.rows[1], { ...data2 });
  });

  it("Should import identity_key", async () => {
    const app1 = {
      application_timestamp: new Date("2014-01-01T00:00:00.000Z"),
      operator_id: 1,
      last_name_trunc: "CCC",
      phone_trunc: "+336123458",
      datetime: new Date("2014-01-01T00:00:00.000Z"),
      journey_type: CeeJourneyTypeEnum.Short,
      identity_key: "id",
      driving_license: "toto",
      operator_journey_id: "operator_journey_id-58",
    };

    // import the application for the test
    await repository.importApplication({ ...app1, identity_key: undefined });

    // import the identity_key from the phone_trunc
    await repository.importSpecificApplicationIdentity(app1);

    // check if the identity_key is imported properly
    const result1 = await db.connection.getClient().query<any>({
      text: `
        SELECT identity_key
        FROM ${repository.ceeApplicationsTable}
        WHERE phone_trunc = $1
      `,
      values: [app1.phone_trunc],
    });
    assertEquals(result1.rows[0].identity_key, app1.identity_key);

    // importing several times should raise an exception
    await assertRejects(async () => repository.importSpecificApplicationIdentity(app1));

    /**
     * Get a CEE application UUID where the identity_key is null
     * and is_specific is false (standardized application).
     *
     * Import the identity_key for the application
     * and check if it is imported properly.
     */
    const uuidResult = await db.connection.getClient().query<any>(`
      SELECT _id as cee_application_uuid, operator_id
      FROM ${repository.ceeApplicationsTable}
      WHERE identity_key is null AND is_specific = false
      LIMIT 1
    `);
    const app2 = { ...uuidResult.rows[0], identity_key: "id2" };
    await repository.importStandardizedApplicationIdentity(app2);
    const result2 = await db.connection.getClient().query<any>({
      text: `
        SELECT identity_key
        FROM ${repository.ceeApplicationsTable}
        WHERE _id = $1
      `,
      values: [app2.cee_application_uuid],
    });

    assertEquals(result2.rows[0].identity_key, app2.identity_key);

    // importing several times should raise an exception
    await assertRejects(async () => repository.importStandardizedApplicationIdentity(app2));
  });

  it("Should find existing application", async () => {
    const result = await repository.findCeeByUuid(applicationUuid);
    assertEquals(result.uuid, applicationUuid);
  });

  it("Shoud delete existing application", async () => {
    await repository.deleteCeeByUuid(1, applicationUuid);
    const uuidResult = await db.connection.getClient().query<any>({
      text: `
      SELECT _id as uuid
      FROM ${repository.ceeApplicationsTable}
      WHERE _id = $1
      LIMIT 1
    `,
      values: [applicationUuid],
    });
    assertEquals(uuidResult.rowCount, 0);
    const uuidDResult = await db.connection.getClient().query<any>({
      text: `
      SELECT _id as uuid
      FROM ${repository.ceeDeletedApplicationsTable}
      WHERE _id = $1
      LIMIT 1
    `,
      values: [applicationUuid],
    });
    assertEquals(uuidDResult.rowCount, 1);
  });

  it("Should raise exception if not found", async () => {
    await assertRejects(async () => repository.findCeeByUuid(applicationUuid));
    await assertRejects(async () => repository.deleteCeeByUuid(1, applicationUuid));
  });
});
