import { assert, assertEquals, assertObjectMatch, assertRejects } from "dep:assert";
import { afterAll, afterEach, beforeAll, beforeEach, describe, it } from "dep:testing-bdd";
import { ConflictException } from "@/ilos/common/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";
import { GeoProvider } from "@/pdc/providers/geo/index.ts";
import { LegacyDbContext, makeLegacyDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import sinon, { SinonSandbox } from "dep:sinon";
import { insertableCarpool, updatableCarpool } from "../mocks/database/carpool.ts";
import { CarpoolGeoRepository } from "../repositories/CarpoolGeoRepository.ts";
import { CarpoolLookupRepository } from "../repositories/CarpoolLookupRepository.ts";
import { CarpoolRepository } from "../repositories/CarpoolRepository.ts";
import { CarpoolRequestRepository } from "../repositories/CarpoolRequestRepository.ts";
import { CarpoolStatusRepository } from "../repositories/CarpoolStatusRepository.ts";
import { CarpoolAcquisitionService } from "./CarpoolAcquisitionService.ts";

describe("CarpoolAcquisitionService", () => {
  let carpoolRepository: CarpoolRepository;
  let statusRepository: CarpoolStatusRepository;
  let requestRepository: CarpoolRequestRepository;
  let lookupRepository: CarpoolLookupRepository;
  let geoRepository: CarpoolGeoRepository;
  let geoService: GeoProvider;
  let db: LegacyDbContext;
  let sinonSB: SinonSandbox;

  const { before, after } = makeLegacyDbBeforeAfter();
  beforeAll(async () => {
    db = await before();
    geoService = sinon.createStubInstance(GeoProvider);
    carpoolRepository = new CarpoolRepository(db.connection);
    statusRepository = new CarpoolStatusRepository(db.connection);
    requestRepository = new CarpoolRequestRepository(db.connection);
    lookupRepository = new CarpoolLookupRepository(db.connection);
    geoRepository = new CarpoolGeoRepository(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  beforeEach(() => {
    sinonSB = sinon.createSandbox();
  });

  afterEach(() => {
    sinonSB.restore();
  });

  function getService(overrides: any): CarpoolAcquisitionService {
    return new CarpoolAcquisitionService(
      db.connection,
      overrides.statusRepository ?? statusRepository,
      overrides.requestRepository ?? requestRepository,
      overrides.lookupRepository ?? lookupRepository,
      overrides.carpoolRepository ?? carpoolRepository,
      overrides.geoRepository ?? geoRepository,
      geoService,
    );
  }

  it("Should create carpool", async () => {
    const carpoolRepositoryL = sinonSB.spy(carpoolRepository);
    const requestRepositoryL = sinonSB.spy(requestRepository);
    const statusRepositoryL = sinonSB.spy(statusRepository);

    const service = getService({
      carpoolRepository: carpoolRepositoryL,
      requestRepository: requestRepositoryL,
      statusRepository: statusRepositoryL,
    });

    const data = { ...insertableCarpool };
    await service.registerRequest({ ...data, api_version: "3" });

    assert(carpoolRepositoryL.register.calledOnce);
    assert(requestRepositoryL.save.calledOnce);
    assert(statusRepositoryL.saveAcquisitionStatus.calledOnce);

    const r = await lookupRepository.findOne(
      data.operator_id,
      data.operator_journey_id,
    );

    const { _id, uuid, created_at, updated_at, ...carpool } = r || {};

    assertObjectMatch(carpool, {
      ...data,
      fraud_status: "pending",
      acquisition_status: "received",
    });
  });

  it("Should throw conflict exception on existing carpool", async () => {
    // Arrange
    const carpoolRepositoryL = sinonSB.spy(carpoolRepository);
    const requestRepositoryL = sinonSB.spy(requestRepository);
    const statusRepositoryL = sinonSB.spy(statusRepository);

    const service = getService({
      carpoolRepository: carpoolRepositoryL,
      requestRepository: requestRepositoryL,
      statusRepository: statusRepositoryL,
    });

    const data = { ...insertableCarpool };

    // Act & Assert
    await assertRejects(
      () => service.registerRequest({ ...data, api_version: "3" }),
      ConflictException,
    );

    // Assert
    assert(carpoolRepositoryL.register.calledOnce);
    assert(requestRepositoryL.save.calledOnce);
    assert(statusRepositoryL.saveAcquisitionStatus.notCalled);

    const r = await lookupRepository.findOne(
      data.operator_id,
      data.operator_journey_id,
    );

    const { _id, uuid, created_at, updated_at, ...carpool } = r || {};

    assertObjectMatch(carpool, {
      ...data,
      fraud_status: "pending",
      acquisition_status: "received",
    });
  });

  it("Should patch carpool", async () => {
    const carpoolRepositoryL = sinonSB.spy(carpoolRepository);
    const requestRepositoryL = sinonSB.spy(requestRepository);
    const statusRepositoryL = sinonSB.spy(statusRepository);

    const service = getService({
      carpoolRepository: carpoolRepositoryL,
      requestRepository: requestRepositoryL,
      statusRepository: statusRepositoryL,
    });

    const data = { ...updatableCarpool };
    await service.patchCarpool({
      ...data,
      api_version: "3",
      operator_id: insertableCarpool.operator_id,
      operator_journey_id: insertableCarpool.operator_journey_id,
    });

    assert(carpoolRepositoryL.update.calledOnce);
    assert(requestRepositoryL.save.calledOnce);
    assert(statusRepositoryL.saveAcquisitionStatus.calledOnce);

    const r = await lookupRepository.findOne(
      insertableCarpool.operator_id,
      insertableCarpool.operator_journey_id,
    );

    const { _id, uuid, created_at, updated_at, ...carpool } = r || {};

    assertObjectMatch(carpool, {
      ...insertableCarpool,
      ...updatableCarpool,
      fraud_status: "pending",
      acquisition_status: "updated",
    });
  });

  it("Should cancel carpool", async () => {
    const lookupRepositoryL = sinonSB.spy(lookupRepository);
    const requestRepositoryL = sinonSB.spy(requestRepository);
    const statusRepositoryL = sinonSB.spy(statusRepository);

    const service = getService({
      lookupRepository: lookupRepositoryL,
      requestRepository: requestRepositoryL,
      statusRepository: statusRepositoryL,
    });

    const data = {
      cancel_code: "FRAUD",
      cancel_message: "Got u",
      api_version: "3",
      operator_id: insertableCarpool.operator_id,
      operator_journey_id: insertableCarpool.operator_journey_id,
    };
    await service.cancelRequest(data);

    assert(lookupRepositoryL.findOneStatus.calledOnce);
    assert(requestRepositoryL.save.calledOnce);
    assert(statusRepositoryL.saveAcquisitionStatus.calledOnce);

    const r = lookupRepository.findOne(
      insertableCarpool.operator_id,
      insertableCarpool.operator_journey_id,
    );

    const { _id, uuid, created_at, updated_at, ...carpool } = await r || {};

    assertObjectMatch(carpool, {
      ...insertableCarpool,
      ...updatableCarpool,
      fraud_status: "pending",
      acquisition_status: "canceled",
    });
  });

  it("Should rollback if something fails", async () => {
    const carpoolRepositoryL = sinonSB.spy(carpoolRepository);
    const requestRepositoryL = sinonSB.spy(requestRepository);
    sinonSB.replace(
      statusRepository,
      "saveAcquisitionStatus",
      sinonSB.fake.throws(new Error("DB")),
    );

    const service = getService({
      carpoolRepository: carpoolRepositoryL,
      requestRepository: requestRepositoryL,
    });

    const data = {
      ...insertableCarpool,
      operator_journey_id: "operator_journey_id_2",
    };
    await assertRejects(async () => await service.registerRequest({ ...data, api_version: "3" }));

    assert(carpoolRepositoryL.register.calledOnce);
    assert(requestRepositoryL.save.calledOnce);

    const result = await db.connection
      .getClient()
      .query(
        sql`SELECT * FROM ${
          raw(carpoolRepository.table)
        } WHERE operator_id = ${data.operator_id} AND operator_journey_id = ${data.operator_journey_id}`,
      );
    assertEquals(result.rows, []);
  });

  it("Should raise error if distance too short terms is violated", async () => {
    const carpoolL = sinonSB.spy(lookupRepository);
    const service = getService({
      CarpoolLookupRepository: carpoolL,
    });

    const data = {
      operator_id: 1,
      created_at: new Date("2024-01-01T05:00:00.000Z"),
      distance: 4_000,
      driver_identity_key: "key_driver",
      passenger_identity_key: "key_passenger",
      start_datetime: new Date("2024-01-01T02:00:00.000Z"),
      end_datetime: new Date("2024-01-01T04:00:00.000Z"),
      operator_trip_id: "operator_trip_id",
      start_position: { lon: 2.3522, lat: 48.8566 },
    };

    const errors = await service.verifyTermsViolation({ ...data, distance: 100 });
    assertEquals(errors, [{ label: "distance_too_short" }]);
    assertEquals(
      carpoolL.countJourneyBy.getCalls().map((c: any) => c.args),
      [
        [
          {
            identity_key: ["key_driver"],
            identity_key_or: true,
            start_date: {
              max: new Date("2024-01-01T22:59:59.999Z"),
              min: new Date("2023-12-31T23:00:00.000Z"),
            },
            operator_id: 1,
          },
          undefined,
        ],
        [
          {
            identity_key: ["key_passenger"],
            identity_key_or: true,
            start_date: {
              max: new Date("2024-01-01T22:59:59.999Z"),
              min: new Date("2023-12-31T23:00:00.000Z"),
            },
            operator_id: 1,
          },
          undefined,
        ],
        [
          {
            identity_key: ["key_driver", "key_passenger"],
            identity_key_or: false,
            start_date: {
              min: new Date("2024-01-01T02:00:00.000Z"),
              max: new Date("2024-01-01T04:30:00.000Z"),
            },
            end_date: {
              min: new Date("2024-01-01T01:30:00.000Z"),
              max: new Date("2024-01-01T04:00:00.000Z"),
            },
            operator_trip_id: "operator_trip_id",
            operator_id: 1,
          },
          undefined,
        ],
      ],
    );
  });

  it("Should raise error if expired terms is violated", async () => {
    const carpoolL = sinonSB.spy(lookupRepository);
    const service = getService({
      CarpoolLookupRepository: carpoolL,
    });

    const data = {
      operator_id: 1,
      created_at: new Date("2024-10-24 06:37:58.000Z"),
      start_datetime: new Date("2024-10-23 05:00:00.000Z"),
      distance: 4_000,
      driver_identity_key: "key_driver",
      passenger_identity_key: "key_passenger",
      end_datetime: new Date("2024-10-23 07:20:00.000Z"),
      operator_trip_id: "operator_trip_id",
      start_position: { lon: 2.3522, lat: 48.8566 },
    };

    const errors = await service.verifyTermsViolation(data);
    assertEquals(errors, [{ label: "expired" }]);
    assertEquals(
      carpoolL.countJourneyBy.getCalls().map((c: any) => c.args),
      [
        [
          {
            identity_key: ["key_driver"],
            identity_key_or: true,
            operator_id: 1,
            start_date: {
              max: new Date("2024-10-23T21:59:59.999Z"),
              min: new Date("2024-10-22T22:00:00.000Z"),
            },
          },
          undefined,
        ],
        [
          {
            identity_key: ["key_passenger"],
            identity_key_or: true,
            operator_id: 1,
            start_date: {
              max: new Date("2024-10-23T21:59:59.999Z"),
              min: new Date("2024-10-22T22:00:00.000Z"),
            },
          },
          undefined,
        ],
        [
          {
            end_date: {
              max: new Date("2024-10-23T07:20:00.000Z"),
              min: new Date("2024-10-23T04:30:00.000Z"),
            },
            identity_key: [
              "key_driver",
              "key_passenger",
            ],
            identity_key_or: false,
            operator_id: 1,
            operator_trip_id: "operator_trip_id",
            start_date: {
              max: new Date("2024-10-23T07:50:00.000Z"),
              min: new Date("2024-10-23T05:00:00.000Z"),
            },
          },
          undefined,
        ],
      ],
    );
  });

  describe("too_many_trips_by_day", () => {
    const operator_id = insertableCarpool.operator_id;
    // created_at is set by the DB to "now" on insert, so start_datetime must stay
    // recent (< 24h) to avoid tripping the unrelated "expired" rule.
    const day = new Date(Date.now() - 4 * 3_600_000);

    function tripAt(minuteOffset: number, overrides: Partial<typeof insertableCarpool>) {
      const start_datetime = new Date(day.valueOf() + minuteOffset * 60_000);
      return {
        ...insertableCarpool,
        operator_id,
        start_datetime,
        end_datetime: new Date(start_datetime.valueOf() + 1_800_000),
        ...overrides,
      };
    }

    async function register(service: CarpoolAcquisitionService, data: ReturnType<typeof tripAt>) {
      return service.registerRequest({ ...data, api_version: "3" });
    }

    it("Should not flag two regular users each having 4 trips a day meeting for a 5th trip together", async () => {
      const service = getService({});
      const driver_identity_key = "a".repeat(64);
      const passenger_identity_key = "b".repeat(64);

      // driver already did 4 trips today, each with a different passenger
      for (let i = 0; i < 4; i++) {
        await register(
          service,
          tripAt(i * 15, {
            operator_journey_id: `too_many_driver_${i}`,
            operator_trip_id: `too_many_driver_${i}`,
            driver_identity_key,
            passenger_identity_key: `driver_partner_${i}`.padEnd(64, "0"),
          }),
        );
      }

      // passenger already did 4 trips today, each with a different driver
      for (let i = 0; i < 4; i++) {
        await register(
          service,
          tripAt(60 + i * 15, {
            operator_journey_id: `too_many_passenger_${i}`,
            operator_trip_id: `too_many_passenger_${i}`,
            driver_identity_key: `passenger_partner_${i}`.padEnd(64, "0"),
            passenger_identity_key,
          }),
        );
      }

      // driver and passenger meet for a 5th trip each, together
      const res = await register(
        service,
        tripAt(150, {
          operator_journey_id: "too_many_together",
          operator_trip_id: "too_many_together",
          driver_identity_key,
          passenger_identity_key,
        }),
      );

      assert(!res.terms_violation_error_labels.includes("too_many_trips_by_day"));
    });

    it("Should flag a user having 5 trips a day", async () => {
      const service = getService({});
      const driver_identity_key = "c".repeat(64);

      for (let i = 0; i < 5; i++) {
        await register(
          service,
          tripAt(i * 15, {
            operator_journey_id: `five_trips_${i}`,
            operator_trip_id: `five_trips_${i}`,
            driver_identity_key,
            passenger_identity_key: `five_trips_partner_${i}`.padEnd(64, "0"),
          }),
        );
      }

      const res = await register(
        service,
        tripAt(150, {
          operator_journey_id: "five_trips_new",
          operator_trip_id: "five_trips_new",
          driver_identity_key,
          passenger_identity_key: "new_partner".padEnd(64, "0"),
        }),
      );

      assert(res.terms_violation_error_labels.includes("too_many_trips_by_day"));

      const detail = res.terms_violation_error_details.find((d) => d.label === "too_many_trips_by_day");
      assert(detail && "metas" in detail);
      assertEquals(detail.metas, { driver: 5, passenger: 0, limit: 4 });
    });
  });

  it("Should keep terms_violation_error status when geo batch processes the carpool", async () => {
    const service = getService({});

    const data = {
      ...insertableCarpool,
      operator_journey_id: "tve_geo_guard",
      distance: 100, // triggers distance_too_short
    };

    const res = await service.registerRequest({ ...data, api_version: "3" });
    assert(res.terms_violation_error_labels.includes("distance_too_short"));

    const before = await statusRepository.getStatusByOperatorJourneyId(
      data.operator_id,
      data.operator_journey_id,
    );
    assertEquals(before?.acquisition_status, "terms_violation_error");

    // The geo batch must geocode the carpool WITHOUT downgrading its status.
    await service.processGeo({
      batchSize: 1000,
      from: new Date(Date.now() - 86_400_000),
      to: new Date(Date.now() + 86_400_000),
      failedOnly: false,
    });

    const after = await statusRepository.getStatusByOperatorJourneyId(
      data.operator_id,
      data.operator_journey_id,
    );
    assertEquals(after?.acquisition_status, "terms_violation_error");
  });

  it("Should move a received carpool to processed when geo batch runs", async () => {
    const service = getService({});

    const data = {
      ...insertableCarpool,
      operator_journey_id: "geo_no_violation",
      driver_identity_key: "2".repeat(64),
      passenger_identity_key: "3".repeat(64),
    };

    const res = await service.registerRequest({ ...data, api_version: "3" });
    assertEquals(res.terms_violation_error_labels, []);

    const before = await statusRepository.getStatusByOperatorJourneyId(
      data.operator_id,
      data.operator_journey_id,
    );
    assertEquals(before?.acquisition_status, "received");

    await service.processGeo({
      batchSize: 1000,
      from: new Date(Date.now() - 86_400_000),
      to: new Date(Date.now() + 86_400_000),
      failedOnly: false,
    });

    const after = await statusRepository.getStatusByOperatorJourneyId(
      data.operator_id,
      data.operator_journey_id,
    );
    assertEquals(after?.acquisition_status, "processed");
  });
});
