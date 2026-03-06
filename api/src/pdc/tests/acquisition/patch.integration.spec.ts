import { assert, assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import {
  CarpoolAcquisitionService,
  CarpoolGeoRepository,
  CarpoolRepository,
  CarpoolRequestRepository,
  CarpoolStatusRepository,
} from "@/pdc/providers/carpool/index.ts";
import {
  CarpoolAcquisitionStatusEnum,
  CarpoolAnomalyStatusEnum,
  CarpoolFraudStatusEnum,
} from "@/pdc/providers/carpool/interfaces/common.ts";
import { SelectableCarpool } from "@/pdc/providers/carpool/interfaces/index.ts";
import { insertableCarpool } from "@/pdc/providers/carpool/mocks/database/carpool.ts";
import { CarpoolLookupRepository } from "@/pdc/providers/carpool/repositories/CarpoolLookupRepository.ts";
import { GeoProvider } from "@/pdc/providers/geo/GeoProvider.ts";
import { EtalabAPIGeoProvider } from "@/pdc/providers/geo/providers/EtalabAPIGeoProvider.ts";
import { EtalabBaseAdresseNationaleProvider } from "@/pdc/providers/geo/providers/EtalabBaseAdresseNationaleProvider.ts";
import { LocalGeoProvider } from "@/pdc/providers/geo/providers/LocalGeoProvider.ts";
import { OSRMProvider } from "@/pdc/providers/geo/providers/OSRMProvider.ts";
import { LegacyDbContext, makeLegacyDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";
import { KernelContext, makeKernelBeforeAfter } from "@/pdc/providers/test/helpers.ts";
import { AcquisitionServiceProvider } from "@/pdc/services/acquisition/AcquisitionServiceProvider.ts";

const { before: kernelBefore, after: kernelAfter } = makeKernelBeforeAfter(AcquisitionServiceProvider);
const { before: dbBefore, after: dbAfter } = makeLegacyDbBeforeAfter();

describe("Operator patches a journey", () => {
  // ---------------------------------------------------------------------------
  // SETUP
  // ---------------------------------------------------------------------------

  let db: LegacyDbContext;
  let kc: KernelContext;
  let carpoolRepository: CarpoolRepository;
  let statusRepository: CarpoolStatusRepository;
  let requestRepository: CarpoolRequestRepository;
  let lookupRepository: CarpoolLookupRepository;
  let geoRepository: CarpoolGeoRepository;
  let geoProvider: GeoProvider;
  let service: CarpoolAcquisitionService;
  let carpool: SelectableCarpool | undefined;

  function getService(overrides: any = {}): CarpoolAcquisitionService {
    return new CarpoolAcquisitionService(
      db.connection,
      overrides.statusRepository ?? statusRepository,
      overrides.requestRepository ?? requestRepository,
      overrides.lookupRepository ?? lookupRepository,
      overrides.carpoolRepository ?? carpoolRepository,
      overrides.geoRepository ?? geoRepository,
      geoProvider,
    );
  }

  const payload = { ...insertableCarpool };

  /**
   * - boot up postgresql connection
   * - create the kernel
   * - stop the existing kernel connection to replace it with the test one
   * - setup the db macro with the connection
   */
  beforeAll(async () => {
    db = await dbBefore();
    kc = await kernelBefore();
    await kc.kernel.getContainer().get(LegacyPostgresConnection).down();
    kc.kernel
      .getContainer()
      .rebind(LegacyPostgresConnection)
      .toConstantValue(db.connection);

    geoProvider = new GeoProvider(
      new EtalabAPIGeoProvider(),
      new EtalabBaseAdresseNationaleProvider(),
      new LocalGeoProvider(db.connection),
      new OSRMProvider(),
    );
    carpoolRepository = new CarpoolRepository(db.connection);
    statusRepository = new CarpoolStatusRepository(db.connection);
    requestRepository = new CarpoolRequestRepository(db.connection);
    lookupRepository = new CarpoolLookupRepository(db.connection);
    geoRepository = new CarpoolGeoRepository(db.connection);
    service = getService();
  });

  afterAll(async () => {
    await kernelAfter(kc);
    await dbAfter(db);
  });

  // ---------------------------------------------------------------------------
  // TESTS
  // ---------------------------------------------------------------------------
  it("should create a carpool", async () => {
    await service.registerRequest({ ...payload, api_version: "3" });
    carpool = await lookupRepository.findOne(
      payload.operator_id,
      payload.operator_journey_id,
    );

    assert(carpool?.acquisition_status === CarpoolAcquisitionStatusEnum.Received);
    assert(carpool.fraud_status === CarpoolFraudStatusEnum.Pending);
    assert(carpool?.anomaly_status === CarpoolAnomalyStatusEnum.Pending);
  });

  it("should not find geo for the inserted carpool", async () => {
    assert(carpool);
    const geo = await geoRepository.findOne(carpool._id);
    assert(geo === null);
  });

  it("should process geo data and find it", async () => {
    await service.processGeo({
      batchSize: 100,
      failedOnly: false,
      from: new Date(new Date().valueOf() - 200_000),
      to: new Date(new Date().valueOf() + 100_000),
    });

    assert(carpool);
    const geo = await geoRepository.findOne(carpool._id);
    assertEquals(geo?.start_geo_code, "91377");
    assertEquals(geo?.end_geo_code, "91477");
  });

  it("should fake async fraud and anomaly checks", async () => {
    assert(carpool);
    await statusRepository.saveFraudStatus(carpool._id, CarpoolFraudStatusEnum.Passed);
    await statusRepository.saveAnomalyStatus(carpool._id, CarpoolAnomalyStatusEnum.Passed);
  });

  it("should have set the status to 'processed'", async () => {
    const c = await lookupRepository.findOne(
      payload.operator_id,
      payload.operator_journey_id,
    );

    assert(c?.acquisition_status === CarpoolAcquisitionStatusEnum.Processed);
    assert(c?.fraud_status === CarpoolFraudStatusEnum.Passed);
    assert(c?.anomaly_status === CarpoolAnomalyStatusEnum.Passed);
  });

  it("should patch the journey and reset statuses", async () => {
    assert(carpool);
    await service.patchCarpool({
      operator_id: carpool.operator_id,
      operator_journey_id: carpool.operator_journey_id,
      api_version: "3",

      // patched data
      // Invert the positions to be able to check geo processing again.
      operator_trip_id: "patched_operator_trip_id",
      start_position: { ...payload.end_position },
      end_position: { ...payload.start_position },
    });

    const c = await lookupRepository.findOne(
      payload.operator_id,
      payload.operator_journey_id,
    );

    assert(c?.operator_trip_id === "patched_operator_trip_id");
    assert(c?.acquisition_status === CarpoolAcquisitionStatusEnum.Updated);
    assert(c?.fraud_status === CarpoolFraudStatusEnum.Pending);
    assert(c?.anomaly_status === CarpoolAnomalyStatusEnum.Pending);
  });

  it("should have deleted the geo data", async () => {
    assert(carpool);
    const geo = await geoRepository.findOne(carpool._id);
    assert(geo === null);
  });

  it("should process geo data again and find it", async () => {
    await service.processGeo({
      batchSize: 100,
      failedOnly: false,
      from: new Date(new Date().valueOf() - 200_000),
      to: new Date(new Date().valueOf() + 100_000),
    });

    assert(carpool);
    const geo = await geoRepository.findOne(carpool._id);

    // check inverted start and end positions
    assertEquals(geo?.start_geo_code, "91477");
    assertEquals(geo?.end_geo_code, "91377");
  });
});
