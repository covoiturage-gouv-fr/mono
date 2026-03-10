import { assertEquals } from "dep:assert";
import { afterEach, beforeEach, describe, it } from "dep:testing-bdd";
import { KernelInterfaceResolver } from "@/ilos/common/index.ts";
import { NativeCursor } from "@/ilos/connection-postgres/LegacyPostgresConnection.ts";
import { APDFNameProvider } from "@/pdc/providers/storage/index.ts";
import excel from "dep:excel";
import sinon from "dep:sinon";
import { PolicyStatusEnum } from "../../../policy/contracts/common/interfaces/PolicyInterface.ts";
import { SliceInterface } from "../../../policy/contracts/common/interfaces/Slices.ts";
import { PolicyStatsInterface } from "../../contracts/interfaces/PolicySliceStatInterface.ts";
import { CampaignSearchParamsInterface } from "../../interfaces/APDFRepositoryProviderInterface.ts";
import { DataRepositoryProvider } from "../DataRepositoryProvider.ts";
import { BuildExcel } from "./BuildExcel.ts";
import { SlicesWorksheetWriter } from "./SlicesWorksheetWriter.ts";
import { TripsWorksheetWriter } from "./TripsWorksheetWriter.ts";
import { wrapSlices } from "./wrapSlicesHelper.ts";

describe("BuildExcel", () => {
  let kernelStub: sinon.SinonStub;
  let getPolicyCursorStub: sinon.SinonStub;
  let policyStatsStub: sinon.SinonStub<
    [params: CampaignSearchParamsInterface, slices: SliceInterface[]],
    Promise<PolicyStatsInterface>
  >;
  let filenameStub: sinon.SinonStub;
  let filepathStub: sinon.SinonStub;
  let tripsWorkbookWriterStub: sinon.SinonStub;
  let slicesWorkbookWriterStub: sinon.SinonStub;
  let workbookWriterStub: sinon.SinonStub;
  let campaign = {
    _id: 458,
    name: "IDFM normal",
    territory_id: 0,
    description: "description",
    start_date: new Date("2022-01-01T00:00:00Z"),
    end_date: new Date("2022-02-01T00:00:00Z"),
    handler: "handler.js",
    status: PolicyStatusEnum.ACTIVE,
    incentive_sum: 4000n,
    params: {
      slices: [
        { start: 2_000, end: 150_000 },
        { start: 15_000, end: 300_000 },
      ],
    },
  };
  const start_date = new Date("2022-01-01T00:00:00Z");
  const end_date = new Date("2022-02-01T00:00:00Z");
  const operator_id = 4;
  const filename = "APDF-idfm.xlsx";
  const filepath = "/tmp/APDF-idfm.xlsx";
  const workbookWriterMock = { commit: () => {} };
  const config = { tz: "Europe/Paris", booster_dates: new Set<string>(), extras: {} };
  const kernel = new (class extends KernelInterfaceResolver {})();
  const createSlicesSheetToWorkbook = new SlicesWorksheetWriter();
  const dataRepositoryProvider = new DataRepositoryProvider(null as any);
  const nameProvider = new APDFNameProvider();
  const streamDataToWorkbook = new TripsWorksheetWriter();
  const buildExcel = new BuildExcel(
    kernel,
    dataRepositoryProvider,
    streamDataToWorkbook,
    createSlicesSheetToWorkbook,
    nameProvider,
  );
  const cursorCallback: NativeCursor<unknown[]> = {
    read: async (_rowCount?: number) => {
      return [];
    },
    release: async () => {},
  };

  beforeEach(() => {
    workbookWriterStub = sinon
      .stub(BuildExcel, "initWorkbookWriter")
      .returns(
        workbookWriterMock as excel.stream.xlsx.WorkbookWriter,
      );
    filenameStub = sinon.stub(nameProvider, "filename");
    filepathStub = sinon.stub(nameProvider, "filepath");
    kernelStub = sinon.stub(kernel, "call");
    tripsWorkbookWriterStub = sinon.stub(
      streamDataToWorkbook,
      "call",
    );
    getPolicyCursorStub = sinon.stub(
      dataRepositoryProvider,
      "getPolicyCursor",
    );
    policyStatsStub = sinon.stub(
      dataRepositoryProvider,
      "getPolicyStats",
    );
    slicesWorkbookWriterStub = sinon.stub(
      createSlicesSheetToWorkbook,
      "call",
    );
  });

  afterEach(() => {
    workbookWriterStub!.restore();
    filenameStub!.restore();
    filepathStub!.restore();
    kernelStub!.restore();
    tripsWorkbookWriterStub!.restore();
    getPolicyCursorStub!.restore();
    policyStatsStub.restore();
    slicesWorkbookWriterStub!.restore();
  });

  it("BuildExcel: should call stream data and create slice then return excel filepath", async () => {
    // Arrange
    getPolicyCursorStub!.returns(cursorCallback);
    filenameStub!.returns(filename);
    filepathStub!.returns(filepath);
    tripsWorkbookWriterStub!.resolves();
    kernelStub!.resolves(config);
    policyStatsStub?.resolves({
      total_count: 111,
      total_sum: 222_00,
      subsidized_count: 111,
      slices: [
        {
          count: 1,
          sum: 2_00,
          subsidized: 1_50,
          slice: { start: 0, end: 10_000 },
        },
      ],
    });

    // Act
    const { filename: fn, filepath: fp } = await buildExcel!.call(
      campaign!,
      start_date!,
      end_date!,
      operator_id!,
    );

    // Assert
    sinon.assert.calledOnceWithExactly(
      policyStatsStub!,
      {
        campaign_id: campaign._id,
        operator_id: operator_id,
        start_date: start_date,
        end_date: end_date,
      },
      wrapSlices(campaign.params.slices),
    );

    sinon.assert.calledOnceWithExactly(getPolicyCursorStub!, {
      campaign_id: campaign._id,
      operator_id: operator_id,
      start_date: start_date,
      end_date: end_date,
    });

    sinon.assert.calledOnceWithExactly(filenameStub!, {
      name: campaign!.name,
      campaign_id: campaign?._id,
      operator_id: operator_id,
      datetime: start_date,
      trips: 111,
      subsidized: 111,
      amount: 222_00,
    });

    sinon.assert.calledOnceWithExactly(
      tripsWorkbookWriterStub!,
      cursorCallback,
      config,
      workbookWriterMock,
    );
    sinon.assert.calledOnce(policyStatsStub!);
    sinon.assert.calledOnce(slicesWorkbookWriterStub!);
    assertEquals(filename, fn);
    assertEquals(filepath, fp);
  });

  it("BuildExcel: should call stream data and return filepath even if create slice error occurs", async () => {
    // Arrange
    getPolicyCursorStub!.returns(cursorCallback);
    filenameStub!.returns(filename);
    filepathStub!.returns(filepath);
    slicesWorkbookWriterStub!.rejects("Error while computing slices");
    kernelStub!.resolves(config);
    policyStatsStub?.resolves({
      total_count: 111,
      total_sum: 222_00,
      subsidized_count: 111,
      slices: [
        {
          count: 1,
          sum: 2_00,
          subsidized: 1_50,
          slice: { start: 0, end: 10_000 },
        },
      ],
    });

    // Act
    const { filename: fn, filepath: fp } = await buildExcel!.call(
      campaign!,
      start_date!,
      end_date!,
      operator_id!,
    );

    // Assert
    sinon.assert.calledOnceWithExactly(
      policyStatsStub!,
      {
        campaign_id: campaign._id,
        operator_id: operator_id,
        start_date: start_date,
        end_date: end_date,
      },
      wrapSlices(campaign.params.slices),
    );

    sinon.assert.calledOnceWithExactly(getPolicyCursorStub!, {
      campaign_id: campaign._id,
      operator_id: operator_id,
      start_date: start_date,
      end_date: end_date,
    });

    sinon.assert.calledOnceWithExactly(filenameStub!, {
      name: campaign!.name,
      campaign_id: campaign?._id,
      operator_id: operator_id,
      datetime: start_date,
      trips: 111,
      subsidized: 111,
      amount: 222_00,
    });

    sinon.assert.calledOnceWithExactly(
      tripsWorkbookWriterStub!,
      cursorCallback,
      config,
      workbookWriterMock,
    );
    sinon.assert.calledOnce(policyStatsStub!);
    sinon.assert.calledOnce(slicesWorkbookWriterStub!);
    assertEquals(filename, fn);
    assertEquals(filepath, fp);
  });

  it("BuildExcel: should call stream data and return excel filepath without slices", async () => {
    // Arrange
    campaign = { ...campaign!, params: { slices: [] } };

    getPolicyCursorStub!.returns(cursorCallback);
    filenameStub!.returns(filename);
    filepathStub!.returns(filepath);
    slicesWorkbookWriterStub!.rejects("Error while computing slices");
    kernelStub!.resolves(config);
    policyStatsStub?.resolves({
      total_count: 111,
      total_sum: 222_00,
      subsidized_count: 111,
      slices: [],
    });

    // Act
    const { filename: fn, filepath: fp } = await buildExcel!.call(
      campaign!,
      start_date!,
      end_date!,
      operator_id!,
    );

    // Assert
    sinon.assert.calledOnceWithExactly(
      policyStatsStub!,
      {
        campaign_id: campaign._id,
        operator_id: operator_id,
        start_date: start_date,
        end_date: end_date,
      },
      wrapSlices(campaign.params.slices),
    );

    sinon.assert.calledOnceWithExactly(getPolicyCursorStub!, {
      campaign_id: campaign._id,
      operator_id: operator_id,
      start_date: start_date,
      end_date: end_date,
    });

    sinon.assert.calledOnceWithExactly(filenameStub!, {
      name: campaign!.name,
      campaign_id: campaign?._id,
      operator_id: operator_id,
      datetime: start_date,
      trips: 111,
      subsidized: 111,
      amount: 222_00,
    });

    sinon.assert.calledOnceWithExactly(
      tripsWorkbookWriterStub!,
      cursorCallback,
      config,
      workbookWriterMock,
    );
    sinon.assert.calledOnce(policyStatsStub!);
    sinon.assert.notCalled(slicesWorkbookWriterStub!);
    assertEquals(filename, fn);
    assertEquals(filepath, fp);
  });
});
