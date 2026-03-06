import sinon from "dep:sinon";
import { afterEach, assertEquals, beforeEach, describe, it } from "@/dev_deps.ts";
import { KernelInterfaceResolver } from "@/ilos/common/index.ts";
import { BucketName, S3StorageProvider } from "@/pdc/providers/storage/index.ts";
import { uuid } from "@/pdc/providers/test/index.ts";
import { PolicyStatusEnum } from "@/pdc/services/policy/contracts/common/interfaces/PolicyInterface.ts";
import { ResultInterface as Campaign } from "@/pdc/services/policy/contracts/find.contract.ts";
import { startOfMonth, subMonths } from "dep:date-fns";
import { fromZonedTime } from "dep:date-fns-tz";
import { faker } from "dep:faker";
import { createGetCampaignResult } from "../helpers/createGetCampaignResult.helper.ts";
import { DataRepositoryProviderInterfaceResolver } from "../interfaces/APDFRepositoryProviderInterface.ts";
import { CheckCampaign } from "../providers/CheckCampaign.ts";
import { BuildExcel } from "../providers/excel/BuildExcel.ts";
import { ExportAction } from "./ExportAction.ts";

describe("Application Service", () => {
  let kernelStub: sinon.SinonStub;
  let checkCampaignStub: sinon.SinonStub;
  let s3StorageProviderStub: sinon.SinonStub;
  let buildExcelStub: sinon.SinonStub;
  let apdfRepositoryStub: sinon.SinonStub;

  class TR extends DataRepositoryProviderInterfaceResolver {}
  const START_DATE = new Date("2020-01-08T00:00:00Z");
  const END_DATE = new Date("2020-02-08T00:00:00Z");
  const START_DATE_STRING = START_DATE.toISOString();
  const END_DATE_STRING = END_DATE.toISOString();
  const kernel = new (class extends KernelInterfaceResolver {})();
  const checkCampaign = new CheckCampaign(null as any);
  const s3StorageProvider = new S3StorageProvider(null as any);
  const buildExcel = new BuildExcel(
    null as any,
    null as any,
    null as any,
    null as any,
    null as any,
  );
  const config = {
    get<T>(): T {
      return true as T; // enable upload
    },
  };
  const apdfRepository = new TR();
  const buildExcelsExportAction = new ExportAction(
    kernel,
    checkCampaign,
    s3StorageProvider,
    apdfRepository,
    buildExcel,
    config,
  );

  beforeEach(() => {
    kernelStub = sinon.stub(kernel, "call");
    checkCampaignStub = sinon.stub(checkCampaign, "call");
    s3StorageProviderStub = sinon.stub(
      s3StorageProvider,
      "upload",
    );
    buildExcelStub = sinon.stub(buildExcel, "call");
    apdfRepositoryStub = sinon.stub(
      apdfRepository,
      "getPolicyActiveOperators",
    );
  });

  afterEach(() => {
    kernelStub.restore();
    checkCampaignStub!.restore();
    s3StorageProviderStub!.restore();
    buildExcelStub.restore();
    apdfRepositoryStub.restore();
  });

  it("ExportAction: should create 1 xlsx file for last month if no date range provided, 1 campaign with 1 operator", async () => {
    // Arrange
    const campaign: Campaign = createGetCampaignResult(PolicyStatusEnum.ACTIVE);
    const filename = `campaign-${uuid()}.xlsx`;
    const filepath = `/tmp/exports/${filename}`;
    checkCampaignStub!.resolves(campaign);
    buildExcelStub!.resolves(filepath);
    s3StorageProviderStub!.resolves(filename);
    apdfRepositoryStub!.resolves([4]);

    // Act
    const result = await buildExcelsExportAction!.handle(
      {
        format: { tz: "Europe/Paris" },
        query: {
          campaign_id: [campaign._id!],
        },
      },
      {
        channel: {
          service: "",
          transport: undefined,
          metadata: undefined,
        },
      },
    );

    // Assert
    const startDate = fromZonedTime(
      startOfMonth(subMonths(new Date(), 1)),
      "Europe/Paris",
    );
    const endDate = fromZonedTime(
      startOfMonth(new Date()),
      "Europe/Paris",
    );

    assertEquals(result, [filename]);
    sinon.assert.calledOnceWithMatch(checkCampaignStub!, campaign._id);
    sinon.assert.called(s3StorageProviderStub!);
    assertEquals(
      new Date(checkCampaignStub!.args[0][1]),
      startDate,
    );
    assertEquals(new Date(checkCampaignStub!.args[0][2]), endDate);
  });

  it("ExportAction: should create 1 xlsx file if date range provided and 1 campaign id", async () => {
    // Arrange
    const campaign: Campaign = createGetCampaignResult(PolicyStatusEnum.ACTIVE);
    const filename = `campaign-${uuid()}.xlsx`;
    const filepath = `/tmp/exports/${filename}`;
    const s3_key: string = faker.system.fileName();
    checkCampaignStub!.resolves(campaign);
    buildExcelStub!.resolves({ filename, filepath });
    s3StorageProviderStub!.resolves(s3_key);
    apdfRepositoryStub!.resolves([4]);

    // Act
    const result: string[] = await buildExcelsExportAction!.handle(
      {
        format: { tz: "Europe/Paris" },
        query: {
          campaign_id: [campaign._id],
          date: {
            start: START_DATE_STRING,
            end: END_DATE_STRING,
          },
        },
        // Cast to check date type conversion
      } as any,
      {
        channel: {
          service: "",
          transport: undefined,
          metadata: undefined,
        },
      },
    );

    // Assert
    sinon.assert.calledOnceWithExactly(
      checkCampaignStub,
      campaign._id,
      START_DATE,
      END_DATE,
    );

    sinon.assert.calledOnceWithExactly(
      s3StorageProviderStub,
      BucketName.APDF,
      filepath,
      filename,
      `${campaign._id}`,
    );

    sinon.assert.calledOnceWithExactly(
      apdfRepositoryStub,
      campaign._id,
      START_DATE,
      END_DATE,
    );
    assertEquals(result, [s3_key]);
  });

  it("ExportAction: should create 4 xlsx file if date range provided and 2 campaigns with 2 operators each", async () => {
    // Arrange
    const campaign1: Campaign = createGetCampaignResult(
      PolicyStatusEnum.ACTIVE,
    );
    const campaign2: Campaign = createGetCampaignResult(
      PolicyStatusEnum.ACTIVE,
    );
    const expectedFiles: string[] = [0, 1, 2, 3].map((i) => {
      const filename = `${faker.system.fileName()}.xlsx`;
      const filepath = `/tmp/exports/${filename}`;
      buildExcelStub!.onCall(i).resolves({ filename, filepath });
      s3StorageProviderStub!.onCall(i).resolves(filename);
      return filename;
    });
    checkCampaignStub!.withArgs(campaign1._id).resolves(campaign1);
    checkCampaignStub!.withArgs(campaign2._id).resolves(campaign2);
    apdfRepositoryStub!.resolves([4, 5]);

    // Act
    const result = await buildExcelsExportAction!.handle(
      {
        format: { tz: "Europe/Paris" },
        query: {
          campaign_id: [campaign1._id, campaign2._id],
          date: {
            start: START_DATE_STRING,
            end: END_DATE_STRING,
          },
        },
        // Cast to check date type conversion
      } as any,
      {
        channel: {
          service: "",
          transport: undefined,
          metadata: undefined,
        },
      },
    );

    // Assert
    sinon.assert.calledWithExactly(
      checkCampaignStub!.firstCall,
      campaign1._id,
      START_DATE,
      END_DATE,
    );
    sinon.assert.calledWithExactly(
      checkCampaignStub!.secondCall,
      campaign2._id,
      START_DATE,
      END_DATE,
    );
    assertEquals(result.sort(), expectedFiles.sort());
    assertEquals(checkCampaignStub!.args[0][0], campaign1._id);
    assertEquals(checkCampaignStub!.args[1][0], campaign2._id);
  });

  it("ExportAction: should send error and process other if 1 export failed", async () => {
    // Arrange
    const campaign1: Campaign = createGetCampaignResult(
      PolicyStatusEnum.ACTIVE,
    );
    const campaign2: Campaign = createGetCampaignResult(
      PolicyStatusEnum.ACTIVE,
    );
    checkCampaignStub!.withArgs(campaign1._id).resolves(campaign1);
    checkCampaignStub!.withArgs(campaign2._id).resolves(campaign2);
    apdfRepositoryStub!.resolves([4, 5]);
    const filename = `${faker.system.fileName()}.xlsx`;
    const filepath = `/tmp/exports/${filename}`;
    buildExcelStub!.resolves({ filename, filepath });
    s3StorageProviderStub!.resolves(filename);
    buildExcelStub!.onCall(3).rejects(`Error`);

    // Act
    const result = await buildExcelsExportAction!.handle(
      {
        format: { tz: "Europe/Paris" },
        query: {
          campaign_id: [campaign1._id, campaign2._id],
          date: {
            start: START_DATE_STRING,
            end: END_DATE_STRING,
          },
        },
        // Cast to check date type conversion
      } as any,
      {
        channel: {
          service: "",
          transport: undefined,
          metadata: undefined,
        },
      },
    );

    // Assert
    sinon.assert.calledWithExactly(
      checkCampaignStub!.firstCall,
      campaign1._id,
      START_DATE,
      END_DATE,
    );
    sinon.assert.calledWithExactly(
      checkCampaignStub!.secondCall,
      campaign2._id,
      START_DATE,
      END_DATE,
    );
    assertEquals(
      result.sort(),
      [
        filename,
        filename,
        `[apdf:export] (campaign: ${campaign2.name}, operator_id: 5) Export failed`,
        filename,
      ].sort(),
    );
    assertEquals(checkCampaignStub!.args[0][0], campaign1._id);
    assertEquals(checkCampaignStub!.args[1][0], campaign2._id);
  });
});
