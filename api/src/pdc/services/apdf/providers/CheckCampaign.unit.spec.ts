import { afterEach, assert, assertRejects, beforeEach, describe, it } from "@/dev_deps.ts";
import { ContextType, KernelInterfaceResolver, NotFoundException } from "@/ilos/common/index.ts";
import { PolicyStatusEnum } from "@/pdc/services/policy/contracts/common/interfaces/PolicyInterface.ts";
import { ResultInterface as GetCampaignResultInterface } from "@/pdc/services/policy/contracts/find.contract.ts";
import { faker } from "dep:faker";
import sinon from "dep:sinon";
import { createGetCampaignResult } from "../helpers/createGetCampaignResult.helper.ts";
import { CheckCampaign } from "./CheckCampaign.ts";

describe("CheckCampaign", () => {
  let kernelInterfaceResolverStub: sinon.SinonStub<[method: string, params: any, context: ContextType]>;
  const CAMPAIGN_NAME = faker.word.noun();
  const kernelInterfaceResolver = new (class extends KernelInterfaceResolver {})();
  const checkCampaign = new CheckCampaign(
    kernelInterfaceResolver,
  );

  beforeEach(() => {
    kernelInterfaceResolverStub = sinon.stub(
      kernelInterfaceResolver,
      "call",
    );
  });

  afterEach(() => {
    kernelInterfaceResolverStub.restore();
  });

  const successStubArrange = (
    operator_ids: number[],
  ): GetCampaignResultInterface => {
    const campaign: GetCampaignResultInterface = createGetCampaignResult(
      PolicyStatusEnum.ACTIVE,
      CAMPAIGN_NAME,
      new Date(new Date().getTime() - 1 * 365 * 24 * 60 * 60 * 1000),
      new Date(new Date().getTime() + 1 * 365 * 24 * 60 * 60 * 1000),
      operator_ids,
    );

    kernelInterfaceResolverStub.resolves(campaign);
    return campaign;
  };

  // eslint-disable-next-line max-len
  it("GetCampaignAndCallBuildExcel: should campaign be valid if provided dates are in date range and one operator", async () => {
    // Arrange
    const campaign: GetCampaignResultInterface = successStubArrange([
      5,
    ]);

    const startOfMonth: Date = new Date();
    startOfMonth.setDate(1);
    startOfMonth.setMonth(startOfMonth.getMonth() - 1);

    const endOfMonth: Date = new Date(
      startOfMonth.getFullYear(),
      startOfMonth.getMonth() + 1,
      0,
    );

    // Act
    await checkCampaign.call(campaign._id, startOfMonth, endOfMonth);

    // Assert
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
    assert(true);
  });

  // eslint-disable-next-line max-len
  it("GetCampaignAndCallBuildExcel: should campaign be valid provided dates intersect range and 2 operators", async () => {
    // Arrange
    const operator_ids = [5, 6];
    const campaign: GetCampaignResultInterface = successStubArrange(
      operator_ids,
    );

    const todayMinus3Years: Date = new Date();
    todayMinus3Years.setFullYear(todayMinus3Years.getFullYear() - 3);

    const todayPlus1Year: Date = new Date();
    todayPlus1Year.setFullYear(todayPlus1Year.getFullYear() + 1);

    // Act
    await checkCampaign.call(
      campaign._id,
      todayMinus3Years,
      todayPlus1Year,
    );

    // Assert
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
    assert(true);
  });

  // eslint-disable-next-line max-len
  it("GetCampaignAndCallBuildExcel: should campaign be valid if dates are in larger date range and 1 operator", async () => {
    // Arrange
    const campaign: GetCampaignResultInterface = successStubArrange([
      5,
    ]);

    const todayMinus3Years: Date = new Date();
    todayMinus3Years.setFullYear(todayMinus3Years.getFullYear() - 3);

    const todayPlus3Years: Date = new Date();
    todayPlus3Years.setFullYear(todayPlus3Years.getFullYear() + 3);

    // Act
    await checkCampaign.call(
      campaign._id,
      todayMinus3Years,
      todayPlus3Years,
    );

    // Assert
    assert(true);
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
  });

  // eslint-disable-next-line max-len
  it("GetCampaignAndCallBuildExcel: should campaign be valid if dates are in larger date range and no operator whitelist", async () => {
    // Arrange
    const campaign: GetCampaignResultInterface = successStubArrange(
      [],
    );

    const todayMinus3Years: Date = new Date();
    todayMinus3Years.setFullYear(todayMinus3Years.getFullYear() - 3);

    const todayPlus3Years: Date = new Date();
    todayPlus3Years.setFullYear(todayPlus3Years.getFullYear() + 3);

    // Act
    await checkCampaign.call(
      campaign._id,
      todayMinus3Years,
      todayPlus3Years,
    );

    // Assert
    assert(true);
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
  });

  it("GetCampaignAndCallBuildExcel: should throw NotFoundException if no campaign with id", async () => {
    // Arrange
    kernelInterfaceResolverStub.rejects(new NotFoundException());

    // Act
    await assertRejects(async () => {
      await checkCampaign.call(
        faker.number.int(),
        new Date(),
        new Date(),
      );
    });

    // Assert
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
  });

  it("GetCampaignAndCallBuildExcel: should throw Error if draft campaign", async () => {
    // Arrange
    kernelInterfaceResolverStub.resolves(
      createGetCampaignResult(PolicyStatusEnum.DRAFT, CAMPAIGN_NAME),
    );

    // Act
    await assertRejects(async () => {
      await checkCampaign.call(
        faker.number.int(),
        new Date(),
        new Date(),
      );
    });

    // Assert
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
  });

  it("GetCampaignAndCallBuildExcel: should throw Error if campaign dates are not in date range", async () => {
    // Arrange
    kernelInterfaceResolverStub.resolves(
      createGetCampaignResult(
        PolicyStatusEnum.ACTIVE,
        CAMPAIGN_NAME,
        new Date(new Date().getTime() - 1 * 365 * 24 * 60 * 60 * 1000),
        new Date(new Date().getTime() + 1 * 365 * 24 * 60 * 60 * 1000),
      ),
    );

    const todayMinus3Years: Date = new Date();
    todayMinus3Years.setFullYear(todayMinus3Years.getFullYear() - 3);

    const todayMinus2Years: Date = new Date();
    todayMinus2Years.setFullYear(todayMinus2Years.getFullYear() - 2);

    // Act
    await assertRejects(async () => {
      await checkCampaign.call(
        faker.number.int(),
        todayMinus3Years,
        todayMinus2Years,
      );
    });

    // Assert
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
  });

  it("isValidDateRange: lower = start. end = upper", async () => {
    // Arrange
    const campaign = createGetCampaignResult(
      PolicyStatusEnum.ACTIVE,
      CAMPAIGN_NAME,
      new Date("2022-01-01T00:00:00+0100"),
      new Date("2023-01-01T00:00:00+0100"),
    );
    kernelInterfaceResolverStub.resolves(campaign);

    // Act
    await checkCampaign.call(
      campaign._id,
      new Date("2022-01-01T00:00:00+0100"),
      new Date("2023-01-01T00:00:00+0100"),
    );

    // Assert
    assert(true);
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
  });

  it("isValidDateRange: lower = start. end < upper", async () => {
    // Arrange
    const campaign = createGetCampaignResult(
      PolicyStatusEnum.ACTIVE,
      CAMPAIGN_NAME,
      new Date("2022-01-01T00:00:00+0100"),
      new Date("2023-01-01T00:00:00+0100"),
    );
    kernelInterfaceResolverStub.resolves(campaign);

    // Act
    await checkCampaign.call(
      campaign._id,
      new Date("2022-01-01T00:00:00+0100"),
      new Date("2022-02-01T00:00:00+0100"),
    );

    // Assert
    assert(true);
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
  });

  it("isValidDateRange: lower < start. end = upper", async () => {
    // Arrange
    const campaign = createGetCampaignResult(
      PolicyStatusEnum.ACTIVE,
      CAMPAIGN_NAME,
      new Date("2022-01-01T00:00:00+0100"),
      new Date("2023-01-01T00:00:00+0100"),
    );
    kernelInterfaceResolverStub.resolves(campaign);

    // Act
    await checkCampaign.call(
      campaign._id,
      new Date("2022-02-01T00:00:00+0100"),
      new Date("2023-01-01T00:00:00+0100"),
    );

    // Assert
    assert(true);
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
  });

  it("isValidDateRange: lower > start. end < upper", async () => {
    // Arrange
    const campaign = createGetCampaignResult(
      PolicyStatusEnum.ACTIVE,
      CAMPAIGN_NAME,
      new Date("2022-01-01T00:00:00+0100"),
      new Date("2023-01-01T00:00:00+0100"),
    );
    kernelInterfaceResolverStub.resolves(campaign);

    // Act
    await checkCampaign.call(
      campaign._id,
      new Date("2021-12-01T00:00:00+0100"),
      new Date("2022-02-01T00:00:00+0100"),
    );

    // Assert
    assert(true);
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
  });

  it("isValidDateRange: lower < start. end > upper", async () => {
    // Arrange
    const campaign = createGetCampaignResult(
      PolicyStatusEnum.ACTIVE,
      CAMPAIGN_NAME,
      new Date("2022-01-01T00:00:00+0100"),
      new Date("2023-01-01T00:00:00+0100"),
    );
    kernelInterfaceResolverStub.resolves(campaign);

    // Act
    await checkCampaign.call(
      campaign._id,
      new Date("2022-12-01T00:00:00+0100"),
      new Date("2023-02-01T00:00:00+0100"),
    );

    // Assert
    assert(true);
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
  });

  it("isValidDateRange: lower < start. end < upper", async () => {
    // Arrange
    const campaign = createGetCampaignResult(
      PolicyStatusEnum.ACTIVE,
      CAMPAIGN_NAME,
      new Date("2022-01-01T00:00:00+0100"),
      new Date("2023-01-01T00:00:00+0100"),
    );
    kernelInterfaceResolverStub.resolves(campaign);

    // Act
    await checkCampaign.call(
      campaign._id,
      new Date("2022-12-01T00:00:00+0100"),
      new Date("2023-02-01T00:00:00+0100"),
    );

    // Assert
    assert(true);
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
  });

  it("isValidDateRange: lower > start. end > upper", async () => {
    // Arrange
    const campaign = createGetCampaignResult(
      PolicyStatusEnum.ACTIVE,
      CAMPAIGN_NAME,
      new Date("2022-01-01T00:00:00+0100"),
      new Date("2023-01-01T00:00:00+0100"),
    );
    kernelInterfaceResolverStub.resolves(campaign);

    // Act
    await checkCampaign.call(
      campaign._id,
      new Date("2021-12-01T00:00:00+0100"),
      new Date("2023-02-01T00:00:00+0100"),
    );

    // Assert
    assert(true);
    sinon.assert.calledOnce(kernelInterfaceResolverStub);
  });
});
