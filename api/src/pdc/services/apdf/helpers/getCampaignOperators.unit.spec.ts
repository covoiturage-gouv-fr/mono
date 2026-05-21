import { assertEquals, assertRejects } from "dep:assert";
import { afterEach, beforeEach, describe, it } from "dep:testing-bdd";
import { KernelInterfaceResolver, NotFoundException } from "@/ilos/common/index.ts";
import sinon from "dep:sinon";
import {
  ParamsInterface as FindByUuidParams,
  ResultInterface as FindByUuidResult,
} from "../../operator/contracts/findbyuuid.contract.ts";
import { PolicyStatusEnum } from "../../policy/contracts/common/interfaces/PolicyInterface.ts";
import { ResultInterface as PolicyFindResult } from "../../policy/contracts/find.contract.ts";
import { getCampaignOperators, getPolicyUuidList, uuidToOperatorId } from "./getCampaignOperators.helper.ts";

describe("getCampaignOperator", () => {
  const kernel = new (class extends KernelInterfaceResolver {})();
  let kernelStub: any;
  beforeEach(() => {
    kernelStub = sinon.stub(kernel, "call");
  });

  afterEach(() => {
    kernelStub.restore();
  });

  it("getPolicyUuidList: success", async () => {
    const response: PolicyFindResult = fakePolicy(1, [
      "12345678900001",
      "98765432100001",
    ]);
    kernelStub.resolves(response);
    const result = await getPolicyUuidList(kernel, "apdf", 1);
    assertEquals(result, ["12345678900001", "98765432100001"]);
  });

  it("getPolicyUuidList: no policy found", async () => {
    const message = "policy:find not found";
    kernelStub.throws(new NotFoundException(message));
    await assertRejects(async () => getPolicyUuidList(kernel, "apdf", 1));
  });

  it("getPolicyUuidList: no operators", async () => {
    const response: PolicyFindResult = fakePolicy(1, []);
    kernelStub.resolves(response);
    await assertRejects(
      async () => getPolicyUuidList(kernel, "apdf", 1),
      "No UUID declared in policy 1",
    );
  });

  it("uuidToOperatorId: success", async () => {
    const policy = fakePolicy(1, ["12345678900001", "98765432100001"]);
    const query: FindByUuidParams["uuid"] = policy.params.operators || [];
    const response: FindByUuidResult = [
      { _id: 1, uuid: "12345678900001" },
      { _id: 2, uuid: "98765432100001" },
    ];
    kernelStub.resolves(response);
    const result = await uuidToOperatorId(kernel, "apdf", query);
    assertEquals(result, [1, 2]);
  });

  it("uuidToOperatorId: empty", async () => {
    const policy = fakePolicy(1, []);
    const query: FindByUuidParams["uuid"] = policy.params.operators || [];
    const response: FindByUuidResult = [];
    kernelStub.resolves(response);
    const result = await uuidToOperatorId(kernel, "apdf", query);
    assertEquals(result, []);
  });

  it("uuidToOperatorId: no matching UUID", async () => {
    const policy = fakePolicy(1, ["12345678900001", "98765432100001"]);
    const query: FindByUuidParams["uuid"] = policy.params.operators || [];
    const response: FindByUuidResult = [];
    kernelStub.resolves(response);
    const result = await uuidToOperatorId(kernel, "apdf", query);
    assertEquals(result, []);
  });

  it("getDeclaredOperators: success", async () => {
    const policy = fakePolicy(1, ["12345678900001", "98765432100001"]);
    const operators = [
      { _id: 1, uuid: "12345678900001" },
      { _id: 2, uuid: "98765432100001" },
    ];

    kernelStub.onFirstCall().resolves(policy);
    kernelStub.onSecondCall().resolves(operators);

    const result = await getCampaignOperators(
      kernel,
      "apdf",
      policy._id,
    );
    assertEquals(result, [1, 2]);
  });

  it("getDeclaredOperators: no matching UUID", async () => {
    const policy = fakePolicy(1, ["12345678900001", "98765432100001"]);

    kernelStub.onFirstCall().resolves(policy);
    kernelStub.onSecondCall().resolves([]);

    const result = await getCampaignOperators(
      kernel,
      "apdf",
      policy._id,
    );
    assertEquals(result, []);
  });

  it("getDeclaredOperators: empty", async () => {
    const policy = fakePolicy(1, []);

    kernelStub.onFirstCall().resolves(policy);
    kernelStub.onSecondCall().resolves([]);

    const result = await getCampaignOperators(
      kernel,
      "apdf",
      policy._id,
    );
    assertEquals(result, []);
  });

  function fakePolicy(id: number, operators: string[]): PolicyFindResult {
    return {
      _id: id,
      territory_id: id,
      name: `Policy ${id}`,
      description: `Description ${id}`,
      start_date: new Date("2020-01-08T00:00:00Z"),
      end_date: new Date("2020-02-08T00:00:00Z"),
      status: PolicyStatusEnum.ACTIVE,
      handler: `handler_${id}`,
      incentive_sum: 100,
      params: {
        operators,
      },
    };
  }
});
