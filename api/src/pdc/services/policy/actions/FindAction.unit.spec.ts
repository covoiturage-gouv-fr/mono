import { assertEquals } from "dep:assert";
import { beforeEach, describe, it } from "dep:testing-bdd";
import { ContextType, KernelInterfaceResolver } from "@/ilos/common/index.ts";
import sinon, { SinonStub } from "dep:sinon";
import { ResultInterface as OperatorResultInterface } from "../../operator/contracts/find.contract.ts";
import { PolicyStatusEnum } from "../contracts/common/interfaces/PolicyInterface.ts";
import { ParamsInterface, ResultInterface } from "../contracts/find.contract.ts";
import {
  OperatorsEnum,
  PolicyRepositoryProviderInterfaceResolver,
  SerializedPolicyInterface,
} from "../interfaces/index.ts";
import { FindAction } from "./FindAction.ts";

describe("FindAction", () => {
  // injected objects
  let fakeKernelInterfaceResolver: KernelInterfaceResolver;
  let repository: PolicyRepositoryProviderInterfaceResolver;

  // tested
  let findAction: FindAction;

  // sinon stubs
  let kernelInterfaceResolverStub: SinonStub<
    [method: string, params: any, context: ContextType]
  >;
  let repositoryStub: SinonStub<
    [method: string, params: any, context: ContextType]
  >;

  class FakePolicyRepositoryProvider extends PolicyRepositoryProviderInterfaceResolver {
    override updateDescriptiveSheetUrl(_id: number, _descriptiveSheetUrl: string | null): Promise<void> {
      throw new Error("Method not implemented.");
    }
    find(_id: number, _territoryId?: number): Promise<SerializedPolicyInterface | undefined> {
      throw new Error("Method not implemented.");
    }
    create(_data: Omit<SerializedPolicyInterface, "_id">): Promise<SerializedPolicyInterface> {
      throw new Error("Method not implemented.");
    }
    patch(_data: SerializedPolicyInterface): Promise<SerializedPolicyInterface> {
      throw new Error("Method not implemented.");
    }
    delete(_id: number): Promise<void> {
      throw new Error("Method not implemented.");
    }
    findWhere(
      _search: {
        _id?: number;
        territory_id?: number | null | number[];
        status?: PolicyStatusEnum;
        datetime?: Date;
        ends_in_the_future?: boolean;
      },
    ): Promise<SerializedPolicyInterface[]> {
      throw new Error("Method not implemented.");
    }
    listApplicablePoliciesId(): Promise<number[]> {
      throw new Error("Method not implemented.");
    }
    activeOperators(_policy_id: number): Promise<number[]> {
      throw new Error("Method not implemented.");
    }
    syncIncentiveSum(_campaign_id: number): Promise<void> {
      throw new Error("Method not implemented.");
    }
    updateAllCampaignStatuses(): Promise<void> {
      throw new Error("Method not implemented.");
    }
  }

  beforeEach(() => {
    // object
    fakeKernelInterfaceResolver = new (class extends KernelInterfaceResolver {})();
    repository = new FakePolicyRepositoryProvider();

    // tested
    findAction = new FindAction(
      fakeKernelInterfaceResolver,
      repository,
    );

    //strub
    kernelInterfaceResolverStub = sinon.stub(
      fakeKernelInterfaceResolver,
      "call",
    );
    repositoryStub = sinon.stub(
      repository,
      "find",
    );
  });

  it("(policy) FindAction: should return policy for operator even if removed from current running policy rules", async () => {
    // Arrange
    const params: ParamsInterface = {
      _id: 1017,
    };
    const policies: SerializedPolicyInterface = {
      _id: 1017,
      territory_id: 335,
      territory_selector: {},
      name: "Pays de la Loire 2024",
      start_date: new Date(),
      end_date: new Date(),
      tz: "Europe/Paris",
      handler: "pdll_2024",
      status: PolicyStatusEnum.ACTIVE,
      incentive_sum: 100,
    };
    const operator: OperatorResultInterface = {
      name: "Klaxit",
      legal_name: "Klaxit",
      siret: "",
      uuid: OperatorsEnum.KLAXIT,
    };

    repositoryStub.resolves(policies);
    kernelInterfaceResolverStub.resolves(operator);

    // Act
    const result: ResultInterface = await findAction.handle(params);

    // Assert
    assertEquals(result._id, 1017);
  });

  it("(policy) FindAction: should work even if allTimeOperator is not defined", async () => {
    // Arrange
    const params: ParamsInterface = {
      _id: 1017,
    };
    const policies: SerializedPolicyInterface = {
      _id: 99,
      territory_id: 598,
      territory_selector: {},
      name: "Pole Métropolitain du Genevois 2024",
      start_date: new Date(),
      end_date: new Date(),
      tz: "Europe/Paris",
      handler: "pmgf_late_2023",
      status: PolicyStatusEnum.FINISHED,
      incentive_sum: 100,
    };

    const operator: OperatorResultInterface = {
      name: "Klaxit",
      legal_name: "Klaxit",
      siret: "",
      uuid: OperatorsEnum.KLAXIT,
    };

    repositoryStub.resolves(policies);
    kernelInterfaceResolverStub.resolves(operator);

    // Act
    const result: ResultInterface = await findAction.handle(params);

    // Assert
    assertEquals(result._id, 99);
  });
});
