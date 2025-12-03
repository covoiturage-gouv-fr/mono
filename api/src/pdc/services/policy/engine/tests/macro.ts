import { assertEquals } from "../../../../../dev_deps.ts";
import { PolicyStatusEnum } from "../../contracts/common/interfaces/PolicyInterface.ts";
import {
  CarpoolInterface,
  MetadataLifetime,
  MetadataRepositoryProviderInterfaceResolver,
  PolicyHandlerInterface,
  SerializedIncentiveInterface,
  SerializedPolicyInterface,
  SerializedStoredMetadataInterface,
} from "../../interfaces/index.ts";
import { MetadataStore } from "../entities/MetadataStore.ts";
import { Policy } from "../entities/Policy.ts";
import { generateCarpool } from "./helpers.ts";

interface ProcessParams {
  carpool: Array<Partial<CarpoolInterface>>;
  handler?: PolicyHandlerInterface;
  policy?: Partial<SerializedPolicyInterface>;
  meta?: Array<{ key: string; value: bigint } & Partial<SerializedStoredMetadataInterface>>;
  lifetime?: MetadataLifetime;
}

interface ProcessResult {
  incentive: Array<number>;
  meta?: Array<{ key: string; value: bigint }>;
}

class MemoryMetadataRepository implements MetadataRepositoryProviderInterfaceResolver {
  constructor(public data: SerializedStoredMetadataInterface[] = []) {}

  async get(
    policyId: number,
    keys: string[],
    datetime?: Date,
  ): Promise<SerializedStoredMetadataInterface[]> {
    return this.data;
  }

  async set(data: SerializedStoredMetadataInterface[]): Promise<void> {
    this.data = data;
  }
}

export const makeProcessHelper = (cp?: CarpoolInterface) => {
  return async (
    input: ProcessParams,
    expected: ProcessResult,
  ) => {
    const inputMeta: Array<SerializedStoredMetadataInterface> = (input.meta || []).map((m) => ({
      datetime: new Date(),
      policy_id: 1,
      ...m,
    }));
    const carpools = input.carpool.map((c) => generateCarpool(c, cp));
    const [start_date, end_date] = carpools
      .map((c) => c.datetime)
      .reduce(
        (
          [oldest, newest],
          i,
        ) => [i < oldest ? i : oldest, i > newest ? i : newest],
        [new Date(), new Date()],
      );

    const policyDef: SerializedPolicyInterface = {
      start_date,
      end_date,
      _id: 1,
      territory_id: 1,
      territory_selector: {
        country: ["XXXXX"],
      },
      name: "",
      status: PolicyStatusEnum.ACTIVE,
      tz: "Europe/Paris",
      handler: "",
      incentive_sum: 0n,
      max_amount: 10_000_000_00n,
      ...(input.policy || {}),
    };

    const policy = input.handler
      ? new Policy(
        policyDef._id,
        policyDef.territory_id,
        policyDef.territory_selector,
        policyDef.name,
        policyDef.start_date,
        policyDef.end_date,
        policyDef.tz,
        input.handler,
        policyDef.status,
        policyDef.incentive_sum,
      )
      : await Policy.import(policyDef);
    const store = new MetadataStore(new MemoryMetadataRepository(inputMeta));
    const incentives: SerializedIncentiveInterface[] = [];
    for (const carpool of carpools) {
      const statelessIncentive = await policy.processStateless(carpool);
      const statefulIncentive = await policy.processStateful(
        store,
        statelessIncentive.export(),
      );
      incentives.push(statefulIncentive.export());
    }

    assertEquals(
      incentives.map((i) => i.statefulAmount),
      expected.incentive,
    );

    if (expected.meta) {
      assertEquals(
        (await store.store(input.lifetime || MetadataLifetime.Day))
          .map((m) => ({ key: m.key, value: m.value })),
        expected.meta.map((m) => ({ key: m.key, value: m.value })),
      );
    }
  };
};

export const process = makeProcessHelper();
