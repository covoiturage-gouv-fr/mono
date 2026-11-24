import {
  IncentiveRepositoryProviderInterfaceResolver,
  IncentiveStateEnum,
  IncentiveStatusEnum,
  SerializedIncentiveInterface,
} from "../interfaces/index.ts";

type Row = SerializedIncentiveInterface<number>;

const keyOf = (i: { policy_id: number; operator_id: number; operator_journey_id: string }) =>
  `${i.policy_id}/${i.operator_id}/${i.operator_journey_id}`;

/**
 * Mirrors `policy.incentives` for the dry-run: same upsert key, status/state transitions
 * and draft ordering as IncentiveRepositoryProvider. Nothing leaves the process.
 */
export class InMemoryIncentiveRepository implements IncentiveRepositoryProviderInterfaceResolver {
  private readonly rows = new Map<string, Row>();
  private nextId = 1;

  /** @param excluded `operator_id/operator_journey_id` of carpools canceled or failing anomaly/fraud checks */
  constructor(private readonly excluded: ReadonlySet<string> = new Set()) {}

  all(): Row[] {
    return [...this.rows.values()];
  }

  createOrUpdateMany(data: SerializedIncentiveInterface<undefined>[]): Promise<void> {
    for (const i of data) {
      const key = keyOf(i);
      const meta = structuredClone(i.meta || []);
      const state = i.statefulAmount === 0 ? IncentiveStateEnum.Null : IncentiveStateEnum.Regular;
      const status = i.status || IncentiveStatusEnum.Draft;
      const existing = this.rows.get(key);
      if (existing) {
        Object.assign(existing, {
          statelessAmount: i.statelessAmount,
          statefulAmount: i.statefulAmount,
          status,
          state,
          meta,
        });
      } else {
        this.rows.set(key, { ...i, _id: this.nextId++, status, state, meta });
      }
    }
    return Promise.resolve();
  }

  disableOnExcludedCarpool(from: Date, to: Date): Promise<void> {
    for (const row of this.rows.values()) {
      if (row.datetime < from || row.datetime >= to) continue;
      if (this.excluded.has(`${row.operator_id}/${row.operator_journey_id}`)) {
        row.state = IncentiveStateEnum.Disabled;
        row.status = IncentiveStatusEnum.Error;
      }
    }
    return Promise.resolve();
  }

  setStatus(from: Date, to: Date, hasFailed = false): Promise<void> {
    const next = hasFailed ? IncentiveStatusEnum.Draft : IncentiveStatusEnum.Validated;
    for (const row of this.rows.values()) {
      if (row.datetime >= from && row.datetime < to && row.status === IncentiveStatusEnum.Pending) {
        row.status = next;
      }
    }
    return Promise.resolve();
  }

  updateStatefulAmount(data: SerializedIncentiveInterface<number>[], status?: IncentiveStatusEnum): Promise<void> {
    const byId = new Map([...this.rows.values()].map((r) => [r._id, r]));
    for (const i of data) {
      const row = byId.get(i._id);
      if (!row) continue;
      row.statefulAmount = i.statefulAmount;
      row.status = status ?? i.status;
      if (i.statefulAmount === 0) row.state = IncentiveStateEnum.Null;
    }
    return Promise.resolve();
  }

  // deno-lint-ignore require-await
  async *findDraftIncentive(to: Date, batchSize = 100, from?: Date): AsyncGenerator<Row[], void, void> {
    const drafts = [...this.rows.values()]
      .filter((r) => r.status === IncentiveStatusEnum.Draft && (!from || r.datetime >= from) && r.datetime <= to)
      .sort((a, b) => a.datetime.getTime() - b.datetime.getTime() || a._id - b._id)
      // copies, like a DB read: the caller works on what it receives
      .map((r) => structuredClone(r));

    for (let i = 0; i < drafts.length; i += batchSize) {
      yield drafts.slice(i, i + batchSize);
    }
  }

  latestDraft(): Promise<Date> {
    throw new Error("not used by the dry-run");
  }

  updateIncentiveSum(): Promise<void> {
    throw new Error("not used by the dry-run");
  }
}
