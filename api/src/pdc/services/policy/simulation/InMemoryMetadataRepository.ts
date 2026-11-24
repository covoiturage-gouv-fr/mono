import { MetadataRepositoryProviderInterfaceResolver, SerializedStoredMetadataInterface } from "../interfaces/index.ts";

/** Same lookup as `policy.policy_metas`: latest row at or before `datetime`, ties broken by insertion order. */
export class InMemoryMetadataRepository implements MetadataRepositoryProviderInterfaceResolver {
  private readonly rows: SerializedStoredMetadataInterface[] = [];

  get(policyId: number, keys: string[], datetime?: Date): Promise<SerializedStoredMetadataInterface[]> {
    const result: SerializedStoredMetadataInterface[] = [];
    for (const key of keys) {
      let best: SerializedStoredMetadataInterface | undefined;
      for (const row of this.rows) {
        if (row.policy_id !== policyId || row.key !== key) continue;
        if (datetime && row.datetime > datetime) continue;
        if (!best || row.datetime >= best.datetime) best = row;
      }
      if (best) result.push({ ...best });
    }
    return Promise.resolve(result);
  }

  set(data: SerializedStoredMetadataInterface[]): Promise<void> {
    this.rows.push(...data.map((d) => ({ ...d })));
    return Promise.resolve();
  }
}
