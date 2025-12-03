import {
  MetadataAccessorInterface,
  MetadataLifetime,
  MetadataRegistryInterface,
  MetadataRepositoryProviderInterfaceResolver,
  MetadataStoreInterface,
  SerializedAccessibleMetadataInterface,
  SerializedMetadataVariableDefinitionInterface,
  SerializedStoredMetadataInterface,
  StoredMetadataVariableInterface,
} from "../../interfaces/index.ts";
import { UnknownMetaException } from "../exceptions/UnknownMetaException.ts";
import { MetadataAccessor } from "./MetadataAccessor.ts";

function getCacheKey(policy_id: number, key: string): string {
  return `${policy_id.toString()}-${key}`;
}
function getDefaultVariableDefinition(
  definition: SerializedMetadataVariableDefinitionInterface,
): SerializedMetadataVariableDefinitionInterface {
  return { initialValue: 0n, lifetime: MetadataLifetime.Always, ...definition };
}

export class MetadataStore implements MetadataStoreInterface {
  public cache: Map<string, StoredMetadataVariableInterface> = new Map();

  constructor(
    protected readonly repository?: MetadataRepositoryProviderInterfaceResolver,
  ) {}

  async load(registry: MetadataRegistryInterface): Promise<MetadataAccessorInterface> {
    const variables = registry.export();
    const keys = variables.map((v) => v.key);

    if (keys.length === 0) {
      return MetadataAccessor.import(registry.datetime, new Map());
    }

    const keysToQuery = keys.filter((k) => !this.cache.has(getCacheKey(registry.policy_id, k)));
    const missingData = (await this.repository?.get(
      registry.policy_id,
      keysToQuery,
      registry.datetime,
    )) || [];
    for (const key of keysToQuery) {
      const variableDefinition = getDefaultVariableDefinition(variables.find((v) => v.key === key)!);
      const value = missingData.find((k) => k.key === key)?.value || variableDefinition.initialValue || 0;
      this.cache.set(getCacheKey(registry.policy_id, key), {
        ...variableDefinition,
        policy_id: registry.policy_id,
        datetime: registry.datetime,
        value: BigInt(value),
      });
    }
    const data = variables
      .map((v) => [v, this.cache.get(getCacheKey(registry.policy_id, v.key))])
      .reduce(
        (
          m,
          [v, i]: [
            SerializedMetadataVariableDefinitionInterface,
            StoredMetadataVariableInterface,
          ],
        ) => {
          m.set(v.uuid, {
            policy_id: i.policy_id,
            key: i.key,
            value: i.value,
            ...(v.carpoolValue ? { carpoolValue: v.carpoolValue } : {}),
          });
          return m;
        },
        new Map<string, SerializedAccessibleMetadataInterface>(),
      );

    return MetadataAccessor.import(registry.datetime, data);
  }

  async save(dataAccessor: MetadataAccessorInterface): Promise<void> {
    const data = dataAccessor.export();
    for (const { key, value, policy_id } of data) {
      const cacheKey = getCacheKey(policy_id, key);
      const oldData = this.cache.get(cacheKey);
      if (!oldData) {
        throw new UnknownMetaException();
      }
      this.cache.set(cacheKey, { ...oldData, value });
    }
  }

  async store(lifetime: MetadataLifetime): Promise<Array<SerializedStoredMetadataInterface>> {
    const dataToSave: Array<SerializedStoredMetadataInterface> = [];
    for (const uuid of this.cache.keys()) {
      const data = this.cache.get(uuid)!;
      if (data.lifetime! > lifetime) {
        dataToSave.push({
          policy_id: data.policy_id,
          datetime: data.datetime,
          key: data.key,
          value: data.value,
        });
      }
    }
    await this.repository?.set(dataToSave);
    return dataToSave;
  }
}
