import {
  MetadataLifetime,
  MetadataRegistryInterface,
  MetadataVariableDefinitionInterface,
  SerializedMetadataVariableDefinitionInterface,
} from "../../interfaces/index.ts";

function getMetaKey(datetime: Date, metaDefinition: MetadataVariableDefinitionInterface): string {
  if (metaDefinition.key) {
    return metaDefinition.key;
  }
  const date = typeof datetime.getMonth === "function" ? datetime : new Date(datetime);
  const [day, month, year] = [
    date.getDate(),
    date.getMonth(), // zero-based !
    date.getFullYear(),
  ];

  let keyPeriod = "global";
  let period = "campaign";
  switch (metaDefinition.lifetime) {
    case MetadataLifetime.Carpool:
      keyPeriod = "-1";
      period = "carpool";
      break;
    case MetadataLifetime.Day:
      keyPeriod = `${day}-${month}-${year}`;
      period = "day";
      break;
    case MetadataLifetime.Month:
      keyPeriod = `${month}-${year}`;
      period = "month";
      break;
    case MetadataLifetime.Year:
      keyPeriod = `${year}`;
      period = "year";
      break;
    default:
      keyPeriod = "global";
      period = "campaign";
      break;
  }

  return `${metaDefinition.name}.${metaDefinition.scope || "global"}.${period}.${keyPeriod}`;
}

export class MetadataRegistry implements MetadataRegistryInterface {
  constructor(
    public readonly policy_id: number,
    public readonly datetime: Date,
    public readonly data: Map<string, MetadataVariableDefinitionInterface> = new Map(),
  ) {}

  static create(policy_id: number, datetime: Date): MetadataRegistry {
    return new MetadataRegistry(policy_id, datetime);
  }

  static import(
    policy_id: number,
    datetime: Date,
    data: Array<MetadataVariableDefinitionInterface>,
  ): MetadataRegistry {
    const md = new Map(data.map((d) => [d.uuid, d]));
    return new MetadataRegistry(policy_id, datetime, md);
  }

  register(variable: MetadataVariableDefinitionInterface): void {
    this.data.set(variable.uuid, variable);
  }

  export(): Array<SerializedMetadataVariableDefinitionInterface> {
    return [...this.data.values()].map((d) => {
      const { uuid, lifetime, initialValue, carpoolValue } = d;
      return {
        uuid,
        lifetime,
        initialValue,
        key: getMetaKey(this.datetime, d),
        ...(carpoolValue ? { carpoolValue } : {}),
      };
    });
  }
}
