import { logger } from "../../../../../lib/logger/index.ts";
import { MetadataAccessorInterface, SerializedAccessibleMetadataInterface } from "../../interfaces/index.ts";
import { UnknownMetaException } from "../exceptions/UnknownMetaException.ts";

export class MetadataAccessor implements MetadataAccessorInterface {
  constructor(
    public readonly datetime: Date,
    public readonly data: Map<string, SerializedAccessibleMetadataInterface> = new Map(),
  ) {}

  static import(
    datetime: Date,
    data: Map<string, SerializedAccessibleMetadataInterface>,
  ): MetadataAccessor {
    return new MetadataAccessor(datetime, data);
  }

  export(): Array<SerializedAccessibleMetadataInterface> {
    return [...this.data.values()];
  }

  get(uuid: string): number {
    const meta = this.data.get(uuid);
    if (!meta || !("value" in meta)) {
      logger.error(`key ${uuid} not found in [${[...this.data.keys()].join(", ")}] (${JSON.stringify(meta)})`);
      throw new UnknownMetaException(`${uuid} not found`);
    }
    return meta.value;
  }

  getRaw(uuid: string): SerializedAccessibleMetadataInterface {
    return this.data.get(uuid)!;
  }

  set(uuid: string, value: number): void {
    const data = this.getRaw(uuid);
    this.data.set(uuid, { ...data, value });
  }

  isEmpty(): boolean {
    return this.data.size === 0;
  }
}
