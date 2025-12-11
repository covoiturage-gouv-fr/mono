import type { RegisterHookInterface, ServiceContainerInterface } from "@/ilos/common/index.ts";
import { ConfigInterfaceResolver, extension } from "@/ilos/common/index.ts";
import { get } from "@/lib/object/index.ts";

export class ConfigStore extends ConfigInterfaceResolver {
  constructor(protected config: { [k: string]: any }) {
    super();
  }

  override get(key: string, fallback?: any): any {
    if (fallback === undefined && get(this.config, key) === undefined) {
      throw new Error(`Unknown config key '${key}'`);
    }

    return get(this.config, key, fallback);
  }
}

@extension({
  name: "config",
  autoload: true,
})
export class Config implements RegisterHookInterface {
  constructor(protected readonly params: { [k: string]: any } = {}) {}

  async register(serviceContainer: ServiceContainerInterface) {
    serviceContainer.getContainer().bind(ConfigInterfaceResolver)
      .toConstantValue(new ConfigStore(this.params));
  }
}
