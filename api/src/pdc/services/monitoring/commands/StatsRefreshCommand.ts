import { command, CommandInterface, ContextType, KernelInterfaceResolver, ResultType } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { signature } from "../contracts/statsrefresh.contract.ts";

interface CommandOptions {
  schema: string;
}

@command({
  signature: "monitoring:stats:refresh",
  description: "Refresh stats materialized views",
  options: [
    {
      signature: "-s, --schema <schema>",
      description: "DB schema to refresh",
      default: "stats",
    },
  ],
})
export class StatsRefreshCommand implements CommandInterface {
  constructor(protected kernel: KernelInterfaceResolver) {}

  public async call({ schema }: CommandOptions): Promise<ResultType> {
    const context: ContextType = {
      // Canal dédié : l'action refuse désormais le canal proxy (voir StatsRefreshAction).
      channel: { service: "monitoring" },
      call: { user: {} },
    };

    logger.info(`Running [monitoring:stats:refresh] for schema ${schema}`);
    await this.kernel.call(signature, { schema }, context);

    return "";
  }
}
