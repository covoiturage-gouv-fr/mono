import {
  command,
  CommandInterface,
  ContextType,
  KernelInterfaceResolver,
  ResultType,
} from "../../../../ilos/common/index.ts";
import { logger } from "../../../../lib/logger/index.ts";
import { Timezone } from "../../../providers/validator/index.ts";
import { ParamsInterface, signature as finalize } from "../contracts/finalize.contract.ts";
import { castUserStringToUTC, toISOString } from "../helpers/index.ts";

interface CommandOptions {
  from: string;
  to: string;
  tz: Timezone;
  resync: boolean;
  clear: boolean;
}

@command({
  signature: "campaign:finalize",
  description: "Finalize stateful campaign rules",
  options: [
    {
      signature: "-f, --from <from>",
      description: "from date <YYYY-MM-DD>",
    },
    {
      signature: "-t, --to <to>",
      description: "to date <YYYY-MM-DD>",
    },
    {
      signature: "--tz <tz>",
      description: "timezone",
      default: "Europe/Paris",
    },
    {
      signature: "--resync",
      description: "resync the max_amount_restriction keys to incentive_sum",
      default: false,
    },
  ],
})
export class FinalizeCommand implements CommandInterface {
  constructor(protected kernel: KernelInterfaceResolver) {}

  public async call(options: CommandOptions): Promise<ResultType> {
    try {
      const { tz, resync: sync_incentive_sum } = options;
      const context: ContextType = { channel: { service: "campaign" } };
      const params: ParamsInterface = { tz, sync_incentive_sum };

      if ("from" in options && options.from) {
        const from = castUserStringToUTC(options.from, tz);
        if (from) params.from = toISOString(from);
      }

      if ("to" in options && options.to) {
        const to = castUserStringToUTC(options.to, tz);
        if (to) params.to = toISOString(to);
      }

      await this.kernel.call(finalize, params, context);

      return "";
    } catch (e) {
      logger.error(e.rpcError?.data || e.message);
    }
  }
}
