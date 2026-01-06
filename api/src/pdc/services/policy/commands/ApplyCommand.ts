import { command, CommandInterface, ContextType, KernelInterfaceResolver, ResultType } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { get } from "@/lib/object/index.ts";
import { CampaignRepository } from "@/pdc/providers/campaign/repositories/CampaignRepository.ts";
import { Timezone } from "@/pdc/providers/validator/index.ts";
import { signature as apply } from "../contracts/apply.contract.ts";
import { castUserStringToUTC, subDaysTz, today, toISOString } from "../helpers/index.ts";
import { PolicyRepositoryProviderInterfaceResolver } from "../interfaces/index.ts";

interface CommandOptions {
  campaigns: number[];
  from: string;
  to: string;
  tz: Timezone;
  override: boolean;
}

@command({
  signature: "campaign:apply",
  description: "Apply stateless campaign rules",
  options: [
    {
      signature: "-c, --campaigns <campaigns>",
      description: "list of campaign_id",
      default: [],
      coerce(s: string): number[] {
        return s
          .split(",")
          .map((i: string): number => parseInt(i, 10))
          .filter((i: number): boolean => !!i);
      },
    },
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
      signature: "--override",
      description: "override existing incentives",
      default: false,
    },
  ],
})
export class ApplyCommand implements CommandInterface {
  constructor(
    protected kernel: KernelInterfaceResolver,
    private policyRepository: PolicyRepositoryProviderInterfaceResolver,
    private campaignRepository: CampaignRepository,
  ) {}

  public async call(options: CommandOptions): Promise<ResultType> {
    try {
      const { tz, override } = options;

      // cast or default from/to dates
      const from = options.from && castUserStringToUTC(options.from, tz) || subDaysTz(today(tz), 7);
      const to = options.to && castUserStringToUTC(options.to, tz) || today(tz);

      // update all campaign statuses
      await this.policyRepository.updateAllCampaignStatuses();

      // list campaigns
      const campaigns = options.campaigns.length
        ? options.campaigns
        : (await this.campaignRepository.findByRange(from, to));

      for (const policy_id of campaigns) {
        const context: ContextType = { channel: { service: "campaign" } };

        // configure params to pass schema validation
        const params: { policy_id: number } & Partial<CommandOptions> = {
          policy_id,
          tz,
          override,
        };

        if (from) params.from = toISOString(from);
        if (to) params.to = toISOString(to);

        // warn the user if 'override' and 'to' are used together
        // as the processed finalized incentives might affect the result
        // of further incentives.
        if (params.override && params.to) {
          logger.warn(
            `[campaign:apply] Be careful when re-processing incentives with the --override option in conjunction ` +
              `with a --to end date. Further incentives will have to be re-processed and finalized too.`,
          );
        }

        // call the action
        try {
          await this.kernel.call(apply, params, context);
        } catch (e) {
          const message = Error.isError(e) ? e.message : "Unknown error";
          logger.error(message, { params });
        }
      }

      return "";
    } catch (e) {
      const message = Error.isError(e) ? e.message : "Unknown error";
      logger.error(get(e, "rpcError.data", message));
    }
  }
}
