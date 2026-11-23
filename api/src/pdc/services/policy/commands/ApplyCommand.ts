import { command, CommandInterface, ContextType, KernelInterfaceResolver, ResultType } from "@/ilos/common/index.ts";
import { taggedLogger } from "@/lib/logger/tagged.ts";
import { get } from "@/lib/object/index.ts";
import { CampaignRepository } from "@/pdc/providers/campaign/repositories/CampaignRepository.ts";
import { Timezone } from "@/pdc/providers/validator/index.ts";
import { signature as apply } from "../contracts/apply.contract.ts";
import { castUserStringToUTC, subDaysTz, today, toISOString } from "../helpers/index.ts";
import { PolicyRepositoryProviderInterfaceResolver } from "../interfaces/index.ts";
import { toCsv } from "../simulation/csv.ts";
import { SimulationService } from "../simulation/SimulationService.ts";

const log = taggedLogger("[campaign]", { maxLength: 512 });

interface CommandOptions {
  campaigns: number[];
  from: string;
  to: string;
  tz: Timezone;
  override: boolean;
  dryRun: boolean;
  out?: string;
  perimeter?: string;
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
    {
      signature: "--dry-run",
      description: "simulate apply --override then finalize in memory, write nothing in database (needs -c and --out)",
      default: false,
    },
    {
      signature: "--out <file>",
      description: "dry-run: CSV file, one line per trip, amounts in cents",
    },
    {
      signature: "--perimeter <codes>",
      description: "dry-run: replaces the territory perimeter, <type:code,...> as territory:perimeter (ex. reg:76)",
    },
  ],
})
export class ApplyCommand implements CommandInterface {
  constructor(
    protected kernel: KernelInterfaceResolver,
    private policyRepository: PolicyRepositoryProviderInterfaceResolver,
    private campaignRepository: CampaignRepository,
    private simulation: SimulationService,
  ) {}

  public async call(options: CommandOptions): Promise<ResultType> {
    try {
      const { tz, override } = options;

      // cast or default from/to dates
      const from = options.from && castUserStringToUTC(options.from, tz) || subDaysTz(today(tz), 7);
      const to = options.to && castUserStringToUTC(options.to, tz) || today(tz);

      if (options.dryRun) {
        return await this.dryRun(options, from, to);
      }
      if (options.perimeter || options.out) {
        throw new Error("--perimeter and --out require --dry-run");
      }

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
          log.warn("apply override with to date, reprocess and finalize further incentives", { policy_id });
        }

        // call the action
        try {
          await this.kernel.call(apply, params, context);
        } catch (e) {
          const message = Error.isError(e) ? e.message : "Unknown error";
          log.error("apply failed", {
            policy_id,
            from: params.from,
            to: params.to,
            override: params.override,
            error: message,
          });
        }
      }

      return "";
    } catch (e) {
      const message = Error.isError(e) ? e.message : "Unknown error";
      log.error("apply command failed", { error: String(get(e, "rpcError.data", message)) });
    }
  }

  private async dryRun(options: CommandOptions, from: Date, to: Date): Promise<string> {
    if (options.campaigns.length !== 1 || !options.out) {
      throw new Error("--dry-run needs exactly one campaign (-c) and --out <file>");
    }

    const rows = await this.simulation.simulate({
      policy_id: options.campaigns[0],
      from,
      to,
      perimeter: options.perimeter,
    });
    await Deno.writeTextFile(options.out, toCsv(rows));
    log.info("dry-run written", { policy_id: options.campaigns[0], out: options.out, trips: rows.length });
    return "";
  }
}
