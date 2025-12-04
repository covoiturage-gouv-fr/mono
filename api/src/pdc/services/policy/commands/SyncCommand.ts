import { command, CommandInterface, ResultType } from "../../../../ilos/common/index.ts";
import { logger } from "../../../../lib/logger/index.ts";
import { PolicyRepositoryProviderInterfaceResolver } from "../interfaces/index.ts";

@command({
  signature: "campaign:sync",
  description: "Sync incentive sum and status for all campaigns",
})
export class SyncCommand implements CommandInterface {
  constructor(
    protected policyRepository: PolicyRepositoryProviderInterfaceResolver,
  ) {}

  public async call(): Promise<ResultType> {
    // sync status
    logger.info("Syncing campaign statuses");
    await this.policyRepository.updateAllCampaignStatuses();

    // sync incentive sum
    logger.info("Syncing campaign incentive_sum");
    for (const campaign_id of await this.policyRepository.listApplicablePoliciesId()) {
      await this.policyRepository.syncIncentiveSum(campaign_id);
    }
  }
}
