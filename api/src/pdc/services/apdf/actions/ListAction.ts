import {
  ConfigInterfaceResolver,
  ContextType,
  handler,
  KernelInterfaceResolver,
  NotFoundException,
} from "../../../../ilos/common/index.ts";
import { Action } from "../../../../ilos/core/index.ts";
import { logger } from "../../../../lib/logger/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "../../../providers/middleware/index.ts";
import {
  ParamsInterface as CampaignFindParams,
  ResultInterface as CampaignFindResult,
} from "../../policy/contracts/find.contract.ts";
import { handlerConfig, ParamsInterface, ResultsInterface } from "../contracts/list.contract.ts";
import { alias } from "../contracts/list.schema.ts";
import { StorageRepositoryProviderInterfaceResolver } from "../interfaces/StorageRepositoryProviderInterface.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      territory: "territory.apdf.list",
      operator: "operator.apdf.list",
      registry: "registry.apdf.list",
    }),
    ["validate", alias],
  ],
  apiRoute: {
    path: "/apdf/list",
    method: "POST",
  },
})
export class ListAction extends Action {
  constructor(
    private kernel: KernelInterfaceResolver,
    private storageRepository: StorageRepositoryProviderInterfaceResolver,
    private config: ConfigInterfaceResolver,
  ) {
    super();
  }

  public override async handle(params: ParamsInterface, context: ContextType): Promise<ResultsInterface> {
    const { campaign_id, operator_id } = params;

    // fetch the policy by id
    let campaign: CampaignFindResult | null = null;
    try {
      campaign = await this.kernel.call<CampaignFindParams, CampaignFindResult>(
        "campaign:find",
        { _id: campaign_id },
        { channel: { service: "apdf" }, call: context.call },
      );
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : String(e);
      logger.error(`[apdf:list -> campaign:find] ${errMsg}`);
      throw e;
    }

    if (!campaign) {
      throw new NotFoundException(`Could not find campaign ${campaign_id}`);
    }

    // curry operators and campaign filters
    // find policies, enrich and filter by operator
    const opsFilter = this.storageRepository.operatorsFilter(operator_id ? [operator_id] : []);
    const cmpFilter = this.storageRepository.campaignsFilter([campaign_id]);

    // month filter. We can hide the current month to operators and territories
    // by setting the env var APP_APDF_SHOW_LAST_MONTH to 'false'
    // default: true
    const monthFilter = this.storageRepository.showCurrentMonthFilter(
      context.call!.user.permissions,
      this.config.get("apdf.showLastMonth"),
    );

    return (await this.storageRepository.enrich(await this.storageRepository.findByCampaign(campaign)))
      .filter(cmpFilter)
      .filter(opsFilter)
      .filter(monthFilter);
  }
}
