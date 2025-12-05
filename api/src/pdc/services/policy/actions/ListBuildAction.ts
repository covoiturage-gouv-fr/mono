import { handler } from "../../../../ilos/common/index.ts";
import { Action as AbstractAction } from "../../../../ilos/core/index.ts";
import { SerializedPolicyInterface } from "../interfaces/engine/PolicyInterface.ts";
import { PolicyRepositoryProviderInterfaceResolver } from "../interfaces/index.ts";

const publicHandlerConfig = {
  service: "campaign",
  method: "listBuild",
} as const;

@handler({
  ...publicHandlerConfig,
  middlewares: [],
  apiRoute: {
    path: "campaigns/build/list",
    action: "campaign:listBuild",
    method: "GET",
    public: true,
  },
})
export class ListBuildAction extends AbstractAction {
  constructor(
    private repository: PolicyRepositoryProviderInterfaceResolver,
  ) {
    super();
  }

  public override async handle(): Promise<Array<{ _id: number }>> {
    const policies: SerializedPolicyInterface[] = await this.repository.findWhere({});

    return policies.map((r) => ({
      _id: r._id,
    }));
  }
}
