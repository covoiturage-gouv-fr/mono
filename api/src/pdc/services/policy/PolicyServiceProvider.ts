import { ExtensionInterface, NewableType, serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { APDFNameProvider } from "../../providers/storage/index.ts";
import { ValidatorExtension, ValidatorMiddleware } from "../../providers/validator/index.ts";
import { ApplyAction } from "./actions/ApplyAction.ts";
import { FinalizeAction } from "./actions/FinalizeAction.ts";
import { FindAction } from "./actions/FindAction.ts";
import { ListAction } from "./actions/ListAction.ts";
import { StatsAction } from "./actions/StatsAction.ts";
import { syncIncentiveSumAction } from "./actions/SyncIncentiveSumAction.ts";
import { UpdateDescriptiveSheetUrlAction } from "./actions/UpdateDescriptiveSheetUrlAction.ts";
import { ApplyCommand } from "./commands/ApplyCommand.ts";
import { FinalizeCommand } from "./commands/FinalizeCommand.ts";
import { StatsCommand } from "./commands/StatsCommand.ts";
import { SyncCommand } from "./commands/SyncCommand.ts";
import { config } from "./config/index.ts";
import { binding as applySchemaBinding } from "./contracts/apply.schema.ts";
import { binding as finalizeSchemaBinding } from "./contracts/finalize.schema.ts";
import { binding as findSchemaBinding } from "./contracts/find.schema.ts";
import { binding as listSchemaBinding } from "./contracts/list.schema.ts";
import { binding as statsSchemaBinding } from "./contracts/stats.schema.ts";
import { binding as syncIncentiveSumSchemaBinding } from "./contracts/syncIncentiveSum.schema.ts";
import { binding as updateDescriptiveSheetUrlSchemaBinding } from "./contracts/updateDescriptiveSheetUrl.schema.ts";
import { IncentiveRepositoryProvider } from "./providers/IncentiveRepositoryProvider.ts";
import { MetadataRepositoryProvider } from "./providers/MetadataRepositoryProvider.ts";
import { PolicyRepositoryProvider } from "./providers/PolicyRepositoryProvider.ts";
import { TerritoryRepositoryProvider } from "./providers/TerritoryRepositoryProvider.ts";
import { TripRepositoryProvider } from "./providers/TripRepositoryProvider.ts";

@serviceProvider({
  config,
  providers: [
    APDFNameProvider,
    IncentiveRepositoryProvider,
    MetadataRepositoryProvider,
    PolicyRepositoryProvider,
    TerritoryRepositoryProvider,
    TripRepositoryProvider,
  ],
  validator: [
    applySchemaBinding,
    finalizeSchemaBinding,
    findSchemaBinding,
    listSchemaBinding,
    statsSchemaBinding,
    syncIncentiveSumSchemaBinding,
    updateDescriptiveSheetUrlSchemaBinding,
  ],
  handlers: [
    ApplyAction,
    FinalizeAction,
    FindAction,
    ListAction,
    StatsAction,
    syncIncentiveSumAction,
    UpdateDescriptiveSheetUrlAction,
  ],
  commands: [ApplyCommand, FinalizeCommand, StatsCommand, SyncCommand],
  middlewares: [...defaultMiddlewareBindings, [
    "validate",
    ValidatorMiddleware,
  ]],
})
export class PolicyServiceProvider extends AbstractServiceProvider {
  override readonly extensions: NewableType<ExtensionInterface>[] = [ValidatorExtension];
}
