import { ExtensionInterface, NewableType, serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { defaultNotificationBindings } from "../../providers/notification/index.ts";
import { ValidatorExtension, ValidatorMiddleware } from "../../providers/validator/index.ts";
import { StatsRefreshAction } from "./actions/StatsRefreshAction.ts";
import { StatsRefreshCommand } from "./commands/StatsRefreshCommand.ts";
import { config } from "./config/index.ts";
import { binding as statsRefreshBinding } from "./contracts/statsrefresh.schema.ts";

@serviceProvider({
  config,
  providers: [...defaultNotificationBindings],
  commands: [StatsRefreshCommand],
  validator: [statsRefreshBinding],
  middlewares: [...defaultMiddlewareBindings, [
    "validate",
    ValidatorMiddleware,
  ]],
  handlers: [StatsRefreshAction],
})
export class MonitoringServiceProvider extends AbstractServiceProvider {
  override readonly extensions: NewableType<ExtensionInterface>[] = [ValidatorExtension];
}
