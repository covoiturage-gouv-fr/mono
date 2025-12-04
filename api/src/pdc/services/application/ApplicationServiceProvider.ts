import { ExtensionInterface, NewableType, serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { ValidatorExtension, ValidatorMiddleware } from "../../providers/validator/index.ts";
import { CreateApplicationAction } from "./actions/CreateApplicationAction.ts";
import { FindApplicationAction } from "./actions/FindApplicationAction.ts";
import { ListApplicationAction } from "./actions/ListApplicationAction.ts";
import { RevokeApplicationAction } from "./actions/RevokeApplicationAction.ts";
import { config } from "./config/index.ts";
import { binding as createBinding } from "./contracts/create.schema.ts";
import { binding as findBinding } from "./contracts/find.schema.ts";
import { binding as listBinding } from "./contracts/list.schema.ts";
import { binding as revokeBinding } from "./contracts/revoke.schema.ts";
import { ApplicationPgRepositoryProvider } from "./providers/ApplicationPgRepositoryProvider.ts";

@serviceProvider({
  config,
  providers: [ApplicationPgRepositoryProvider],
  validator: [listBinding, findBinding, createBinding, revokeBinding],
  middlewares: [...defaultMiddlewareBindings, [
    "validate",
    ValidatorMiddleware,
  ]],
  handlers: [
    ListApplicationAction,
    FindApplicationAction,
    CreateApplicationAction,
    RevokeApplicationAction,
  ],
  commands: [],
})
export class ApplicationServiceProvider extends AbstractServiceProvider {
  override readonly extensions: NewableType<ExtensionInterface>[] = [ValidatorExtension];
}
