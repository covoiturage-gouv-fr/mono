import { ExtensionInterface, NewableType, serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { ValidatorExtension, ValidatorMiddleware } from "../../providers/validator/index.ts";
import { FetchAction } from "./actions/FetchAction.ts";
import { FindAction } from "./actions/FindAction.ts";
import { FetchCommand } from "./commands/FetchCommand.ts";
import { config } from "./config/index.ts";
import { binding as fetchBinding, bindingCommand as fetchBindingCommand } from "./contracts/fetch.schema.ts";
import { binding as findBinding } from "./contracts/find.schema.ts";
import { CompanyDataSourceProvider } from "./providers/CompanyDataSourceProvider.ts";
import { CompanyRepositoryProvider } from "./providers/CompanyRepositoryProvider.ts";

@serviceProvider({
  config,
  providers: [CompanyRepositoryProvider, CompanyDataSourceProvider],
  validator: [fetchBinding, fetchBindingCommand, findBinding],
  middlewares: [...defaultMiddlewareBindings, [
    "validate",
    ValidatorMiddleware,
  ]],
  handlers: [FetchAction, FindAction],
  commands: [FetchCommand],
})
export class CompanyServiceProvider extends AbstractServiceProvider {
  override readonly extensions: NewableType<ExtensionInterface>[] = [ValidatorExtension];
}
