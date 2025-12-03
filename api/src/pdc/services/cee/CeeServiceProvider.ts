import { ExtensionInterface, NewableType, serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { ValidatorExtension, ValidatorMiddleware } from "../../providers/validator/index.ts";
import { DeleteCeeAction } from "./actions/DeleteCeeAction.ts";
import { FindCeeAction } from "./actions/FindCeeAction.ts";
import { ImportCeeAction } from "./actions/ImportCeeAction.ts";
import { ImportCeeIdentityAction } from "./actions/ImportCeeIdentityAction.ts";
import { RegisterCeeAction } from "./actions/RegisterCeeAction.ts";
import { SimulateCeeAction } from "./actions/SimulateCeeAction.ts";
import { config } from "./config/index.ts";
import { binding as deleteCeeBinding } from "./contracts/deleteApplication.schema.ts";
import { binding as findCeeBinding } from "./contracts/findApplication.schema.ts";
import { binding as importCeeBinding } from "./contracts/importApplication.schema.ts";
import { binding as importIdentityCeeBinding } from "./contracts/importApplicationIdentity.schema.ts";
import { binding as registerCeeBinding } from "./contracts/registerApplication.schema.ts";
import { binding as simulateCeeBinding } from "./contracts/simulateApplication.schema.ts";
import { CeeRepositoryProvider } from "./providers/CeeRepositoryProvider.ts";

@serviceProvider({
  config,
  providers: [CeeRepositoryProvider],
  validator: [
    importCeeBinding,
    importIdentityCeeBinding,
    registerCeeBinding,
    simulateCeeBinding,
    findCeeBinding,
    deleteCeeBinding,
  ],
  handlers: [
    ImportCeeAction,
    RegisterCeeAction,
    SimulateCeeAction,
    ImportCeeIdentityAction,
    FindCeeAction,
    DeleteCeeAction,
  ],
  middlewares: [...defaultMiddlewareBindings, [
    "validate",
    ValidatorMiddleware,
  ]],
})
export class CeeServiceProvider extends AbstractServiceProvider {
  override readonly extensions: NewableType<ExtensionInterface>[] = [ValidatorExtension];
}
