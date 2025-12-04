import { ExtensionInterface, NewableType, serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { ValidatorExtension, ValidatorMiddleware } from "../../providers/validator/index.ts";
import { CreateOperatorAction } from "./actions/CreateOperatorAction.ts";
import { DeleteOperatorAction } from "./actions/DeleteOperatorAction.ts";
import { FindByUuidOperatorAction } from "./actions/FindByUuidOperatorAction.ts";
import { FindOperatorAction } from "./actions/FindOperatorAction.ts";
import { ListOperatorAction } from "./actions/ListOperatorAction.ts";
import { PatchContactsOperatorAction } from "./actions/PatchContactsOperatorAction.ts";
import { PatchThumbnailOperatorAction } from "./actions/PatchThumbnailOperatorAction.ts";
import { QuickFindOperatorAction } from "./actions/QuickFindOperatorAction.ts";
import { UpdateOperatorAction } from "./actions/UpdateOperatorAction.ts";
import { config } from "./config/index.ts";
import { binding as createBinding } from "./contracts/create.schema.ts";
import { binding as deleteBinding } from "./contracts/delete.schema.ts";
import { binding as findBinding } from "./contracts/find.schema.ts";
import { binding as findbyuuidBinding } from "./contracts/findbyuuid.schema.ts";
import { binding as patchContactsBinding } from "./contracts/patchContacts.schema.ts";
import { binding as patchThumbnailBinding } from "./contracts/patchThumbnail.schema.ts";
import { binding as quickfindBinding } from "./contracts/quickfind.schema.ts";
import { binding as updateBinding } from "./contracts/update.schema.ts";
import { OperatorPgRepositoryProvider } from "./providers/OperatorPgRepositoryProvider.ts";

@serviceProvider({
  config,
  providers: [OperatorPgRepositoryProvider],
  validator: [
    createBinding,
    deleteBinding,
    findBinding,
    findbyuuidBinding,
    patchContactsBinding,
    patchThumbnailBinding,
    quickfindBinding,
    updateBinding,
  ],
  handlers: [
    CreateOperatorAction,
    DeleteOperatorAction,
    FindByUuidOperatorAction,
    FindOperatorAction,
    ListOperatorAction,
    PatchContactsOperatorAction,
    PatchThumbnailOperatorAction,
    QuickFindOperatorAction,
    UpdateOperatorAction,
  ],
  middlewares: [...defaultMiddlewareBindings, [
    "validate",
    ValidatorMiddleware,
  ]],
  commands: [],
})
export class OperatorServiceProvider extends AbstractServiceProvider {
  override readonly extensions: NewableType<ExtensionInterface>[] = [ValidatorExtension];
}
