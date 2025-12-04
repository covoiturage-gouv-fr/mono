import { ExtensionInterface, NewableType, serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { APDFNameProvider, S3StorageProvider } from "../../providers/storage/index.ts";
import { ValidatorExtension, ValidatorMiddleware } from "../../providers/validator/index.ts";
import { ExportAction } from "./actions/ExportAction.ts";
import { ListAction } from "./actions/ListAction.ts";
import { ExportCommand } from "./commands/ExportCommand.ts";
import { config } from "./config/index.ts";
import { binding as exportBinding } from "./contracts/export.schema.ts";
import { binding as listBinding } from "./contracts/list.schema.ts";
import { DataRepositoryProvider } from "./providers/DataRepositoryProvider.ts";
import { StorageRepositoryProvider } from "./providers/StorageRepositoryProvider.ts";

@serviceProvider({
  config,
  validator: [listBinding, exportBinding],
  providers: [
    APDFNameProvider,
    DataRepositoryProvider,
    S3StorageProvider,
    StorageRepositoryProvider,
  ],
  handlers: [ListAction, ExportAction],
  commands: [ExportCommand],
  middlewares: [...defaultMiddlewareBindings, [
    "validate",
    ValidatorMiddleware,
  ]],
})
export class APDFServiceProvider extends AbstractServiceProvider {
  override readonly extensions: NewableType<ExtensionInterface>[] = [ValidatorExtension];
}
