import { CommandExtension } from "../../../ilos/cli/index.ts";
import { ExtensionInterface, NewableType, serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { DefaultTimezoneMiddleware } from "../../middlewares/DefaultTimezoneMiddleware.ts";
import { DataGouvAPIProvider } from "../../providers/datagouv/DataGouvAPIProvider.ts";
import { DataGouvMetadataProvider } from "../../providers/datagouv/DataGouvMetadataProvider.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { S3StorageProvider } from "../../providers/storage/index.ts";
import { ValidatorExtension, ValidatorMiddleware } from "../../providers/validator/index.ts";
import { CreateAction } from "./actions/CreateAction.ts";
import { listAction } from "./actions/ListAction.ts";
import { CreateCommand } from "./commands/CreateCommand.ts";
import { DataGouvCommand } from "./commands/DataGouvCommand.ts";
import { ProcessCommand } from "./commands/ProcessCommand.ts";
import { config } from "./config/index.ts";
import { bindingV3 as createBindingV3 } from "./contracts/create.schema.ts";
import { binding as listBinding } from "./contracts/list.schema.ts";
import { CampaignRepository } from "./repositories/CampaignRepository.ts";
import { CarpoolRepository } from "./repositories/CarpoolRepository.ts";
import { ExportRepository } from "./repositories/ExportRepository.ts";
import { LogRepository } from "./repositories/LogRepository.ts";
import { RecipientRepository } from "./repositories/RecipientRepository.ts";
import { TerritoryRepository } from "./repositories/TerritoryRepository.ts";
import { UserRepository } from "./repositories/UserRepository.ts";
import { DataGouvFileCreatorService } from "./services/DataGouvFileCreatorService.ts";
import { FieldService } from "./services/FieldService.ts";
import { FileCreatorService } from "./services/FileCreatorService.ts";
import { LogService } from "./services/LogService.ts";
import { NameService } from "./services/NameService.ts";
import { RecipientService } from "./services/RecipientService.ts";
import { TerritoryService } from "./services/TerritoryService.ts";

// Services are from the ./services folder
// and are used to implement the business logic of the application.
// They are injected in commands and handlers.
const services = [
  FieldService,
  FileCreatorService,
  DataGouvFileCreatorService,
  LogService,
  NameService,
  RecipientService,
  TerritoryService,
];

// Repositories are from the ./repositories folder
// and are used to access the database or other data sources.
const repositories = [
  CampaignRepository,
  CarpoolRepository,
  ExportRepository,
  LogRepository,
  RecipientRepository,
  TerritoryRepository,
  UserRepository,
];

// External providers are from the @pdc namespace
const externalProviders = [S3StorageProvider, DataGouvAPIProvider, DataGouvMetadataProvider];

// Commands are from the ./commands folder
// and are used to implement the CLI commands.
const commands = [CreateCommand, DataGouvCommand, ProcessCommand];

// Handlers are from the ./actions folder
// and are used to implement the API endpoints (also called actions).
const handlers = [CreateAction, listAction];

// Validator bindings are from the @shared/export/*.schema.ts files
// and are used to validate the input data using JSON Schema.
const validators = [createBindingV3, listBinding];

@serviceProvider({
  config,
  commands,
  handlers,
  providers: [...externalProviders, ...repositories, ...services],
  validator: [...validators],
  middlewares: [
    ...defaultMiddlewareBindings,
    ["validate", ValidatorMiddleware],
    ["timezone", DefaultTimezoneMiddleware],
    // scopeToGroupBinding,
  ],
})
export class ExportServiceProvider extends AbstractServiceProvider {
  override readonly extensions: NewableType<ExtensionInterface>[] = [
    CommandExtension,
    ValidatorExtension,
  ];
}
