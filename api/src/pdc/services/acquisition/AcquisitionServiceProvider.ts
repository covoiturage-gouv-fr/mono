import { ExtensionInterface, NewableType, serviceProvider } from "@/ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "@/ilos/core/index.ts";
import { CarpoolAcquisitionService } from "@/pdc/providers/carpool/index.ts";
import { CarpoolStatusService } from "@/pdc/providers/carpool/providers/CarpoolStatusService.ts";
import { GeoProvider } from "@/pdc/providers/geo/index.ts";
import { defaultMiddlewareBindings } from "@/pdc/providers/middleware/index.ts";
import { ValidatorExtension, ValidatorMiddleware } from "@/pdc/providers/validator/index.ts";
import { CancelJourneyAction } from "./actions/CancelJourneyAction.ts";
import { CreateJourneyAction } from "./actions/CreateJourneyAction.ts";
import { ListJourneyAction } from "./actions/ListJourneyAction.ts";
import { PatchJourneyAction } from "./actions/PatchJourneyAction.ts";
import { StatusJourneyAction } from "./actions/StatusJourneyAction.ts";
import { JourneyStatusCommand } from "./commands/JourneyStatusCommand.ts";
import { AcquisitionMigrateCommand } from "./commands/MigrateAcquisitionCommand.ts";
import { ProcessGeoCommand } from "./commands/ProcessGeoCommand.ts";
import { config } from "./config/index.ts";
import { binding as cancelBinding } from "./contracts/cancel.schema.ts";
import { v3binding } from "./contracts/create.schema.ts";
import { binding as listBinding } from "./contracts/list.schema.ts";
import { binding as patchBinding } from "./contracts/patch.schema.ts";
import { binding as statusBinding } from "./contracts/status.schema.ts";

@serviceProvider({
  config,
  commands: [
    AcquisitionMigrateCommand,
    ProcessGeoCommand,
    JourneyStatusCommand,
  ],
  providers: [
    GeoProvider,
    CarpoolAcquisitionService,
    CarpoolStatusService,
  ],
  validator: [
    v3binding,
    listBinding,
    cancelBinding,
    statusBinding,
    patchBinding,
  ],
  middlewares: [...defaultMiddlewareBindings, [
    "validate",
    ValidatorMiddleware,
  ]],
  handlers: [
    CancelJourneyAction,
    CreateJourneyAction,
    StatusJourneyAction,
    ListJourneyAction,
    PatchJourneyAction,
  ],
})
export class AcquisitionServiceProvider extends AbstractServiceProvider {
  readonly extensions: NewableType<ExtensionInterface>[] = [ValidatorExtension];
}
