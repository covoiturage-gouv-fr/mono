import { ExtensionInterface, NewableType, serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { ValidatorMiddleware as SuperStructValidator } from "../../providers/superstruct/ValidatorMiddleware.ts";
import { ValidatorExtension, ValidatorMiddleware } from "../../providers/validator/index.ts";
import { FindGeoBySirenAction } from "./actions/geo/FindGeoBySirenAction.ts";
import { IndexAllGeoAction } from "./actions/geo/IndexAllGeoAction.ts";
import { ListGeoAction } from "./actions/geo/ListGeoAction.ts";
import { CreateTerritoryAction } from "./actions/group/CreateTerritoryAction.ts";
import { DeleteTerritoryAction } from "./actions/group/DeleteTerritoryAction.ts";
import { DeleteTerritoryActionV2 } from "./actions/group/DeleteTerritoryActionV2.ts";
import { FindTerritoryAction } from "./actions/group/FindTerritoryAction.ts";
import { GetAuthorizedCodesAction } from "./actions/group/GetAuthorizedCodesAction.ts";
import { ListTerritoryAction } from "./actions/group/ListTerritoryAction.ts";
import { ListTerritoryActionV2 } from "./actions/group/ListTerritoryActionV2.ts";
import { PatchContactsTerritoryAction } from "./actions/group/PatchContactsTerritoryAction.ts";
import { UpdateTerritoryAction } from "./actions/group/UpdateTerritoryAction.ts";
import { IndexCommand } from "./commands/IndexCommand.ts";
import { config } from "./config/index.ts";
import { create } from "./contracts/create.schema.ts";
import { deleteTerritory } from "./contracts/delete.schema.ts";
import { binding as findBinding } from "./contracts/find.schema.ts";
import { binding as findGeoBySirenBinding } from "./contracts/findGeoBySiren.schema.ts";
import { binding as getAuthorizedCodesBinding } from "./contracts/getAuthorizedCodes.schema.ts";
import { binding as listBinding } from "./contracts/list.schema.ts";
import { binding as listGeoBinding } from "./contracts/listGeo.schema.ts";
import { patchContacts } from "./contracts/patchContacts.schema.ts";
import { update } from "./contracts/update.schema.ts";
import { GeoRepositoryProvider } from "./providers/GeoRepositoryProvider.ts";
import { TerritoryRepositoryProvider } from "./providers/TerritoryRepositoryProvider.ts";

@serviceProvider({
  config,
  providers: [TerritoryRepositoryProvider, GeoRepositoryProvider],
  validator: [
    ["territory.create", create],
    ["territory.update", update],
    ["territory.delete", deleteTerritory],
    ["territory.patchContacts", patchContacts],
    findBinding,
    listBinding,
    findGeoBySirenBinding,
    listGeoBinding,
    getAuthorizedCodesBinding,
  ],
  middlewares: [...defaultMiddlewareBindings, [
    "validate",
    ValidatorMiddleware,
  ], ["validate-superstruct", SuperStructValidator]],
  handlers: [
    FindTerritoryAction,
    ListTerritoryAction,
    ListGeoAction,
    DeleteTerritoryAction,
    DeleteTerritoryActionV2,
    UpdateTerritoryAction,
    PatchContactsTerritoryAction,
    CreateTerritoryAction,
    FindGeoBySirenAction,
    ListTerritoryActionV2,
    GetAuthorizedCodesAction,
    IndexAllGeoAction,
  ],
  commands: [IndexCommand],
})
export class TerritoryServiceProvider extends AbstractServiceProvider {
  override readonly extensions: NewableType<ExtensionInterface>[] = [ValidatorExtension];
}
