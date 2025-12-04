import { ExtensionInterface, NewableType, serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { GeoProvider } from "../../providers/geo/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { ValidatorExtension, ValidatorMiddleware } from "../../providers/validator/index.ts";
import { GetPointByAddressAction } from "./actions/GetPointByAddressAction.ts";
import { GetPointByCodeAction } from "./actions/GetPointByCodeAction.ts";
import { GetRouteMetaAction } from "./actions/GetRouteMetaAction.ts";
import { config } from "./config/index.ts";
import { binding as getPointByAddressBinding } from "./contracts/getPointByAddress.schema.ts";
import { binding as getPointByCodeBinding } from "./contracts/getPointByCode.schema.ts";
import { binding as getRouteMeta } from "./contracts/getRouteMeta.schema.ts";

@serviceProvider({
  config,
  providers: [GeoProvider],
  validator: [getPointByAddressBinding, getPointByCodeBinding, getRouteMeta],
  middlewares: [...defaultMiddlewareBindings, [
    "validate",
    ValidatorMiddleware,
  ]],
  handlers: [GetPointByAddressAction, GetPointByCodeAction, GetRouteMetaAction],
})
export class GeoServiceProvider extends AbstractServiceProvider {
  override readonly extensions: NewableType<ExtensionInterface>[] = [ValidatorExtension];
}
