import { router, serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { ValidatorMiddleware } from "../../providers/superstruct/ValidatorMiddleware.ts";
import { CreateAccessTokenAction } from "./actions/CreateAccessTokenAction.ts";
import { CreateCredentialsAction } from "./actions/CreateCredentialsAction.ts";
import { DeleteCredentialsAction } from "./actions/DeleteCredentialsAction.ts";
import { ReadCredentialsAction } from "./actions/ReadCredentialsAction.ts";
import { AuthRouter } from "./AuthRouter.ts";
import { config } from "./config/index.ts";
import { DexOIDCProvider } from "./providers/DexOIDCProvider.ts";
import { OperatorRepository } from "./providers/OperatorRepository.ts";
import { ProConnectOIDCProvider } from "./providers/ProConnectOIDCProvider.ts";
import { UserRepository } from "./providers/UserRepository.ts";

@serviceProvider({
  config,
  providers: [
    UserRepository,
    OperatorRepository,
    DexOIDCProvider,
    ProConnectOIDCProvider,
    [router, AuthRouter],
  ],
  middlewares: [
    ["validate", ValidatorMiddleware],
    ...defaultMiddlewareBindings,
  ],
  handlers: [CreateAccessTokenAction, ReadCredentialsAction, CreateCredentialsAction, DeleteCredentialsAction],
})
export class AuthServiceProvider extends AbstractServiceProvider {}
