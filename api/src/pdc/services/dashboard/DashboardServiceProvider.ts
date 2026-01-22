import { serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { S3StorageProvider } from "../../providers/storage/index.ts";
import { ValidatorMiddleware } from "../../providers/superstruct/ValidatorMiddleware.ts";
import { CampaignApdfAction } from "./actions/CampaignApdfAction.ts";
import { CampaignsAction } from "./actions/CampaignsAction.ts";
import { JourneysIncentiveByDayAction } from "./actions/JourneysIncentiveByDayAction.ts";
import { JourneysIncentiveByMonthAction } from "./actions/JourneysIncentiveByMonthAction.ts";
import { JourneysOperatorsByDayAction } from "./actions/JourneysOperatorsByDayAction.ts";
import { JourneysOperatorsByMonthAction } from "./actions/JourneysOperatorsByMonthAction.ts";
import { CreateOperatorAction } from "./actions/operators/CreateOperatorAction.ts";
import { DeleteOperatorAction } from "./actions/operators/DeleteOperatorAction.ts";
import { OperatorAction } from "./actions/operators/OperatorAction.ts";
import { OperatorsAction } from "./actions/operators/OperatorsAction.ts";
import { UpdateOperatorAction } from "./actions/operators/UpdateOperatorAction.ts";
import { CreateUserAction } from "./actions/users/CreateUserAction.ts";
import { DeleteUserAction } from "./actions/users/DeleteUserAction.ts";
import { UpdateUserAction } from "./actions/users/UpdateUserAction.ts";
import { UserAction } from "./actions/users/UserAction.ts";
import { UsersAction } from "./actions/users/UsersAction.ts";
import { config } from "./config/index.ts";
import { BrevoProvider } from "./providers/BrevoProvider.ts";
import { CampaignsRepository } from "./providers/CampaignsRepository.ts";
import { JourneysRepository } from "./providers/JourneysRepository.ts";
import { OperatorsRepository } from "./providers/OperatorsRepository.ts";
import { TerritoriesRepository } from "./providers/TerritoriesRepository.ts";
import { UsersRepository } from "./providers/UsersRepository.ts";

@serviceProvider({
  config,
  commands: [],
  providers: [
    S3StorageProvider,
    BrevoProvider,
    OperatorsRepository,
    TerritoriesRepository,
    JourneysRepository,
    CampaignsRepository,
    UsersRepository,
  ],
  handlers: [
    JourneysOperatorsByMonthAction,
    JourneysOperatorsByDayAction,
    JourneysIncentiveByMonthAction,
    JourneysIncentiveByDayAction,
    CampaignsAction,
    CampaignApdfAction,
    OperatorAction,
    OperatorsAction,
    CreateOperatorAction,
    DeleteOperatorAction,
    UpdateOperatorAction,
    UserAction,
    UsersAction,
    CreateUserAction,
    UpdateUserAction,
    DeleteUserAction,
  ],
  middlewares: [...defaultMiddlewareBindings, [
    "validate",
    ValidatorMiddleware,
  ]],
})
export class DashboardServiceProvider extends AbstractServiceProvider {}
