import { serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { defaultNotificationBindings } from "../../providers/notification/index.ts";
import { S3StorageProvider } from "../../providers/storage/index.ts";
import { ValidatorMiddleware } from "../../providers/superstruct/ValidatorMiddleware.ts";
import { SessionRepository } from "../auth/providers/SessionRepository.ts";
import { UserScopeRepository } from "../auth/providers/UserScopeRepository.ts";
import { externalCampaignsFeatureMiddlewareBinding } from "./middlewares/ExternalCampaignsFeatureMiddleware.ts";
import { userScopeGuardMiddlewareBinding } from "./middlewares/UserScopeGuardMiddleware.ts";
import { CampaignApdfAction } from "./actions/CampaignApdfAction.ts";
import { CampaignsAction } from "./actions/CampaignsAction.ts";
import { JourneysIncentiveByDayAction } from "./actions/JourneysIncentiveByDayAction.ts";
import { JourneysIncentiveByMonthAction } from "./actions/JourneysIncentiveByMonthAction.ts";
import { JourneysOperatorsByDayAction } from "./actions/JourneysOperatorsByDayAction.ts";
import { JourneysOperatorsByMonthAction } from "./actions/JourneysOperatorsByMonthAction.ts";
import { CreateExternalCampaignAction } from "./actions/external-campaigns/CreateExternalCampaignAction.ts";
import { DeleteExternalCampaignAction } from "./actions/external-campaigns/DeleteExternalCampaignAction.ts";
import { ExternalCampaignsAction } from "./actions/external-campaigns/ExternalCampaignsAction.ts";
import { UpdateExternalCampaignAction } from "./actions/external-campaigns/UpdateExternalCampaignAction.ts";
import { ValidateExternalCampaignAction } from "./actions/external-campaigns/ValidateExternalCampaignAction.ts";
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
import { PurgeInactiveUsersCommand } from "./commands/PurgeInactiveUsersCommand.ts";
import { config } from "./config/index.ts";
import { BrevoProvider } from "./providers/BrevoProvider.ts";
import { CampaignsRepository } from "./providers/CampaignsRepository.ts";
import { ExternalCampaignsRepository } from "./providers/ExternalCampaignsRepository.ts";
import { JourneysRepository } from "./providers/JourneysRepository.ts";
import { OperatorsRepository } from "./providers/OperatorsRepository.ts";
import { TerritoriesRepository } from "./providers/TerritoriesRepository.ts";
import { UsersRepository } from "./providers/UsersRepository.ts";

@serviceProvider({
  config,
  commands: [PurgeInactiveUsersCommand],
  providers: [
    S3StorageProvider,
    BrevoProvider,
    OperatorsRepository,
    TerritoriesRepository,
    JourneysRepository,
    CampaignsRepository,
    ExternalCampaignsRepository,
    UsersRepository,
    UserScopeRepository,
    SessionRepository,
    ...defaultNotificationBindings,
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
    ExternalCampaignsAction,
    CreateExternalCampaignAction,
    UpdateExternalCampaignAction,
    DeleteExternalCampaignAction,
    ValidateExternalCampaignAction,
  ],
  middlewares: [
    ...defaultMiddlewareBindings,
    [
      "validate",
      ValidatorMiddleware,
    ],
    userScopeGuardMiddlewareBinding,
    externalCampaignsFeatureMiddlewareBinding,
  ],
})
export class DashboardServiceProvider extends AbstractServiceProvider {}
