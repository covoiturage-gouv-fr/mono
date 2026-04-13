import { serviceProvider } from "../../../ilos/common/index.ts";
import { ServiceProvider as AbstractServiceProvider } from "../../../ilos/core/index.ts";
import { defaultMiddlewareBindings } from "../../providers/middleware/index.ts";
import { ValidatorMiddleware } from "../../providers/superstruct/ValidatorMiddleware.ts";
import { JourneysByDistancesAction } from "./actions/distribution/JourneysByDistancesAction.ts";
import { JourneysByHoursAction } from "./actions/distribution/JourneysByHoursAction.ts";
import { BestFluxAction } from "./actions/flux/BestFluxAction.ts";
import { EvolFluxAction } from "./actions/flux/EvolFluxAction.ts";
import { FluxAction } from "./actions/flux/FluxAction.ts";
import { IncentiveAction } from "./actions/incentive/IncentiveAction.ts";
import { CampaignsAction } from "./actions/incentiveCampaigns/CampaignsAction.ts";
import { AiresCovoiturageAction } from "./actions/infra/AiresCovoiturageAction.ts";
import { KeyfiguresAction } from "./actions/keyfigures/KeyfiguresAction.ts";
import { LocationAction } from "./actions/location/LocationAction.ts";
import { BestTerritoriesAction } from "./actions/occupation/BestTerritoriesAction.ts";
import { EvolOccupationAction } from "./actions/occupation/EvolOccupationAction.ts";
import { LastRecordAction } from "./actions/LastRecordAction.ts";
import { OccupationAction } from "./actions/occupation/OccupationAction.ts";
import { DistributionRepositoryProvider } from "./providers/DistributionRepositoryProvider.ts";
import { FluxRepositoryProvider } from "./providers/FluxRepositoryProvider.ts";
import { IncentiveCampaignsRepositoryProvider } from "./providers/IncentiveCampaignsRepositoryProvider.ts";
import { IncentiveRepositoryProvider } from "./providers/IncentiveRepositoryProvider.ts";
import { InfraRepositoryProvider } from "./providers/InfraRepositoryProvider.ts";
import { KeyfiguresRepositoryProvider } from "./providers/KeyfiguresRepositoryProvider.ts";
import { LocationRepositoryProvider } from "./providers/LocationRepositoryProvider.ts";
import { LastRecordRepositoryProvider } from "./providers/LastRecordRepositoryProvider.ts";
import { OccupationRepositoryProvider } from "./providers/OccupationRepositoryProvider.ts";

@serviceProvider({
  commands: [],
  providers: [
    DistributionRepositoryProvider,
    FluxRepositoryProvider,
    InfraRepositoryProvider,
    KeyfiguresRepositoryProvider,
    LocationRepositoryProvider,
    OccupationRepositoryProvider,
    IncentiveRepositoryProvider,
    IncentiveCampaignsRepositoryProvider,
    LastRecordRepositoryProvider,
  ],
  handlers: [
    AiresCovoiturageAction,
    BestFluxAction,
    BestTerritoriesAction,
    EvolFluxAction,
    EvolOccupationAction,
    JourneysByDistancesAction,
    JourneysByHoursAction,
    LocationAction,
    FluxAction,
    IncentiveAction,
    KeyfiguresAction,
    OccupationAction,
    CampaignsAction,
    LastRecordAction,
  ],
  middlewares: [...defaultMiddlewareBindings, [
    "validate",
    ValidatorMiddleware,
  ]],
})
export class ObservatoryServiceProvider extends AbstractServiceProvider {}
