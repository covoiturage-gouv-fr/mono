import {
  ContextType,
  middleware,
  MiddlewareInterface,
  NotFoundException,
  ParamsType,
  ResultType,
} from "@/ilos/common/index.ts";
import { env_or_false } from "@/lib/env/index.ts";
import { ConfiguredMiddleware } from "@/pdc/providers/middleware/interfaces.ts";
import { NextFunction } from "dep:express";

// Lu à chaque appel : éteint tant que APP_FEATURE_EXTERNAL_CAMPAIGNS n'est pas "true".
export const externalCampaignsEnabled = (): boolean => env_or_false("APP_FEATURE_EXTERNAL_CAMPAIGNS");

// 404 plutôt que 403 : la fonctionnalité n'existe pas tant qu'elle est éteinte, quel que soit le rôle.
@middleware()
export class ExternalCampaignsFeatureMiddleware implements MiddlewareInterface {
  async process(params: ParamsType, context: ContextType, next: NextFunction): Promise<ResultType> {
    if (!externalCampaignsEnabled()) throw new NotFoundException("Fonctionnalité indisponible");
    return next(params, context);
  }
}

const alias = "dashboard.external_campaigns_feature";

export const externalCampaignsFeatureMiddlewareBinding = [alias, ExternalCampaignsFeatureMiddleware];

export function externalCampaignsFeatureMiddleware(): ConfiguredMiddleware<undefined> {
  return [alias, undefined];
}
