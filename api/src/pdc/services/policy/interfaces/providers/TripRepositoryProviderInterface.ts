import { CarpoolInterface, PolicyInterface } from "../index.ts";

export abstract class TripRepositoryProviderInterfaceResolver {
  abstract findTripByPolicy(
    policy: PolicyInterface,
    from: Date,
    to: Date,
    batchSize?: number,
    override?: boolean,
    arr?: string[],
  ): AsyncGenerator<CarpoolInterface[], void, void>;

  abstract findTripByGeo(
    coms: string[],
    from: Date,
    to: Date,
    batchSize?: number,
    override?: boolean,
  ): AsyncGenerator<CarpoolInterface[], void, void>;
}
