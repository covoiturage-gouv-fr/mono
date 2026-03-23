import { provider } from "@/ilos/common/index.ts";
import { env_or_fail } from "@/lib/env/index.ts";
import fetcher from "@/lib/fetcher/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { get } from "@/lib/object/index.ts";
import { PointInterface, RouteMeta, RouteMetaProviderInterface } from "../interfaces/index.ts";

@provider()
export class OSRMProvider implements RouteMetaProviderInterface {
  // Requires port-forwarding: just forward osrm in the infra repository
  protected domain = env_or_fail(
    "OSRM_URL",
    "http://localhost:5000",
  );

  async getRouteMeta(
    start: PointInterface,
    end: PointInterface,
  ): Promise<RouteMeta> {
    try {
      const query = `${start.lon},${start.lat};${end.lon},${end.lat}`;

      const response = await fetcher.get(
        `${this.domain}/route/v1/driving/${encodeURIComponent(query)}`,
      );
      const data = await response.json();
      const distance = get(data, "routes.0.distance", null) as
        | number
        | null;
      const duration = get(data, "routes.0.duration", null) as
        | number
        | null;

      if (distance === null || duration === null) {
        throw new Error(
          `Unable to load route meta data for (${start.lon};${start.lat}) -> (${end.lon};${end.lat}) on ${this.domain}`,
        );
      }

      return { distance, duration };
    } catch (e) {
      logger.error(
        `[OSRMProvider] (${start.lon},${start.lat};${end.lon},${end.lat})`,
      );
      switch (e.response?.status) {
        case 429:
          throw new Error(`[OSRMProvider] Too many requests on ${this.domain}`);
        default:
          throw e;
      }
    }
  }
}
