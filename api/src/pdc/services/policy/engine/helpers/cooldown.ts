import { MetadataLifetime, StatefulContextInterface, StatelessContextInterface } from "../../interfaces/index.ts";

/**
 * Non-splitting ("non-tronçonnage") cooldown.
 *
 * Registers a metadata storing the epoch (ms) of the last incentivized pickup
 * for a given driver/passenger pair. `applyCooldown` then excludes any pickup
 * happening less than `minMinutes` after the previous one, to prevent a driver
 * from splitting a single ride into several short segments.
 *
 * The metadata uses a Month lifetime on purpose: `MetadataStore.store` only
 * persists metadata whose lifetime is greater than Day, so a Day-scoped value
 * would be lost between the finalize 6-hour windows. Month survives the windows
 * (reset only across a month boundary — an accepted edge case).
 */

const COOLDOWN_NAME = "cooldown_last_pickup";

export function registerCooldown(ctx: StatelessContextInterface, uuid: string): void {
  const { driver_identity_key, passenger_identity_key } = ctx.carpool;
  if (!driver_identity_key || !passenger_identity_key) {
    return;
  }

  ctx.meta.register({
    uuid,
    name: COOLDOWN_NAME,
    // "|" separator: identity keys are hex fingerprints, but a non-alphabet
    // separator removes any theoretical scope collision between pairs.
    scope: `${driver_identity_key}|${passenger_identity_key}`,
    lifetime: MetadataLifetime.Month,
  });
}

export function applyCooldown(
  ctx: StatefulContextInterface,
  uuid: string,
  minMinutes: number,
): void {
  // The pair was not registered (missing identity key): nothing to enforce.
  if (!ctx.meta.getRaw(uuid)) {
    return;
  }

  const previous = ctx.meta.get(uuid); // epoch ms of the last pickup, 0 if none
  const current = ctx.meta.datetime.getTime();

  // Always record the latest pickup so the cooldown chains from it.
  ctx.meta.set(uuid, current);

  if (previous > 0 && current - previous < minMinutes * 60_000) {
    ctx.incentive.set(0);
  }
}
