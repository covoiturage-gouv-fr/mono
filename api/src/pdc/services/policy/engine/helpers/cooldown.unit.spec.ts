import { it } from "dep:testing-bdd";

import { StatefulContextInterface, StatelessContextInterface } from "../../interfaces/index.ts";
import { process } from "../tests/macro.ts";
import { applyCooldown, registerCooldown } from "./cooldown.ts";

const COOLDOWN_UUID = "0f0c3e4a-0000-0000-0000-000000000001";
const DRIVER = "driver-key";
const PASSENGER = "passenger-key";

// Minimal handler that flat-incentivizes every eligible carpool and applies a
// 60-minute non-splitting cooldown per driver/passenger pair.
const handler = {
  id: "cooldown_test",
  limits: [],
  max_amount: 0,
  async load() {},
  processStateless(ctx: StatelessContextInterface) {
    registerCooldown(ctx, COOLDOWN_UUID);
    ctx.incentive.set(100);
  },
  processStateful(ctx: StatefulContextInterface) {
    applyCooldown(ctx, COOLDOWN_UUID, 60);
  },
  params() {
    return { tz: "Europe/Paris", slices: [], operators: [], allTimeOperators: [], limits: {}, extras: {} };
  },
};

// same day, same pair, minutes apart
function carpoolAt(minutes: number, passenger = PASSENGER) {
  return {
    datetime: new Date(new Date("2025-06-16T08:00:00Z").getTime() + minutes * 60_000),
    driver_identity_key: DRIVER,
    passenger_identity_key: passenger,
  };
}

it("excludes a second pickup of the same pair within 60 minutes", async () => {
  await process(
    { handler, carpool: [carpoolAt(0), carpoolAt(30)] },
    { incentive: [100, 0] },
  );
});

it("keeps a pickup exactly 60 minutes after the previous one", async () => {
  await process(
    { handler, carpool: [carpoolAt(0), carpoolAt(60)] },
    { incentive: [100, 100] },
  );
});

it("chains the cooldown from the latest pickup (0, 30, 90)", async () => {
  // t0 kept, t30 excluded (<60 from t0), t90 kept (>=60 from t30)
  await process(
    { handler, carpool: [carpoolAt(0), carpoolAt(30), carpoolAt(90)] },
    { incentive: [100, 0, 100] },
  );
});

it("does not affect a different passenger", async () => {
  await process(
    { handler, carpool: [carpoolAt(0), carpoolAt(30, "other-passenger")] },
    { incentive: [100, 100] },
  );
});
