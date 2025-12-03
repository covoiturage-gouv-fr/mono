import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import {
  CarpoolInterface,
  IncentiveStateEnum,
  IncentiveStatusEnum,
  SerializedIncentiveInterface,
} from "../../interfaces/index.ts";

const defaultPosition = {
  arr: "91377",
  com: "91377",
  aom: "217500016",
  epci: "200056232",
  dep: "91",
  reg: "11",
  country: "XXXXX",
};

const defaultLat = 48.72565703413325;
const defaultLon = 2.261827843187402;

const dftCarpool: CarpoolInterface = {
  operator_trip_id: uuidV4(),
  driver_identity_key: uuidV4(),
  passenger_identity_key: uuidV4(),
  operator_uuid: "0b361f5b-4651-45f1-8f59-5952d5e745fd",
  operator_class: "C",
  passenger_is_over_18: true,
  driver_has_travel_pass: true,
  passenger_has_travel_pass: true,
  datetime: new Date("2019-01-15"),
  seats: 1,
  distance: 5000,
  operator_journey_id: uuidV4(),
  operator_id: 1,
  driver_revenue: 20,
  passenger_contribution: 20,
  start: { ...defaultPosition },
  end: { ...defaultPosition },
  start_lat: defaultLat,
  start_lon: defaultLon,
  end_lat: defaultLat,
  end_lon: defaultLon,
};

export function generateCarpool(
  carpool: Partial<CarpoolInterface> = {},
  defaultCarpool: CarpoolInterface = dftCarpool,
): CarpoolInterface {
  return { ...defaultCarpool, ...carpool };
}

const defaultIncentive: SerializedIncentiveInterface = {
  _id: 1,
  policy_id: 1,
  operator_id: 1,
  operator_journey_id: dftCarpool.operator_journey_id,
  datetime: new Date("2019-01-15"),
  statelessAmount: 100,
  statefulAmount: 100,
  status: IncentiveStatusEnum.Draft,
  state: IncentiveStateEnum.Regular,
  meta: [],
};

export function generateIncentive(incentive: Partial<SerializedIncentiveInterface> = {}): SerializedIncentiveInterface {
  return { ...defaultIncentive, ...incentive };
}

/**
 * Generate a list of partial carpools
 *
 * distance is set to 25_000
 * driver_identity_key is set to 'three'
 * passenger_identity_key is set to a random uuid on every carpool to avoid limits
 *
 * From the date provided, every 3rd carpool will be the next day.
 * You should start at the beginning of the month to spread carpools over the
 * same month when testing for limits.
 *
 * @example
 * it(
 *   'should work with driver amount month limits',
 *   async () => await process(
 *   {
 *     policy: { handler: Handler.id },
 *     carpool: generatePartialCarpools(80, new Date('2023-10-01')),
 *     meta: [],
 *   },
 *   {
 *     incentive: [...[...Array(80).keys()].map(() => 150), 0],
 *     meta: [
 *       {
 *         key: 'max_amount_restriction.0-three.month.9-2023',
 *         value: 120_00,
 *       },
 *       {
 *         key: 'max_amount_restriction.global.campaign.global',
 *         value: 120_00,
 *       },
 *     ],
 *   },
 * );
 */
export function generatePartialCarpools(count = 75, date = new Date("2022-01-01")): Partial<CarpoolInterface>[] {
  return [...Array(count + 1 + 1).slice(1).keys()].map((x) => ({
    // every 3rd carpool is the next day
    datetime: x % 3 == 0 ? date.setDate(date.getDate() + 1) && new Date(date) : new Date(date),
    distance: 25_000,
    driver_identity_key: "three",
    passenger_identity_key: uuidV4(),
  }));
}
