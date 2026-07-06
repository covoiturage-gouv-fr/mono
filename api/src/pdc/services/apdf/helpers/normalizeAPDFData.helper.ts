import { ExcelCampaignConfig } from "@/pdc/services/apdf/interfaces/ExcelTypes.ts";
import { format, toZonedTime } from "dep:date-fns-tz";
import { APDFTripInterface } from "../interfaces/APDFTripInterface.ts";

export function normalize(src: APDFTripInterface, config: ExcelCampaignConfig): APDFTripInterface {
  const sd = toZonedTime(src.start_datetime, config.tz);
  const ed = toZonedTime(src.end_datetime, config.tz);

  // format and convert to user timezone
  const sdf = format(sd, "yyyy-MM-dd'T'HH:mm:ssXXX", { timeZone: config.tz });
  const edf = format(ed, "yyyy-MM-dd'T'HH:mm:ssXXX", { timeZone: config.tz });

  // set the type of incentive depending on the date
  let type: "normale" | "booster" = "normale";
  const s = sdf.substring(0, 10);
  const e = edf.substring(0, 10);

  if ("booster_dates" in config) {
    // const booster_dates = config.booster_dates || '
    if (config.booster_dates.has(s) || config.booster_dates.has(e)) {
      type = "booster";
    }
  }

  // Set the booster date for specific use cases
  if ("extras" in config) {
    if ("idfm_oise2025" in config.extras) {
      const { start_date, end_date, coms } = config.extras.idfm_oise2025;
      const insee = new Set([...coms]);

      if (
        (isBetween(sd, start_date, end_date) || isBetween(ed, start_date, end_date)) &&
        (insee.has(src.start_insee) || insee.has(src.end_insee))
      ) {
        type = "booster";
      }
    }
  }

  const data: APDFTripInterface = {
    ...src,

    operator_journey_id: src.operator_journey_id && String(src.operator_journey_id).toUpperCase(),
    operator_trip_id: src.operator_trip_id && String(src.operator_trip_id).toUpperCase(),
    driver_operator_user_id: src.driver_operator_user_id && String(src.driver_operator_user_id).toUpperCase(),
    passenger_operator_user_id: src.passenger_operator_user_id && String(src.passenger_operator_user_id).toUpperCase(),
    rpc_journey_id: src.rpc_journey_id && String(src.rpc_journey_id).toUpperCase(),

    start_datetime: sdf,
    end_datetime: edf,

    // check if the trip is in regular or booster mode
    incentive_type: type,

    // incentives in euros
    rpc_incentive: src.rpc_incentive / 100,

    // declared operator incentive (territory funder) in euros
    operator_declared_incentive: src.operator_declared_incentive == null ? null : src.operator_declared_incentive / 100,

    // passenger contribution in euros
    passenger_contribution: src.passenger_contribution == null ? null : src.passenger_contribution / 100,
  };

  return data;
}

function isBetween(date: Date, start: Date, end: Date): boolean {
  return date >= start && date <= end;
}
