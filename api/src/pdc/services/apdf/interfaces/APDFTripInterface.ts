export interface APDFTripInterface {
  distance: number;
  driver_operator_user_id: string;
  duration: number;
  end_datetime: string;
  end_epci: string;
  end_insee: string;
  end_location: string;
  incentive_type: "normale" | "booster";
  operator_class: string;
  operator_journey_id: string;
  operator_trip_id: string;
  operator: string;
  operator_declared_incentive: number | null;
  passenger_contribution: number | null;
  passenger_operator_user_id: string;
  rpc_incentive: number;
  rpc_journey_id: string;
  start_datetime: string;
  start_epci: string;
  start_insee: string;
  start_location: string;
}
