import type {
  TerritoryCodeInterface,
  TerritorySelectorsInterface,
} from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";
import { TerritoryCodeEnum } from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";
import type { BoundedSlices, SliceInterface, UnboundedSlices } from "../../contracts/common/interfaces/Slices.ts";

export { IncentiveStateEnum, IncentiveStatusEnum } from "./IncentiveInterface.ts";
export type {
  CommonIncentiveInterface,
  SerializedIncentiveInterface,
  StatefulIncentiveInterface,
  StatelessIncentiveInterface,
} from "./IncentiveInterface.ts";
export { MetadataLifetime } from "./MetadataInterface.ts";
export type {
  MetadataAccessorInterface,
  MetadataRegistryInterface,
  MetadataStoreInterface,
  MetadataVariableDefinitionInterface,
  SerializedAccessibleMetadataInterface,
  SerializedMetadataVariableDefinitionInterface,
  SerializedStoredMetadataInterface,
  StoredMetadataVariableInterface,
} from "./MetadataInterface.ts";
export { OperatorsEnum } from "./OperatorsEnum.ts";
export type {
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  PolicyHandlerStaticInterface,
  PolicyInterface,
  SerializedPolicyInterface,
  StatefulContextInterface,
  StatelessContextInterface,
  StatelessRuleHelper,
} from "./PolicyInterface.ts";
export { TerritoryCodeEnum, TerritoryCodeInterface, TerritorySelectorsInterface };
export type { BoundedSlices, SliceInterface, UnboundedSlices };

export interface CarpoolMetaInterface {
  calc_distance?: number;
  calc_duration?: number;
  payments?: Array<
    {
      index?: number;
      amount?: number;
      siret?: string;
      type?: "incentive" | "payment";
    }
  >;
}

export interface CarpoolInterface {
  passenger_contribution: number;
  passenger_identity_key: string;
  passenger_has_travel_pass: boolean;
  passenger_is_over_18: boolean | null;
  passenger_meta?: CarpoolMetaInterface;
  driver_revenue: number;
  driver_identity_key: string;
  driver_has_travel_pass: boolean;
  driver_meta?: CarpoolMetaInterface;
  operator_journey_id: string;
  operator_id: number;
  operator_trip_id?: string;
  operator_uuid: string;
  operator_class: string;
  datetime: Date;
  seats: number;
  distance: number;
  start: TerritoryCodeInterface;
  start_lat: number;
  start_lon: number;
  end: TerritoryCodeInterface;
  end_lat: number;
  end_lon: number;
  operator_incentives?: Array<{ siret: string; amount: number }>;
}
