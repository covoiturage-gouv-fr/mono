import { TerritoryCodeEnum } from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";
import { territoryCodeSchema } from "@/pdc/services/territory/contracts/common/schema.ts";

const geoSelectorSchema = {
  ...territoryCodeSchema,
  propertyNames: { enum: [...Object.values(TerritoryCodeEnum), "custom"] },
  properties: {
    custom: {
      type: "array",
      maxItems: 64,
      items: { type: "string", pattern: "^[1-9][0-9]{0,9}$" },
    },
  },
};

export const schemaV3 = {
  type: "object",
  additionalProperties: false,
  required: ["tz", "start_at", "end_at"],
  properties: {
    tz: {
      macro: "tz",
    },
    start_at: {
      macro: "timestamp",
    },
    end_at: {
      macro: "timestamp",
    },
    created_by: {
      macro: "serial",
    },
    operator_id: {
      type: "array",
      minItems: 0,
      maxItems: 128,
      items: { macro: "serial" },
    },
    territory_id: {
      type: "array",
      minItems: 0,
      maxItems: 1024,
      items: { macro: "serial" },
    },
    geo_selector: geoSelectorSchema,
  },
};

export const aliasV3 = "export.create.v3";
export const bindingV3 = [aliasV3, schemaV3];
