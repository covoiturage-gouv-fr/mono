export const alias = "policy.updateDescriptiveSheetUrl";

export const schema = {
  $id: alias,
  type: "object",
  required: ["_id"],
  additionalProperties: false,
  properties: {
    _id: {
      type: "integer",
      minimum: 1,
    },
    descriptive_sheet_url: {
      type: ["string", "null"],
      maxLength: 512,
    },
  },
};

export const binding = [alias, schema];
