
export const schema = {
  type: "object",
  additionalProperties: false,
  required: [],
  properties: {
    days: {
      type: "integer",
      minimum: 1,
    },
  },
};

export const alias = "exports.list";
export const binding = [alias, schema];
