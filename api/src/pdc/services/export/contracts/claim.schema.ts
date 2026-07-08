export const alias = "exports.claim";
export const schema = {
  $id: alias,
  type: "object",
  additionalProperties: false,
  required: ["targets"],
  properties: {
    targets: {
      type: "array",
      items: { type: "string", enum: ["operator", "territory"] },
      minItems: 1,
    },
  },
};
export const binding = [alias, schema];
