export const alias = "exports.fail";
export const schema = {
  $id: alias,
  type: "object",
  additionalProperties: false,
  required: ["uuid", "message"],
  properties: {
    uuid: { type: "string", format: "uuid" },
    message: { type: "string", maxLength: 2000 },
  },
};
export const binding = [alias, schema];
