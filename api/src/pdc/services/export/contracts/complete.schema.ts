export const alias = "exports.complete";
export const schema = {
  $id: alias,
  type: "object",
  additionalProperties: false,
  required: ["uuid", "file_size"],
  properties: {
    uuid: { type: "string", format: "uuid" },
    file_size: { type: "integer", minimum: 0 },
  },
};
export const binding = [alias, schema];
