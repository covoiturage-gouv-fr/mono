export const schema = {
  type: "object",
  additionalProperties: false,
  required: ["id"],
  properties: {
    id: {
      type: "string",
      format: "uuid",
    },
  },
};

export const alias = "exports.getDownloadLink";
export const binding = [alias, schema];
