export const alias = "company.fetch";
export const schema = {
  type: "object",
  required: ["siret"],
  additionalProperties: false,
  properties: {
    siret: {
      macro: "siret",
    },
  },
};
export const binding = [alias, schema];

export const aliasCommand = "company.fetch_command";
export const schemaCommand = {
  macro: "siret",
};
export const bindingCommand = [aliasCommand, schemaCommand];
