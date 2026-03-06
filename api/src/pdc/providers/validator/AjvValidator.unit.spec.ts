import { afterEach, assert, assertRejects, beforeEach, describe, it } from "@/dev_deps.ts";
import { InvalidParamsException } from "@/ilos/common/exceptions/index.ts";
import { ConfigInterfaceResolver } from "@/ilos/common/index.ts";
import sinon from "dev:sinon";
import { AjvValidator } from "./AjvValidator.ts";

describe("AjvValidator", () => {
  class FakeObject {
    constructor(data: Record<string, any>) {
      Reflect.ownKeys(data).forEach((key) => {
        // @ts-ignore untypable
        this[key] = data[key];
      });
    }
  }

  const fakeConfig = sinon.createStubInstance(ConfigInterfaceResolver, {
    get() {
      return {};
    },
  });

  const provider = new AjvValidator(fakeConfig);

  beforeEach(() => {
    provider.boot();
  });

  afterEach(() => {
    sinon.restore();
  });

  it("should work", async () => {
    const schema = {
      $schema: "http://json-schema.org/draft-07/schema#",
      $id: "myschema",
      type: "object",
      properties: {
        hello: {
          type: "string",
        },
      },
      required: ["hello"],
    };

    provider.registerValidator(schema, FakeObject);
    const result = await provider.validate(
      new FakeObject({ hello: "world" }),
    );
    assert(result);
  });

  it("should raise exception on invalid data", () => {
    const schema = {
      $schema: "http://json-schema.org/draft-07/schema#",
      $id: "myschema",
      type: "object",
      properties: {
        hello: {
          type: "string",
        },
      },
      required: ["hello"],
    };

    provider.registerValidator(schema, FakeObject);

    assertRejects(
      async () => await provider.validate(new FakeObject({ hello: 1 })),
      InvalidParamsException,
      "Invalid params",
    );
  });

  it("should work with ref", async () => {
    const subSchema = {
      $schema: "http://json-schema.org/draft-07/schema#",
      $id: "myschema.world",
      type: "object",
      properties: {
        world: {
          type: "string",
        },
      },
      required: ["world"],
    };
    const schema = {
      $schema: "http://json-schema.org/draft-07/schema#",
      $id: "myschema",
      type: "object",
      properties: {
        hello: {
          $ref: "myschema.world",
        },
      },
      required: ["hello"],
    };
    provider.registerValidator(subSchema);
    provider.registerValidator(schema, FakeObject);
    const result = await provider.validate(
      new FakeObject({ hello: { world: "!!!" } }),
    );
    assert(result);
  });

  it("should work with inheritance", async () => {
    class FakeObjectExtended extends FakeObject {}

    const schema = {
      $schema: "http://json-schema.org/draft-07/schema#",
      $id: "myschema",
      type: "object",
      properties: {
        hello: {
          type: "boolean",
        },
      },
      required: ["hello"],
    };

    const schemaExtended = {
      $schema: "http://json-schema.org/draft-07/schema#",
      $id: "myschema.extended",
      type: "object",
      properties: {
        hello: {
          type: "string",
        },
      },
      required: ["hello"],
    };

    provider.registerValidator(schema, FakeObject);
    provider.registerValidator(schemaExtended, FakeObjectExtended);

    const resultExtended = await provider.validate(
      new FakeObjectExtended({ hello: "world" }),
    );
    assert(resultExtended);
    const result = await provider.validate(
      new FakeObject({ hello: true }),
    );
    assert(result);
  });
});
