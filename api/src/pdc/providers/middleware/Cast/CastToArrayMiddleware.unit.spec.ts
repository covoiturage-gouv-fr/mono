// deno-fmt-ignore-file

import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { ContextType } from "@/ilos/common/index.ts";
import { CastToArrayMiddleware, HelperArgs } from "./CastToArrayMiddleware.ts";

// typescript brainf???

// FIXME - fails on props not listed in the helper args
type Input = [Record<string, unknown>, HelperArgs];
type Expected<TInput extends Input> = {
  [K in keyof TInput[0]]: K extends TInput[1][number] ? Array<TInput[0][K]>
    : TInput[0][K];
};

// setup a macro for concise test definitions
describe("CastToArrayMiddleware", () => {
  const caster = new CastToArrayMiddleware();
  const nextMock = <TParams>(
    params: TParams,
    _context: ContextType,
  ): TParams => params;
  const requestContext = { channel: { service: "test" } };

  async function macro(
    input: Input,
    expected: Expected<Input>,
  ): Promise<void> {
      const [params, helperArgs] = input;
      const result = await caster.process(
        params,
        requestContext,
        nextMock,
        helperArgs,
      );
      assertEquals(result, expected);
  }

  // test cases

  it("should cast int to array", () => macro([{ prop: 1 }, ["prop"]], { prop: [1] }));
  it("should cast str to array", () => macro([{ prop: "abc" }, ["prop"]], { prop: ["abc"], }));
  it("should cast null to array", () => macro([{ prop: null }, ["prop"]], { prop: [null], }));
  it("should not wrap existing array", () => macro([{ prop: [1] }, ["prop"]], { prop: [1], }));
  it("should cast multiple props A", () => macro([{ prop: 1, prop2: "abc" }, [ "prop", "prop2", ]], { prop: [1], prop2: ["abc"] }));
  
  // @ts-ignore -- Type 'undefined' is not assignable to type 'unknown[]'
  it("should skip undefined prop value", () => macro([{ prop: undefined }, ["prop"]], { prop: undefined }));

  // @ts-ignore -- prop3 fails on expected type
  it("should cast multiple props B", () => macro([{ prop: 1, prop2: "abc", prop3: "def" }, ["prop", "prop2"]], { prop: [1], prop2: ["abc"], prop3: "def" }));
  
  // @ts-ignore -- prop2 and prop3 fails on expected type
  it("should cast multiple props C", () => macro([{ prop: null, prop2: undefined, prop3: "def" }, ["prop", "prop2"]], { prop: [null], prop2: undefined, prop3: "def" }));
});
