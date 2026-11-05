import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { DeleteOperatorAction } from "./DeleteOperatorAction.ts";

describe("DeleteOperatorAction (operator)", () => {
  it("deletes the operator's API keys before the soft delete", async () => {
    const calls: string[] = [];
    const repository = { delete: async (id: number) => void calls.push(`soft-delete:${id}`) };
    const dexClient = { deleteAllByOperator: async (id: number) => void calls.push(`keys:${id}`) };
    // deno-lint-ignore no-explicit-any
    const action = new DeleteOperatorAction(repository as any, dexClient as any);

    assertEquals(await action.handle({ _id: 5 }), true);
    assertEquals(calls, ["keys:5", "soft-delete:5"]);
  });
});
