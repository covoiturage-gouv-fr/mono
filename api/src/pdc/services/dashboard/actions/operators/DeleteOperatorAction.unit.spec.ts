import { assertEquals, assertRejects } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { DeleteOperatorAction } from "./DeleteOperatorAction.ts";

function setup(failDex = false) {
  const calls: string[] = [];
  const repository = {
    deleteOperator: async ({ id }: { id: number }) => {
      calls.push(`soft-delete:${id}`);
      return { success: true, message: `Operator ${id} deleted` };
    },
  };
  const dexClient = {
    deleteAllByOperator: async (id: number) => {
      if (failDex) throw new Error("dex down");
      calls.push(`keys:${id}`);
    },
  };
  // deno-lint-ignore no-explicit-any
  return { calls, action: new DeleteOperatorAction(repository as any, dexClient as any) };
}

describe("DeleteOperatorAction (dashboard)", () => {
  it("deletes the operator's API keys before the soft delete", async () => {
    const { calls, action } = setup();
    await action.handle({ id: 3 });
    assertEquals(calls, ["keys:3", "soft-delete:3"]);
  });

  it("keeps the operator when its keys cannot be deleted", async () => {
    const { calls, action } = setup(true);
    await assertRejects(() => action.handle({ id: 3 }), Error, "dex down");
    assertEquals(calls, []);
  });
});
