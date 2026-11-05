import { assertEquals, assertRejects } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { DexClient } from "./DexClient.ts";

function setup(passwords: { email: string; username: string }[], failDelete = false) {
  const calls: string[] = [];
  const grpc = {
    ListPasswords: async () => ({ passwords }),
    DeletePassword: async ({ email }: { email: string }) => {
      if (failDelete) throw new Error("dex down");
      calls.push(`delete:${email}`);
    },
  };
  const revoked = {
    revoke: async (ids: string[]) => {
      calls.push(`revoke:${ids.join(",")}`);
    },
    expire: async (ids: string[]) => {
      calls.push(`expire:${ids.join(",")}`);
    },
  };
  class TestDexClient extends DexClient {
    // deno-lint-ignore no-explicit-any
    protected override async getClient(): Promise<any> {
      return grpc;
    }
  }
  // deno-lint-ignore no-explicit-any
  return { calls, client: new TestDexClient({} as any, revoked as any) };
}

const passwords = [
  { email: "k1", username: "operator.application:7" },
  { email: "k2", username: "operator.application:7" },
  { email: "k3", username: "operator.application:8" },
];

describe("DexClient", () => {
  it("revokes a key before deleting it, expires the mark after", async () => {
    const { calls, client } = setup(passwords);
    await client.deleteByOperator(7, "k2");
    assertEquals(calls, ["revoke:k2", "delete:k2", "expire:k2"]);
  });

  it("deletes every key of an operator, and only those", async () => {
    const { calls, client } = setup(passwords);
    await client.deleteAllByOperator(7);
    assertEquals(calls, ["revoke:k1,k2", "delete:k1", "expire:k1", "delete:k2", "expire:k2"]);
  });

  it("keeps a permanent revocation when Dex fails", async () => {
    const { calls, client } = setup(passwords, true);
    await assertRejects(() => client.deleteAllByOperator(7), Error, "dex down");
    assertEquals(calls, ["revoke:k1,k2"]);
  });
});
