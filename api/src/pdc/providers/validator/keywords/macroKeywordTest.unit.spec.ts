import { assert, assertRejects } from "dep:assert";
import { beforeAll, describe, it } from "dep:testing-bdd";
import { Extensions } from "@/ilos/core/index.ts";
import { AjvValidator } from "@/pdc/providers/validator/index.ts";

import { macroKeyword } from "./macroKeyword.ts";

it("should return schema if macro exist", () => {
  assert(Reflect.ownKeys((macroKeyword as any).macro("uuid")).length > 0);
});

it("should return empty schema if macro doest not exist", () => {
  assert(Reflect.ownKeys((macroKeyword as any).macro("aa")).length === 0);
});

describe("macrokeyword", () => {
  const config = new Extensions.ConfigStore({});
  const validator = new AjvValidator(config);

  beforeAll(() => {
    validator.boot();
    validator.registerCustomKeyword({
      type: "keyword",
      definition: macroKeyword,
    });
  });

  it("base64", async () => {
    validator.registerValidator({ macro: "base64" }, "base64");

    assert(await validator.validate("anNvbnNjaGVtYQ==", "base64"));
    assert(await validator.validate("", "base64"));
    await assertRejects(() => validator.validate("*", "base64"));
  });

  it("dbid", async () => {
    validator.registerValidator({ macro: "dbid" }, "dbid");

    assert(await validator.validate("anNvbnNjaGVtYQ==", "dbid"));
    assert(
      await validator.validate(
        "P;Zk7WZPi}$?X#*?U(M7EE_AJN_!?;/=JLkZ_5Aw%857dW+mmZM9/hh.Wz.RuG*%",
        "dbid",
      ),
    );
    assert(
      await validator.validate("fb765772-0b28-4acd-b1b7-20ad958df863", "dbid"),
    );
    assert(await validator.validate(42, "dbid"));
    assert(await validator.validate(Number.MAX_SAFE_INTEGER, "dbid"));
    await assertRejects(() => validator.validate("", "dbid"));
    await assertRejects(() => validator.validate({}, "dbid"));
    await assertRejects(() =>
      validator.validate(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "dbid",
      )
    );
  });

  it("email", async () => {
    validator.registerValidator({ macro: "email" }, "email");

    assert(await validator.validate("jon.doe@example.com", "email"));
    assert(
      await validator.validate("jon.doe+somelabel@example.co.uk", "email"),
    );
    await assertRejects(() => validator.validate("a@b", "email"));
    await assertRejects(() => validator.validate("*", "email"));
  });

  it("permissions", async () => {
    validator.registerValidator({ macro: "permissions" }, "permissions");

    assert(await validator.validate(["common.user.list"], "permissions"));
    assert(await validator.validate(["userList.list"], "permissions"));
    await assertRejects(() =>
      validator.validate(
        ["userList.list", "<script>alert();</script>"],
        "permissions",
      )
    );
    await assertRejects(() => validator.validate(["user:list"], "permissions"));
    await assertRejects(() => validator.validate([424242], "permissions"));
  });

  it("serial", async () => {
    validator.registerValidator({ macro: "serial" }, "serial");

    assert(await validator.validate(42, "serial"));

    // 2147483647 is max int4 value in PostgreSQL
    assert(await validator.validate(2147483647, "serial"));
    await assertRejects(() => validator.validate(2147483647 + 1, "serial"));
  });

  it("timestamp", async () => {
    validator.registerValidator({ macro: "timestamp" }, "ts");

    assert(await validator.validate("2020-01-01T00:00:00Z", "ts"));
    assert(await validator.validate("2020-01-01 00:00:00Z", "ts"));
    assert(await validator.validate("2020-01-01T00:00:00+0100", "ts"));
    assert(await validator.validate("2020-01-01T00:00:00-0530", "ts"));
    await assertRejects(() => validator.validate("2020-01-01", "ts"));
    await assertRejects(() => validator.validate("2020/01/01", "ts"));
    await assertRejects(() => validator.validate("01/01/2020", "ts"));
  });

  it("uuid", async () => {
    validator.registerValidator({ macro: "uuid" }, "uuid");

    assert(
      await validator.validate("fb765772-0b28-4acd-b1b7-20ad958df863", "uuid"),
    );
    assert(
      await validator.validate("FB765772-0B28-4ACD-B1B7-20AD958DF863", "uuid"),
    );
    await assertRejects(() => validator.validate("abcd", "uuid"));
  });
});
