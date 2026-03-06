import { assertEquals, assertRejects } from "dep:assert";
import { it } from "dep:testing-bdd";
import { ContextType } from "@/ilos/common/index.ts";

import {
  ValidateDateMiddleware,
  ValidateDateMiddlewareParams,
} from "./ValidateDateMiddleware.ts";

async function process(
  middlewareParams: ValidateDateMiddlewareParams,
  callParams: any = {
    date: {
      start: new Date("2020-06-01"),
      end: new Date("2020-12-01"),
    },
  },
) {
  const context: ContextType = {
    call: {
      user: {},
    },
    channel: {
      service: "",
    },
  };

  const middleware = new ValidateDateMiddleware();
  const next = (params: any, context: any) => ({ params, context });
  return middleware.process(callParams, context, next, middlewareParams);
}

it("Middleware ValidateDate: should throw if start > end", async () => {
  const middlewareParams = {
    startPath: "date.start",
    endPath: "date.end",
    minStart: () => new Date(),
    maxEnd: () => new Date(),
    applyDefault: true,
  };

  const callParams = {
    date: {
      start: new Date("2021-01-01"),
      end: new Date("2020-01-01"),
    },
  };

  await assertRejects(
    async () => process(middlewareParams, callParams),
    "Start should be before end",
  );
});

it("Middleware ValidateDate: should throw if start < minstart", async () => {
  const middlewareParams = {
    startPath: "date.start",
    endPath: "date.end",
    minStart: () => new Date("2021-01-01"),
    maxEnd: () => new Date("2021-02-01"),
    applyDefault: true,
  };
  await assertRejects(
    async () => process(middlewareParams),
    `Start should be after ${middlewareParams.minStart().toDateString()}`,
  );
});

it("Middleware ValidateDate: should throw if start not exist with minStart", async () => {
  const middlewareParams = {
    startPath: "date.wrongstart",
    endPath: "date.end",
    minStart: () => new Date("2019-01-01"),
    maxEnd: () => new Date("2021-02-01"),
    applyDefault: false,
  };
  await assertRejects(
    async () => process(middlewareParams),
    `Start should be after ${middlewareParams.minStart().toDateString()}`,
  );
});

it("Middleware ValidateDate: should throw if end > maxEnd", async () => {
  const middlewareParams = {
    startPath: "date.start",
    endPath: "date.end",
    minStart: () => new Date("2019-01-01"),
    maxEnd: () => new Date("2020-02-01"),
    applyDefault: true,
  };
  await assertRejects(
    async () => process(middlewareParams),
    `End should be before ${middlewareParams.maxEnd().toDateString()}`,
  );
});

it("Middleware ValidateDate: should throw if end not exist with maxEnd", async () => {
  const middlewareParams = {
    startPath: "date.start",
    endPath: "date.wrongend",
    minStart: () => new Date("2019-01-01"),
    maxEnd: () => new Date("2021-02-01"),
    applyDefault: false,
  };

  await assertRejects(
    async () => process(middlewareParams),
    `End should be before ${middlewareParams.maxEnd().toDateString()}`,
  );
});

it("Middleware ValidateDate: should apply default if start missing", async () => {
  const middlewareParams = {
    startPath: "date.start",
    endPath: "date.end",
    minStart: () => new Date("2019-01-01"),
    maxEnd: () => new Date("2021-02-01"),
    applyDefault: true,
  };

  const callParams = {
    date: {
      end: new Date("2020-01-01"),
    },
  };

  const result = await process(middlewareParams, callParams);
  assertEquals(
    result.params.date.start.toDateString(),
    middlewareParams.minStart().toDateString(),
  );
  assertEquals(
    (callParams.date as any).start.toDateString(),
    middlewareParams.minStart().toDateString(),
  );
});

it("Middleware ValidateDate: should apply default if end missing", async () => {
  const middlewareParams = {
    startPath: "date.start",
    endPath: "date.end",
    minStart: () => new Date("2019-01-01"),
    maxEnd: () => new Date("2021-02-01"),
    applyDefault: true,
  };

  const callParams = {
    date: {
      start: new Date("2020-01-01"),
    },
  };

  const result = await process(middlewareParams, callParams);
  assertEquals(
    result.params.date.end.toDateString(),
    middlewareParams.maxEnd().toDateString(),
  );
  assertEquals(
    (callParams.date as any).end.toDateString(),
    middlewareParams.maxEnd().toDateString(),
  );
});
