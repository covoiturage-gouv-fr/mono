import { assertObjectMatch } from "dep:assert";
import { it } from "dep:testing-bdd";

import { mapResults } from "./dataWrapMiddleware.ts";

it("[mapResults] skips on missing results", () => {
  const payload = {
    id: 1,
    jsonrpc: "2.0",
    error: {
      data: null,
      message: "Server error",
      code: -32000,
    },
  };

  assertObjectMatch(mapResults(payload), payload);
});

it("[mapResults] returns doc on existing data/meta", () => {
  const payload = {
    id: 1,
    jsonrpc: "2.0",
    result: {
      data: {
        data: "meta",
      },
      meta: {
        meta: "data",
      },
    },
  };

  assertObjectMatch(mapResults(payload), payload);
});

it("[mapResults] returns doc with added meta: null if missing", () => {
  const payload = {
    id: 1,
    jsonrpc: "2.0",
    result: {
      data: {
        data: "meta",
      },
    },
  };

  const expectation = {
    id: 1,
    jsonrpc: "2.0",
    result: {
      meta: null,
      data: {
        data: "meta",
      },
    },
  };

  assertObjectMatch(mapResults(payload), expectation);
});

it("[mapResults] wraps result with data/meta if missing", () => {
  const payload = {
    id: 1,
    jsonrpc: "2.0",
    result: {
      _id: 1234,
    },
  };

  const expectation = {
    id: 1,
    jsonrpc: "2.0",
    result: {
      meta: null,
      data: {
        _id: 1234,
      },
    },
  };

  assertObjectMatch(mapResults(payload), expectation);
});

it("[mapResults] succeeds on non-object results (boolean)", () => {
  const payload = {
    id: 1,
    jsonrpc: "2.0",
    result: true,
  };

  const expectation = {
    id: 1,
    jsonrpc: "2.0",
    result: {
      meta: null,
      data: true,
    },
  };

  assertObjectMatch(mapResults(payload), expectation);
});

it("[mapResults] succeeds on non-object results (string)", () => {
  const payload = {
    id: 1,
    jsonrpc: "2.0",
    result: "Hello World!",
  };

  const expectation = {
    id: 1,
    jsonrpc: "2.0",
    result: {
      meta: null,
      data: "Hello World!",
    },
  };

  assertObjectMatch(mapResults(payload), expectation);
});
