import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";
import { IFilterXSSOptions } from "dep:xss";

import { sanitizeKeyword } from "./sanitizeKeyword.ts";

function test(
  config: boolean | IFilterXSSOptions,
  input: any,
  expected: string,
) {
  return () => {
    const xss = (sanitizeKeyword as any).compile(config);

    // ctx is here to fake the context passed by AJV
    const ctx = { parentData: { foo: input }, parentDataProperty: "foo" };
    xss(input, ctx);

    assertEquals((ctx.parentData as any)[ctx.parentDataProperty], expected);
  };
}

// test the default config (strips all tags)
// @source https://github.com/leizongmin/js-xss/blob/master/test/test_xss.js
it("[text/plain] undefined", test(true, undefined, ""));
it("[text/plain] null", test(true, null, ""));
it("[text/plain] number", test(true, 123, "123"));
it("[text/plain] object", test(true, { a: 1111 }, "[object Object]"));

it(
  "#XSS_Filter_Evasion_Cheat_Sheet",
  test(
    true,
    "></SCRIPT>\">'><SCRIPT>alert(String.fromCharCode(88,83,83))</SCRIPT>",
    "&gt;\"&gt;'&gt;",
  ),
);

// test a custom config
it(
  "[text/html] <a> tag allowed",
  test(
    {},
    '<a href="https://example.com">content</a>',
    '<a href="https://example.com">content</a>',
  ),
);

it(
  "[text/html] <a> tag denied",
  test(
    { whiteList: {}, stripIgnoreTag: true },
    '<a href="https://example.com">content</a>',
    "content",
  ),
);

it(
  "[text/html] <form> tag",
  test(
    {},
    '<form action="https://example.com">form content</form>',
    '&lt;form action="https://example.com"&gt;form content&lt;/form&gt;',
  ),
);
