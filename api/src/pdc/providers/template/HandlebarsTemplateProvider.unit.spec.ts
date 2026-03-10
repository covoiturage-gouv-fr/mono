import { assertEquals, assertThrows } from "dep:assert";
import { it } from "dep:testing-bdd";
import { injectable } from "@/ilos/common/index.ts";
import { AbstractTemplate, HandlebarsTemplateProvider } from "./index.ts";

interface TestTemplateData {
  word: string;
}

@injectable()
class TestTemplate extends AbstractTemplate<TestTemplateData> {
  static template = `Hello {{word}}!`;
}

@injectable()
class TestErrorTemplate extends AbstractTemplate<TestTemplateData> {
  static template = `Hello {{#not_exists}}!`;
}

it("should work", () => {
  const processor = new HandlebarsTemplateProvider();
  processor.init();
  const template = new TestTemplate({
    word: "world",
  });

  const rendered = processor.render(template);
  assertEquals(rendered, "Hello world!");
});
it("should work with cache", () => {
  const processor = new HandlebarsTemplateProvider();
  processor.init();
  const template = new TestTemplate({
    word: "world",
  });

  processor.render(template);
  const rendered = processor.render(template);

  assertEquals(rendered, "Hello world!");
});

it("should throw if template has error", async () => {
  const processor = new HandlebarsTemplateProvider();
  processor.init();
  const template = new TestErrorTemplate({
    word: "world",
  });
  assertThrows(() => processor.render(template));
});
