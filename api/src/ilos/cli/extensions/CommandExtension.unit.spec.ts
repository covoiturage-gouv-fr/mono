import sinon from "dep:sinon";
import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { serviceProvider as serviceProviderDecorator } from "@/ilos/common/Decorators.ts";
import { ServiceProvider as AbstractServiceProvider } from "@/ilos/core/index.ts";
import { command } from "../command.ts";
import { runCommand } from "../commander.ts";
import { CommandExtension } from "./CommandExtension.ts";

function setup() {
  const fake = sinon.fake();
  @command({
    signature: "hello <name>",
    description: "toto",
    options: [
      {
        signature: "-h, --hi",
        description: "Say hi",
      },
    ],
  })
  class BasicCommand {
    public async call(name: string, options?: any): Promise<string> {
      fake(name, options);
      return `Hello ${name}`;
    }
  }

  @serviceProviderDecorator({
    commands: [BasicCommand],
  })
  class ServiceProvider extends AbstractServiceProvider {
    override extensions = [CommandExtension];
  }

  const serviceProvider = new ServiceProvider();

  return { serviceProvider, fake };
}

describe("CommandExtension", () => {
  it("should work", async () => {
    const { serviceProvider, fake } = setup();
    await serviceProvider.register();
    await serviceProvider.init();
    await runCommand(serviceProvider, ["", "", "hello", "john"]);
    assertEquals(fake.getCalls().pop().args, ["john", {}]);
  });
});
