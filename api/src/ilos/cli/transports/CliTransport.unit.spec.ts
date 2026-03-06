import { assertEquals, describe, it } from "@/dev_deps.ts";
import {
  command as commandDecorator,
  ExtensionInterface,
  kernel as kernelDecorator,
  NewableType,
} from "@/ilos/common/index.ts";
import { Kernel } from "@/ilos/core/index.ts";
import sinon from "dep:sinon";
import { CommandExtension } from "../extensions/CommandExtension.ts";
import { CliTransport } from "./CliTransport.ts";

describe("Cli transport", () => {
  const fake = sinon.fake();
  @commandDecorator({
    signature: "hello <name>",
    description: "The hello world command",
    options: [
      {
        signature: "-h, --hi",
        description: "Say hi",
      },
    ],
  })
  class BasicCommand {
    public async call(name: string, options?: any): Promise<void> {
      fake(name, options);
    }
  }

  @kernelDecorator({
    commands: [BasicCommand],
  })
  class BasicKernel extends Kernel {
    override readonly extensions: NewableType<ExtensionInterface>[] = [CommandExtension];
  }

  const kernel = new BasicKernel();
  it("should work", async () => {
    await kernel.bootstrap();
    const cliTransport = new CliTransport(kernel);
    await cliTransport.up(["", "", "hello", "john"]);
    assertEquals(fake.getCalls().pop().args, ["john", {}]);
  });
});
