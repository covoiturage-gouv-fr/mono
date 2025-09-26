import { ServiceContainerInterface } from "@/ilos/common/index.ts";
import { CommandOptions } from "@/ilos/common/types/command/CommandInterface.ts";
import { CommandInterface } from "@/ilos/common/types/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { Command as Runner } from "dep:commander";
import { identifierCfg, identifierList } from "../cli/constants.ts";

function registerCommand(
  runner: Runner,
  cmd: CommandOptions,
  processCommand: (...args: unknown[]) => Promise<unknown>,
): void {
  const splitSignature = cmd.signature.split(" ");
  const command = runner.command(splitSignature[0]);
  for (const arg of splitSignature.slice(1)) {
    command.argument(arg);
  }

  if (cmd.description) {
    command.description(cmd.description);
  }

  if (cmd.options && cmd.options.length) {
    for (const option of cmd.options) {
      const { signature, description, coerce, default: def } = option;
      const args = [];
      if (typeof coerce === "function") {
        args.push(coerce);
      }
      if (def !== undefined) {
        args.push(def);
      }
      command.option(signature, description, ...args);
    }
  }

  command.action(async (...args: unknown[]) => {
    try {
      const result = await processCommand(...args);
      if (result) {
        logger.info(result);
      }
    } catch (e) {
      logger.error(e);
      throw e;
    }
  });
}

function readMetaCmdMeta(cmd: CommandInterface): CommandOptions {
  return Reflect.getMetadata(identifierCfg, cmd.constructor);
}

export async function runCommand(serviceContainer: ServiceContainerInterface, argv?: string[]) {
  const commands = await serviceContainer.getContainer().getAllAsync<() => CommandInterface>(identifierList);
  const runner = new Runner();
  for (const cmdGetter of commands) {
    const cmd = cmdGetter();
    const cmdOptions = readMetaCmdMeta(cmd);
    if (!cmdOptions) {
      continue;
    }
    registerCommand(runner, cmdOptions, (...args: unknown[]) => cmd.call(...args));
  }
  return runner.parseAsync(argv);
}
