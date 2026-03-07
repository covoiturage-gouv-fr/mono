import { ContextType } from "@/ilos/common/index.ts";
import { assertEquals, assertRejects } from "dep:assert";
import { KernelContext } from "./helpers.ts";

interface HandlerConfigInterface {
  service: string;
  method: string;
}

const emptyContext: ContextType = {
  call: {
    user: {},
  },
  channel: {
    service: "",
  },
};

export async function assertHandler<P = unknown, R = unknown>(
  context: KernelContext,
  currentContext: ContextType = {} as ContextType,
  handlerConfig: HandlerConfigInterface,
  params: P,
  callback: (result: R) => void | Promise<void>,
) {
  const callContext = { ...emptyContext, ...currentContext };
  const result = await context.kernel.call<P, R>(
    `${handlerConfig.service}:${handlerConfig.method}`,
    params,
    callContext,
  );
  await callback(result);
}

export async function assertSuccessHandler<P = unknown, R = unknown>(
  context: KernelContext,
  handlerConfig: HandlerConfigInterface,
  params: P,
  response: R,
  currentContext = {},
) {
  const callContext = {
    ...emptyContext,
    ...currentContext,
  };
  const result = await context.kernel.call<P, R>(
    `${handlerConfig.service}:${handlerConfig.method}`,
    params,
    callContext,
  );

  assertEquals(result, response);
}

export async function assertErrorHandler<P = unknown, R = unknown>(
  context: KernelContext,
  handlerConfig: HandlerConfigInterface,
  params: P,
  _response: R,
  currentContext = {},
) {
  const callContext = {
    ...emptyContext,
    ...currentContext,
  };

  await assertRejects(async () =>
    context.kernel.call<P, R>(
      `${handlerConfig.service}:${handlerConfig.method}`,
      params,
      callContext,
    )
  );
}
