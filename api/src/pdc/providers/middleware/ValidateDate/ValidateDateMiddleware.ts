import {
  ContextType,
  InvalidParamsException,
  middleware,
  MiddlewareInterface,
  ParamsType,
  ResultType,
} from "@/ilos/common/index.ts";
import { get, set } from "@/lib/object/index.ts";
import { endOfDay, startOfDay } from "dep:date-fns";
import { NextFunction } from "dep:express";
import { ConfiguredMiddleware } from "../interfaces.ts";

/*
 * Check date validity
 */
@middleware()
export class ValidateDateMiddleware
  implements MiddlewareInterface<ValidateDateMiddlewareParams> {
  async process(
    params: ParamsType,
    context: ContextType,
    next: NextFunction,
    options: ValidateDateMiddlewareParams,
  ): Promise<ResultType> {
    if (
      !options.startPath || !options.endPath ||
      Reflect.ownKeys(options).length < 3
    ) {
      throw new InvalidParamsException("Middleware is not properly configured");
    }

    const {
      startPath,
      endPath,
      minStart: minStartFn,
      maxEnd: maxEndFn,
      applyDefault: applyDefaultOpt,
    } = options;
    const minStart: Date | undefined = minStartFn
      ? startOfDay(minStartFn(params, context))
      : undefined;
    const maxEnd: Date | undefined = maxEndFn
      ? endOfDay(maxEndFn(params, context))
      : undefined;
    const applyDefault = applyDefaultOpt ?? false;
    const startDate = get<ParamsType, Date>(params, startPath, undefined) ?? undefined;
    const endDate = get<ParamsType, Date>(params, endPath, undefined) ?? undefined;

    if (startDate && endDate && startDate > endDate) {
      throw new InvalidParamsException("Start should be before end");
    }

    if (
      minStart &&
      ((startDate && startDate < minStart) || (!startDate && !applyDefault))
    ) {
      throw new InvalidParamsException(
        `Start should be after ${minStart.toDateString()}`,
      );
    }

    if (
      maxEnd && ((endDate && endDate > maxEnd) || (!endDate && !applyDefault))
    ) {
      throw new InvalidParamsException(
        `End should be before ${maxEnd.toDateString()}`,
      );
    }

    if (applyDefault) {
      if (!startDate) {
        set(params, startPath, minStart);
      }
      if (!endDate) {
        set(params, endPath, maxEnd);
      }
    }

    return next(params, context);
  }
}

export type ValidateDateMiddlewareParams = {
  startPath: string;
  endPath: string;
  minStart?: (params: ParamsType, context: ContextType) => Date;
  maxEnd?: (params: ParamsType, context: ContextType) => Date;
  applyDefault?: boolean;
};

const alias = "validate.date";

export const validateDateMiddlewareBinding = [alias, ValidateDateMiddleware];

/**
 * Validate start and end date inputs. Make sure start is before end.
 * Apply limits and defaults.
 *
 * @param startPath - Path to the start date in the params object
 * @param endPath - Path to the end date in the params object
 * @param minStart - Minimum start date. If the start date is before this date, an error is thrown
 * @param maxEnd - Maximum end date. If the end date is after this date, an error is thrown
 * @param applyDefault - If true, set the start date to minStart and end date to maxEnd if they are not provided
 *
 * @example
 * middlewares: [
 *  validateDateMiddleware({ startPath: "start_at", endPath: "end_at" }),
 * ],
 *
 * @example
 * middlewares: [
 *   validateDateMiddleware({
 *     startPath: "start_at",
 *     endPath: "end_at",
 *     minStart: () => new Date(new Date().getTime() - minStartDefault),
 *     maxEnd: () => new Date(new Date().getTime() - maxEndDefault),
 *     applyDefault: true,
 *   }),
 * ],
 */
export function validateDateMiddleware(
  params: ValidateDateMiddlewareParams,
): ConfiguredMiddleware<ValidateDateMiddlewareParams> {
  return [alias, params];
}
