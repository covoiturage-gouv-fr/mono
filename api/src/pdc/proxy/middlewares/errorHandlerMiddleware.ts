import { logger } from "@/lib/logger/index.ts";
import { NextFunction, Request, Response } from "dep:express";

// error handler - !! keep the next argument !!
// otherwise Express doesn't use it as error handler
// https://expressjs.com/en/guide/error-handling.html
export function errorHandlerMiddleware(
  err: Error,
  _req: Request,
  res: Response,
  _next: NextFunction,
): void {
  let code: number;

  switch (err.message) {
    case "Bad Request Error":
    case "Bad Request":
    case "Validation Error":
    case "Validation":
      code = 400;
      break;

    case "Unauthorized Error":
    case "Unauthorized":
      code = 401;
      break;

    case "Forbidden Error":
    case "Forbidden":
      code = 403;
      break;

    case "Not Found Error":
    case "Not Found":
      code = 404;
      break;

    case "Conflict Error":
    case "Conflict":
      code = 409;
      break;

    case "Too Many Requests Error":
    case "Too Many Requests":
      code = 429;
      break;

    case "Internal Server Error":
      code = 500;
      break;

    default:
      code = 500;
  }

  try {
    const { id, method } = Array.isArray(_req.body) ? _req.body.pop() : _req.body;

    logger.error(
      `[errorHandler] ${err.name} ${code} ${err.message}\n${err.stack}`,
      { id, method },
    );
  } catch (e) {}

  if (res.headersSent) return;

  // Hide internal error details from clients on 500 responses
  const isInternal = code === 500;
  res.status(code).json({
    id: 1,
    jsonrpc: "2.0",
    error: {
      code,
      data: isInternal ? "Error" : err.name,
      message: isInternal ? "Internal Server Error" : err.message,
    },
  });
}
