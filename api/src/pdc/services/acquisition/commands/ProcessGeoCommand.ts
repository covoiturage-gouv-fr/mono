import { coerceDate, coerceInt } from "@/ilos/cli/index.ts";
import { command, CommandInterface } from "@/ilos/common/index.ts";
import { getPerformanceTimer } from "@/lib/logger/index.ts";
import { taggedLogger } from "@/lib/logger/tagged.ts";
import { CarpoolAcquisitionService } from "@/pdc/providers/carpool/index.ts";
import { ProcessGeoCursor } from "@/pdc/providers/carpool/interfaces/index.ts";
import { subDays } from "dep:date-fns";

const log = taggedLogger("[geo]", { maxLength: 512 });
const perSecond = (count: number, ms: number): number => Math.round((count * 1000) / Math.max(ms, 1));

type Options = {
  loop: boolean;
  size: number;
  after?: Date;
  until?: Date;
  lastDays?: number;
  failed: boolean;
};

@command({
  signature: "acquisition:geo",
  description: "Process acquisition geo",
  options: [
    {
      signature: "-l, --loop",
      description: "Process acquisition while remaining",
      default: false,
    },
    {
      signature: "-s, --size <size>",
      description: "Batch size",
      coerce: coerceInt,
      default: 100,
    },
    {
      signature: "-a, --after <after>",
      description: "Start date",
      coerce: coerceDate,
    },
    {
      signature: "-u, --until <until>",
      description: "End date",
      coerce: coerceDate,
    },
    {
      signature: "-d, --last-days <days>",
      description: "Process x last days from now (default: 1, without --after/--until)",
      coerce: coerceInt,
    },
    {
      signature: "-f, --failed",
      description: "Process failed geo only",
      default: false,
    },
  ],
})
export class ProcessGeoCommand implements CommandInterface {
  protected readonly MAX_BATCH_SIZE = 10000;

  constructor(protected carpool: CarpoolAcquisitionService) {}

  public async call(opts: Options): Promise<void> {
    const options = this.validateOptions(opts);

    const now = new Date();
    const to = options.until ?? now;
    const from = options.after ?? subDays(now, options.lastDays ?? 1);
    log.info("start", {
      from: from.toISOString(),
      to: to.toISOString(),
      size: options.size,
      loop: options.loop,
      failed_only: options.failed,
    });

    const timer = getPerformanceTimer();
    let batch = 0;
    let total = 0;
    let totalFailed = 0;
    let shouldContinue: boolean;
    let cursor: ProcessGeoCursor | undefined;

    do {
      const subtimer = getPerformanceTimer();
      const result = await this.carpool.processGeo({
        batchSize: options.size,
        failedOnly: options.failed,
        from,
        to,
        after: cursor,
      });
      const { count, failed } = result;
      cursor = result.cursor;
      batch++;
      total += count;
      totalFailed += failed;
      if (count > 0) {
        const ms = timer.stop();
        log.info("batch", {
          batch,
          count,
          failed,
          total,
          total_failed: totalFailed,
          cursor: cursor?.start_datetime.toISOString(),
          ms: Math.round(subtimer.stop()),
          per_s: perSecond(total, ms),
        });
      }
      shouldContinue = count === options.size;
    } while (shouldContinue && options.loop);

    const ms = timer.stop();
    log.info("done", {
      batches: batch,
      total,
      total_failed: totalFailed,
      ms: Math.round(ms),
      per_s: perSecond(total, ms),
    });
  }

  protected validateOptions(opts: Options): Options {
    if (opts.after && opts.until && opts.after > opts.until) {
      throw new Error("Start date cannot be after end date");
    }

    if (opts.lastDays !== undefined && opts.after) {
      throw new Error("Cannot use last days and start date at the same time");
    }

    if (opts.lastDays !== undefined && opts.until) {
      throw new Error("Cannot use last days and end date at the same time");
    }

    if (opts.until && !opts.after) {
      throw new Error("End date requires a start date");
    }

    if (opts.lastDays !== undefined && opts.lastDays < 1) {
      throw new Error("Last days must be greater than 0");
    }

    if (opts.size && opts.size <= 1) {
      throw new Error("Batch size must be greater than 1");
    }

    if (opts.size && opts.size > this.MAX_BATCH_SIZE) {
      throw new Error(`${this.MAX_BATCH_SIZE} is a reasonable maximum batch size`);
    }

    return opts;
  }
}
