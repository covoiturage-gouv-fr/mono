import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { config } from "../config/index.ts";
import { LastRecord } from "../dto/LastRecord.ts";
import { LastRecordRepositoryInterfaceResolver } from "../interfaces/LastRecordRepositoryProviderInterface.ts";

export type ResultInterface = { year: number; month: number } | null;

@handler({
  service: "observatory",
  method: "getLastRecord",
  middlewares: [[
    "validate",
    LastRecord,
  ]],
  apiRoute: {
    path: "/observatory/last-record",
    method: "GET",
    public: true,
  },
})
export class LastRecordAction extends AbstractAction {
  constructor(private repository: LastRecordRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(params: LastRecord): Promise<ResultInterface> {
    const cutoff = config.observatory.publishedUntil;
    let maxYear: number | undefined;
    let maxMonth: number | undefined;

    if (cutoff) {
      const parts = cutoff.split("-").map(Number);
      if (parts.length >= 2 && !parts.some(isNaN)) {
        // Cutoff is exclusive: 2026-03-01 means last valid month is Feb 2026
        const d = new Date(parts[0], parts[1] - 1, parts[2] || 1);
        d.setMonth(d.getMonth() - 1);
        maxYear = d.getFullYear();
        maxMonth = d.getMonth() + 1;
      }
    }

    return this.repository.getLastRecord(params, maxYear, maxMonth);
  }
}
