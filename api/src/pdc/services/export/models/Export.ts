import { ContextType } from "@/ilos/common/index.ts";
import { ExportParams } from "./ExportParams.ts";

export enum ExportStatus {
  PENDING = "pending",
  RUNNING = "running",
  UPLOADING = "uploading",
  UPLOADED = "uploaded",
  NOTIFY = "notify",
  SUCCESS = "success",
  FAILURE = "failure",
}
export enum ExportTarget {
  DATAGOUV = "datagouv",
  OPERATOR = "operator",
  TERRITORY = "territory",
}

export type ExportError = string;

export class Export {
  public _id: number;
  public uuid: string;
  public target: ExportTarget;
  public status: ExportStatus;
  public created_by: number;
  public filename: string;
  public file_size: number;
  public params: ExportParams;
  public error: ExportError; // JSON object

  public static fromJSON(data: any): Export {
    const export_ = new Export();
    export_._id = data._id;
    export_.uuid = data.uuid;
    export_.target = data.target;
    export_.status = data.status;
    export_.created_by = data.created_by;
    export_.filename = data.filename;
    export_.file_size = Number(data.file_size) || 0;
    export_.params = new ExportParams(data.params);
    export_.error = data.error;
    return export_;
  }

  public static toJSON(export_: Export): any {
    return {
      _id: export_._id,
      uuid: export_.uuid,
      target: export_.target,
      status: export_.status,
      created_by: export_.created_by,
      filename: export_.filename,
      file_size: export_.file_size,
      params: ExportParams.toJSON(export_.params),
      error: export_.error,
    };
  }

  public static target(
    context: ContextType,
    target: ExportTarget | null = null,
  ): ExportTarget {
    if (target) return target;

    const { operator_id, territory_id } = context.call?.user || {};

    // operator
    if (parseInt(operator_id) > 0) return ExportTarget.OPERATOR;

    // territory
    if (parseInt(territory_id) > 0) return ExportTarget.TERRITORY;

    // registry
    if (
      Number.isNaN(parseInt(operator_id)) &&
      Number.isNaN(parseInt(territory_id))
    ) {
      return ExportTarget.TERRITORY;
    }

    return ExportTarget.DATAGOUV;
  }
}
