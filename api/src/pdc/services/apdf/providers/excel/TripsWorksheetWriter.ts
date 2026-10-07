import { provider } from "@/ilos/common/index.ts";
import { Cursor } from "@/ilos/connection-postgres/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { ExcelCampaignConfig } from "@/pdc/services/apdf/interfaces/ExcelTypes.ts";
import excel from "dep:excel";
import { normalize } from "../../helpers/normalizeAPDFData.helper.ts";
import { APDFTripInterface } from "../../interfaces/APDFTripInterface.ts";
import { AbstractWorksheetWriter } from "./AbstractWorksheetWriter.ts";

@provider()
export class TripsWorksheetWriter extends AbstractWorksheetWriter {
  public readonly CURSOR_BATCH_SIZE = 100;
  public readonly WORKSHEET_NAME = "Trajets";
  // TODO improve listing of columns
  public readonly BASE_COLUMN_KEYS: string[] = [
    "rpc_journey_id",
    "operator_journey_id",
    "operator_trip_id",
    "start_datetime",
    "end_datetime",
    "start_location",
    "start_epci",
    "start_insee",
    "end_location",
    "end_epci",
    "end_insee",
    "duration",
    "distance",
    "operator",
    "operator_class",
    "driver_operator_user_id",
    "passenger_operator_user_id",
    "rpc_incentive",
    "incentive_type",
    "passenger_contribution",
  ];

  async call(
    cursor: Cursor<APDFTripInterface>,
    config: ExcelCampaignConfig,
    workbookWriter: excel.stream.xlsx.WorkbookWriter,
  ): Promise<void> {
    // La colonne du déclaré (U) n'est ajoutée que pour les campagnes exposant le
    // montant déclaré par l'opérateur (config extras.declared_incentives) — cf. GEN-643.
    const columnKeys = config.extras?.declared_incentives
      ? [...this.BASE_COLUMN_KEYS, "operator_declared_incentive"]
      : this.BASE_COLUMN_KEYS;

    const worksheet: excel.Worksheet = this.initWorkSheet(
      workbookWriter,
      this.WORKSHEET_NAME,
      columnKeys.map((header) => ({ header, key: header })),
    );

    // style columns and apply optimised width
    const font = { name: "Courier", family: 3, size: 9 };
    const columns = {
      A: { width: 29, font },
      B: { width: 29, font },
      C: { width: 29, font },
      D: { width: 21, font },
      E: { width: 21, font },
      F: { width: 21, font },
      G: { width: 32, font },
      H: { width: 10, font },
      I: { width: 21, font },
      J: { width: 32, font },
      K: { width: 10, font },
      L: { width: 10, font },
      M: { width: 10, font },
      N: { width: 20, font },
      O: { width: 20, font },
      P: { width: 29, font },
      Q: { width: 29, font },
      R: { width: 12, font },
      S: { width: 12, font },
      T: { width: 12, font },
      ...(config.extras?.declared_incentives ? { U: { width: 12, font } } : {}),
    };

    Object.entries(columns).forEach(([key, value]) => {
      Object.entries(value).forEach(([k, v]) => {
        worksheet.getColumn(key)[k] = v;
      });
    });

    // style the header
    worksheet.getRow(1).font = {
      name: "Courier",
      family: 3,
      bold: true,
      size: 9,
    };

    const b1 = new Date();

    for await (const rows of cursor.read(this.CURSOR_BATCH_SIZE)) {
      rows.forEach((t) => t && worksheet.addRow(normalize(t, config)).commit());
    }

    const b2 = new Date();
    logger.debug(`[apdf:export] writing trips took: ${(b2.getTime() - b1.getTime()) / 1000}s`);
    worksheet.commit();
  }
}
