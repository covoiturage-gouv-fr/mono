import { createHash } from "@/lib/crypto/index.ts";
import { getTmpDir, open, OpenFileDescriptor, remove } from "@/lib/file/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { join } from "@/lib/path/index.ts";
import { toTzString } from "@/pdc/helpers/dates.helper.ts";
import { sanitize } from "@/pdc/helpers/string.helper.ts";
import { castToStatusEnum } from "@/pdc/providers/carpool/helpers/castStatus.ts";
import { Timezone } from "@/pdc/providers/validator/types.ts";
import { transformations } from "@/pdc/services/export/config/export.ts";
import AdmZip from "dep:adm-zip";
import { stringify } from "dep:csv-stringify";
import { AllowedComputedFields, CarpoolRow } from "./CarpoolRow.ts";
import { ExportTarget } from "./Export.ts";

export type Datasources = Map<string, unknown>;

export type Fields<T extends { [k: string]: unknown }> = Array<keyof T | keyof AllowedComputedFields>;

export type FieldFilter<T extends { [k: string]: unknown }> = { target: ExportTarget; exclusions: Partial<Fields<T>> };

export type ComputedProcessors<T extends { [k: string]: unknown }> = Array<ComputedProcessor<T>>;

// TODO find a way to type the compute() return type depending on the name
export type ComputedProcessor<T extends { [k: string]: unknown }> = {
  name: keyof AllowedComputedFields;
  compute: (
    row: CarpoolRow<T>,
    options: Options<T>,
  ) => Promise<AllowedComputedFields[keyof AllowedComputedFields] | null>;
};

export type Options<T extends { [k: string]: unknown }> = {
  tz: Timezone;
  compress: boolean;
  cleanup: boolean;
  fields: Partial<Fields<T>>;
  computed: ComputedProcessors<T>;
  datasources: Datasources;
};

export class CSVWriter<T extends { [k: string]: unknown }> {
  protected fileStream: OpenFileDescriptor | null = null;
  protected fileExt = "csv";
  protected archiveExt = "zip";
  protected folder: string;
  protected basename: string;
  protected options: Options<T> = {
    tz: "Europe/Paris",
    compress: true,
    cleanup: true,
    fields: [],
    computed: [
      {
        name: "journey_start_datetime",
        async compute(row, { tz }) {
          return toTzString(row.value("journey_start_datetime") as Date, tz);
        },
      },
      {
        name: "journey_end_datetime",
        async compute(row, { tz }) {
          return toTzString(row.value("journey_start_datetime") as Date, tz);
        },
      },
      {
        name: "trip_id",
        async compute(row) {
          return await createHash((row.value("trip_id") || "") as string);
        },
      },
      {
        name: "status",
        async compute(row) {
          return castToStatusEnum(
            row.get([
              "acquisition_status",
              "anomaly_status",
              "fraud_status",
            ]),
          );
        },
      },
      {
        name: "incentive_type",
        async compute(row, { datasources }) {
          if (!row) return "normal";

          // for each campaign, get the mode at the start date or the end date
          // and return the higher one (booster)
          // @ts-ignore make the [] work
          return (row.value("campaigns", []) as any[])
            .reduce((acc: string, id: number) => {
              const campaigns = datasources.get("campaigns");
              if (campaigns instanceof Map) {
                const campaign = campaigns.get(id);
                if (!campaign) return acc;
                const mode = campaign.getModeAt([
                  row.value("start_datetime"),
                  row.value("end_datetime"),
                ]);
                return acc === "booster" ? acc : mode;
              }
              return acc;
            }, "normal");
        },
      },
      {
        name: "has_incentive",
        async compute(row) {
          const yes = transformations.has_incentive_yes;
          const no = transformations.has_incentive_no;
          return row.hasIncentive() ? yes : no;
        },
      },
    ],
    datasources: new Map(),
  };
  protected stringifierOptions = {
    header: true,
    delimiter: ";", // Excel uses semicolon as default delimiter
    quoted_string: true,
  };
  protected stringifier = stringify(this.stringifierOptions);

  constructor(filename: string, config: Partial<Options<T>>) {
    this.options = { ...this.options, ...config } as Options<T>;
    this.folder = getTmpDir();
    this.basename = sanitize(filename, 128);
  }

  public async create(): Promise<CSVWriter<T>> {
    this.fileStream = await open(this.csvPath, { write: true });
    this.stringifier.on("readable", () => {
      let row = this.stringifier.read();
      while (row !== null) {
        this.fileStream?.write(row);
        row = this.stringifier.read();
      }
    });
    this.stringifier.on("error", (err) => {
      logger.error(err.message);
    });
    this.stringifier.on("finish", () => {
      this.fileStream?.close();
    });

    return this;
  }

  public async close(): Promise<CSVWriter<T>> {
    this.stringifier.end();
    return this;
  }

  // append lines to the data sheet
  public async append(carpoolRow: CarpoolRow<T>): Promise<CSVWriter<T>> {
    // add computed fields to the carpool row
    for (const field of this.options.computed) {
      carpoolRow.addField(field.name, await field.compute(carpoolRow, this.options));
    }

    this.stringifier.write(carpoolRow.get(this.options.fields as string[]));

    return this;
  }

  public async printHelp(): Promise<CSVWriter<T>> {
    // No help in CSV
    return this;
  }

  // TODO compress the file with ZIP (for now)
  public async compress(): Promise<CSVWriter<T>> {
    if (!this.options.compress) {
      logger.info(`Skipped compression of ${this.csvPath}`);
      return this;
    }

    const zip = new AdmZip();
    zip.addLocalFile(this.csvPath);
    zip.writeZip(this.archivePath);

    if (this.options.cleanup) {
      await remove(this.csvPath);
    }

    return this;
  }

  public get filename(): string {
    return this.options.compress ? this.archiveFilename : this.csvFilename;
  }

  public get path(): string {
    return this.options.compress ? this.archivePath : this.csvPath;
  }

  public get csvFilename(): string {
    return `${this.basename}.${this.fileExt}`;
  }

  public get csvPath(): string {
    return join(this.folder, this.csvFilename);
  }

  public get archiveFilename(): string {
    return `${this.basename}.${this.archiveExt}`;
  }

  public get archivePath(): string {
    return join(this.folder, this.archiveFilename);
  }

  public addDatasource(key: string, value: any): CSVWriter<T> {
    this.options.datasources.set(key, value);
    return this;
  }
}
