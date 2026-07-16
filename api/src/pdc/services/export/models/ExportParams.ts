import { subMonthsTz, today } from "@/pdc/helpers/dates.helper.ts";
import { Timezone } from "@/pdc/providers/validator/index.ts";
import { ExportTarget } from "@/pdc/services/export/models/Export.ts";
import { TerritorySelectorsInterface } from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";

export type Config = Partial<Params>;

export type Params = {
  start_at: Date;
  end_at: Date;
  operator_id: number[];
  geo_selector: TerritorySelectorsInterface | null;
  tz: Timezone;
  target: ExportTarget;
};

/**
 * Configure for the export
 *
 * @todo add validation
 * @todo add support for territory_id (territory_group._id)
 * @todo add support for SIREN
 */
export class ExportParams {
  protected params: Params;

  protected readonly tz = "Europe/Paris" as Timezone;
  protected readonly schema = {};
  protected readonly defaultConfig: Params = {
    start_at: subMonthsTz(today(this.tz), 1),
    end_at: today(),
    operator_id: [],
    geo_selector: null,
    tz: this.tz,
    target: ExportTarget.DATAGOUV,
  };

  constructor(protected config: Config) {
    this.validate<Config>(config);
    this.params = this.normalize(config);
  }

  /**
   * Normalize params
   *
   * @todo normalize params
   * @todo apply limits to dates, etc...
   *
   * @param {Config} config
   * @returns {Params}
   */
  protected normalize(config: Config): Params {
    const n = { ...this.defaultConfig, ...config };
    n.start_at = new Date(n.start_at);
    n.end_at = new Date(n.end_at);
    return n;
  }

  /**
   * Getter for params
   *
   * @returns {Params}
   */
  public get(): Params {
    if (!this.params) {
      throw new Error(`${__filename} not initialized`);
    }

    return this.params;
  }

  /**
   * Validate params
   *
   * @todo to AbstractParams class
   * @todo validate params against schema or throw InvalidParamsException
   *
   * @param {Config} config
   */
  protected validate<T>(config: T): void {}

  /**
   * Create ExportParams from JSON
   *
   * This is needed when gettin configuration from the database.
   * Specific convert rules can be applied here.
   *
   * @param {Config} json
   * @returns {ExportParams}
   */
  public static fromJSON(json: Config): ExportParams {
    return new ExportParams(json);
  }

  /**
   * Convert ExportParams to JSON
   *
   * This is needed to store the configuration in the database.
   * Specific convert rules can be applied here.
   *
   * @param {ExportParams} params
   * @returns {Config}
   */
  public static toJSON(params: ExportParams): Config {
    return params.get();
  }
}
