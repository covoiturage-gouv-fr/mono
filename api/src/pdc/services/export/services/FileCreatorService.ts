import { provider } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { CSVWriter } from "../models/CSVWriter.ts";
import { ExportParams } from "../models/ExportParams.ts";
import { CampaignRepository } from "../repositories/CampaignRepository.ts";
import { CarpoolRepository } from "../repositories/CarpoolRepository.ts";

export abstract class FileCreatorServiceInterfaceResolver {
  protected async configure<T extends { [k: string]: unknown }>(
    params: ExportParams,
    fileWriter: CSVWriter<T>,
  ): Promise<void> {
    throw new Error("Not implemented");
  }
  protected async initialize(): Promise<void> {
    throw new Error("Not implemented");
  }
  protected async data(): Promise<void> {
    throw new Error("Not implemented");
  }
  protected async help(): Promise<void> {
    throw new Error("Not implemented");
  }
  public async write<T extends { [k: string]: unknown }>(
    params: ExportParams,
    fileWriter: CSVWriter<T>,
  ): Promise<string> {
    throw new Error("Not implemented");
  }
}

@provider({
  identifier: FileCreatorServiceInterfaceResolver,
})
export class FileCreatorService {
  protected fileWriter: any;
  protected params: ExportParams;

  constructor(
    protected carpoolRepository: CarpoolRepository,
    protected campaignRepository: CampaignRepository,
  ) {}

  protected async configure<T extends { [k: string]: unknown }>(
    params: ExportParams,
    fileWriter: CSVWriter<T>,
  ): Promise<void> {
    this.params = params;
    this.fileWriter = fileWriter;
  }

  protected async initialize(): Promise<void> {
    await this.fileWriter.create();
  }

  protected async data(): Promise<void> {
    // pass campaign data to the file writer to enrich fields
    const campaigns = await this.campaignRepository.list();
    this.fileWriter.addDatasource("campaigns", campaigns);

    // loop through the carpool data and append rows to the file
    await this.carpoolRepository.list(
      this.params,
      this.fileWriter,
    );
  }

  protected async help(): Promise<void> {
    await this.fileWriter.printHelp();
  }

  public async write<T extends { [k: string]: unknown }>(
    params: ExportParams,
    fileWriter: CSVWriter<T>,
  ): Promise<string> {
    try {
      await this.configure(params, fileWriter);
      await this.initialize();
      await this.data();
      await this.help();
      await this.fileWriter.close();
      await this.fileWriter.compress();

      logger.info(`File written to ${this.fileWriter.path}`);

      return this.fileWriter.path;
    } catch (e) {
      logger.error("FileCreatorService", (e as Error).message);
      await this.fileWriter.close();
      throw e;
    }
  }
}
