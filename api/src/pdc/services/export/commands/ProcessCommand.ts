import { command, CommandInterface } from "@/ilos/common/index.ts";
import { getPerformanceTimer, logger } from "@/lib/logger/index.ts";
import { staleDelay } from "../config/export.ts";
import { CSVWriter } from "../models/CSVWriter.ts";
import { Export, ExportStatus } from "../models/Export.ts";
import { ExportRepositoryInterfaceResolver } from "../repositories/ExportRepository.ts";
import { CarpoolListType } from "../repositories/queries/carpoolListQuery.ts";
import { FieldServiceInterfaceResolver } from "../services/FieldService.ts";
import { FileCreatorServiceInterfaceResolver } from "../services/FileCreatorService.ts";
import { LogServiceInterfaceResolver } from "../services/LogService.ts";
import { NameServiceInterfaceResolver } from "../services/NameService.ts";
import { NotificationService } from "../services/NotificationService.ts";
import { StorageService } from "../services/StorageService.ts";

@command({
  signature: "export:process",
  description: "Process all pending exports",
})
export class ProcessCommand implements CommandInterface {
  constructor(
    protected exportRepository: ExportRepositoryInterfaceResolver,
    protected fileCreatorService: FileCreatorServiceInterfaceResolver,
    protected fieldService: FieldServiceInterfaceResolver,
    protected nameService: NameServiceInterfaceResolver,
    protected logger: LogServiceInterfaceResolver,
    protected storage: StorageService,
    protected notify: NotificationService,
  ) {}

  public async call(): Promise<void> {
    // init the storage service
    await this.storage.init();

    // fail stale exports running for too long
    await this.exportRepository.failStaleExports();
    logger.info(`Patched stale exports running for more than ${staleDelay}`);

    // process pending exports until there are no more
    // picking one at a time to avoid concurrency issues
    // and let multiple workers process the queue in parallel
    let counter = 50;
    let exp = await this.exportRepository.pickPending();
    while (exp && counter > 0) {
      await this.process(exp);
      exp = await this.exportRepository.pickPending();
      counter--;
    }

    logger.info("No more pending exports. Bye!");
  }

  protected async process(exp: Export): Promise<void> {
    const { _id, uuid, target, params } = exp;
    const fields = this.fieldService.byTarget<CarpoolListType>(target);
    const filename = this.nameService.get({ target, uuid }); // TODO add support for territory name

    try {
      const timer = getPerformanceTimer();
      await this.exportRepository.status(_id, ExportStatus.RUNNING);

      // generate the file
      const filepath = await this.fileCreatorService.write<CarpoolListType>(
        params,
        new CSVWriter<CarpoolListType>(filename, { fields }),
      );

      // upload to storage
      await this.exportRepository.status(_id, ExportStatus.UPLOADING);

      const fileInfo = await Deno.stat(filepath);
      const finalFilename = filepath.split("/").pop() || filename.split(".").pop();

      const key = await this.storage.upload(filepath);
      const url = await this.storage.getPublicUrl(key);
      // await this.storage.cleanup(filepath);

      await this.exportRepository.update(_id, {
        download_url: url,
        filename: finalFilename,
        file_size: fileInfo.size,
      });
      await this.exportRepository.status(_id, ExportStatus.UPLOADED);

      // notify the user
      await this.exportRepository.status(_id, ExportStatus.NOTIFY);
      await this.notify.success(exp, url);

      // :tada:
      await this.exportRepository.status(_id, ExportStatus.SUCCESS);
      logger.info(`Export ${uuid} done in ${timer.stop()} ms`);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      logger.error(`Export ${uuid} failed: ${message}`);
      e instanceof Error && logger.error(e.stack);
      await this.exportRepository.error(_id, message);
      await this.notify.error({ ...exp, error: message });
      await this.notify.support({ ...exp, error: message });
    }
  }
}
