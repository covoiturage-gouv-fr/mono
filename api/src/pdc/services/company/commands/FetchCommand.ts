import { command } from "@/ilos/common/Decorators.ts";
import { CommandInterface, ValidatorInterfaceResolver } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { CompanyDataSourceProviderInterfaceResolver } from "@/pdc/services/company/interfaces/CompanyDataSourceProviderInterface.ts";
import { CompanyRepositoryProviderInterfaceResolver } from "@/pdc/services/company/interfaces/CompanyRepositoryProviderInterface.ts";
import { aliasCommand } from "../contracts/fetch.schema.ts";

export type Options = {
  save: boolean;
};

@command({
  signature: "company:fetch <siret>",
  description: "Fetch company data",
  options: [
    {
      signature: "-s, --save",
      description: "Save the fetched data to the database",
      default: false,
    },
  ],
})
export class FetchCommand implements CommandInterface {
  constructor(
    private datasource: CompanyDataSourceProviderInterfaceResolver,
    private repository: CompanyRepositoryProviderInterfaceResolver,
    private validator: ValidatorInterfaceResolver,
  ) {}

  public async call(siret: string, options: Options): Promise<void> {
    // Validate SIRET format
    try {
      logger.info("Validating SIRET format...");
      this.validator.registerValidator({
        $schema: "http://json-schema.org/draft-07/schema#",
        $id: "siret",
        type: "object",
        properties: { siret: { macro: "siret" } },
        required: ["siret"],
      });

      await this.validator.validate(siret, aliasCommand);
    } catch (error) {
      logger.error(error.message);
      logger.error(this.validator.errors);
    }

    // Fetch company data from the INSEE SIRENE API
    logger.log(`Fetching company data for SIRET ${siret}...`);

    try {
      const data = await this.datasource.find(siret);

      if (options.save) {
        logger.info("Saving fetched data to the database...");
        await this.repository.updateOrCreate(data);
      }

      logger.info(data);
    } catch (error) {
      logger.error(error.message);
    }
  }
}
