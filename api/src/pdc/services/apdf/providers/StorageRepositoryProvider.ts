import { subMonths } from "dep:date-fns";
import { provider } from "../../../../ilos/common/index.ts";
import { logger } from "../../../../lib/logger/index.ts";
import {
  APDFNameProvider,
  BucketName,
  S3Object,
  S3ObjectList,
  S3StorageProvider,
} from "../../../providers/storage/index.ts";
import { EnrichedApdfType } from "../contracts/list.contract.ts";
import {
  SerializedPolicyInterface,
  StorageRepositoryProviderInterfaceResolver,
} from "../interfaces/StorageRepositoryProviderInterface.ts";

@provider({
  identifier: StorageRepositoryProviderInterfaceResolver,
})
export class StorageRepositoryProvider implements StorageRepositoryProviderInterfaceResolver {
  private bucket: BucketName = BucketName.APDF;

  constructor(
    private s3StorageProvider: S3StorageProvider,
    private APDFNameProvider: APDFNameProvider,
  ) {}

  async findByCampaign(campaign: SerializedPolicyInterface): Promise<S3ObjectList> {
    try {
      const list = await this.s3StorageProvider.list(this.bucket, `${campaign._id}`);
      return list.filter((obj: S3Object) => obj.size > 0);
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : String(e);
      logger.error(`[Apdf:StorageRepo:findByCampaign] ${errMsg}`);
      throw e;
    }
  }

  async enrich(list: S3ObjectList): Promise<EnrichedApdfType[]> {
    return Promise.all(
      list.map(async (o: S3Object) => ({
        ...this.APDFNameProvider.parse(o.key),
        signed_url: await this.s3StorageProvider.getSignedUrl(this.bucket, o.key, S3StorageProvider.TEN_MINUTES),
        key: o.key,
        size: o.size,
      })),
    );
  }

  // use in a [].filter
  campaignsFilter(campaigns: number[]) {
    return (obj: EnrichedApdfType): boolean => {
      return campaigns.length ? campaigns.indexOf(obj.campaign_id) > -1 : true;
    };
  }

  // use in a [].filter
  operatorsFilter(operators: number[]) {
    return (obj: EnrichedApdfType): boolean => {
      return operators.length ? operators.indexOf(obj.operator_id) > -1 : true;
    };
  }

  // use in a [].filter
  showCurrentMonthFilter(permissions: string[], show: boolean) {
    return (obj: EnrichedApdfType): boolean => {
      // from config
      if (show) return true;

      // from permissions
      if (permissions.indexOf("registry.apdf.listCurrentMonth") > -1) {
        return true;
      }

      // show all but last month
      const fileMonth = obj.datetime.toISOString().substring(0, 7);
      const lastMonth = subMonths(new Date(), 1).toISOString().substring(0, 7);

      return fileMonth !== lastMonth;
    };
  }
}
