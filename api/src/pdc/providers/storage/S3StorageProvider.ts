import { constants as fsConstants } from "dep:fs";
import { access } from "dep:fs-promises";
import { S3Client } from "dep:s3-lite-client";
import { URL } from "dep:url";
import { ConfigInterfaceResolver, provider, ProviderInterface } from "../../../ilos/common/index.ts";
import { env_or_fail } from "../../../lib/env/index.ts";
import { logger } from "../../../lib/logger/index.ts";
import { filenameFromPath, getBucketName } from "./helpers/buckets.ts";
import { S3ObjectList } from "./index.ts";
import { BucketName } from "./interfaces/BucketName.ts";

@provider()
export class S3StorageProvider implements ProviderInterface {
  private s3Instances: Map<BucketName, S3Client> = new Map();
  private endpoint: string | undefined;
  private region: string | undefined;

  public static readonly SEVEN_DAY: number = 7 * 86400;
  public static readonly TEN_MINUTES: number = 7 * 600;

  constructor(protected config: ConfigInterfaceResolver) {}

  async init(): Promise<void> {
    this.endpoint = env_or_fail("AWS_ENDPOINT");
    this.region = env_or_fail("AWS_REGION");

    // Create s3 instances for all buckets in the BucketName list
    this.s3Instances.set(BucketName.APDF, this.createInstance(BucketName.APDF));
    this.s3Instances.set(BucketName.Export, this.createInstance(BucketName.Export));
    this.s3Instances.set(BucketName.Public, this.createInstance(BucketName.Public));
    this.s3Instances.set(BucketName.GeoDatasetsMirror, this.createInstance(BucketName.GeoDatasetsMirror));
  }

  protected createInstance(bucket: BucketName): S3Client {
    if (!this.endpoint || !this.region) {
      throw new Error("Missing endpoint or region in bucket configuration");
    }
    const endPointUrl = new URL(this.endpoint);
    const params = {
      pathStyle: true,
      endPoint: endPointUrl.hostname,
      port: parseInt(endPointUrl.port || "443", 10),
      useSSL: endPointUrl.protocol === "https:",
      region: this.region,
      bucket,
      accessKey: env_or_fail("AWS_ACCESS_KEY_ID"),
      secretKey: env_or_fail("AWS_SECRET_ACCESS_KEY"),
      ...this.config.get("storage.bucket.options", {}),
    };
    return new S3Client(params);
  }

  async list(bucket: BucketName, folder?: string): Promise<S3ObjectList> {
    const client = this.s3Instances.get(bucket);
    if (!client) {
      throw new Error(`Failed to create a client for the bucket: ${bucket}`);
    }

    const params = {
      prefix: folder,
      bucketName: getBucketName(bucket),
    };

    logger.info(
      `[S3StorageProvider:list] bucket ${params.bucketName}/${params.prefix}`,
    );

    const generator = client.listObjects(params);
    const result = [];
    for await (const r of generator) {
      result.push(r);
    }
    return result;
  }

  async upload(
    bucket: BucketName,
    filepath: string,
    filename?: string,
    folder?: string,
    options: Record<string, any> = {},
  ): Promise<string> {
    // Check if file exists
    await access(filepath, fsConstants.R_OK);
    const client = this.s3Instances.get(bucket);
    if (!client) {
      throw new Error("Failed to create a client for the bucket");
    }
    const rs = await Deno.open(filepath, { read: true });
    let key = filename ?? filenameFromPath(filepath);

    if (folder) {
      key = `${folder}/${key}`;
    }

    const bucketName = getBucketName(bucket);
    const params = { ...options, bucketName };

    try {
      await client.putObject(key, rs.readable, params);
      return key;
    } catch (e) {
      // rs is closed by the putObject as the stream is consumed
      // do not call rs.close() or you get a Bad Resource ID error.
      if (e instanceof Error) {
        logger.error(`S3StorageProvider Error: ${e.message} (${bucketName}/${key})`);
      } else {
        logger.error(`S3StorageProvider Error: Unknown error (${filepath})`);
      }
      throw e;
    }
  }

  async getPublicUrl(
    bucket: BucketName,
    filekey: string,
  ): Promise<string> {
    if (bucket !== BucketName.Public) {
      return this.getSignedUrl(bucket, filekey);
    }

    return `${this.endpoint}/${getBucketName(bucket)}/${filekey}`;
  }

  async getSignedUrl(
    bucket: BucketName,
    filekey: string,
    expiresIn: number = S3StorageProvider.SEVEN_DAY,
  ): Promise<string> {
    const client = this.s3Instances.get(bucket);
    if (!client) {
      throw new Error();
    }
    try {
      const url = await client.getPresignedUrl("GET", filekey, {
        bucketName: getBucketName(bucket),
        expirySeconds: expiresIn,
      });
      return url;
    } catch (e) {
      if (e instanceof Error) {
        logger.error(`S3StorageProvider Error: ${e.message} (${filekey})`);
      } else {
        logger.error(`S3StorageProvider Error: Unknown error (${filekey})`);
      }

      throw e;
    }
  }
}
