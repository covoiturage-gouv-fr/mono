import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";
import { setEnv } from "@/lib/env/index.ts";
import { BucketName } from "../interfaces/BucketName.ts";
import {
  getBucketEndpoint,
  getBucketName,
  getBucketPrefix,
} from "./buckets.ts";

it("[getBucketPrefix] returns AWS_BUCKET_PREFIX or empty", () => {
  setEnv("AWS_BUCKET_PREFIX", "");
  assertEquals(getBucketPrefix(), "");

  setEnv("AWS_BUCKET_PREFIX", "test");
  assertEquals(getBucketPrefix(), "test");
});

it("[getBucketEndpoint] no var, no prefix", () => {
  setEnv("AWS_BUCKET_PREFIX");
  setEnv("AWS_BUCKET_EXPORT_ENDPOINT");
  assertEquals(
    getBucketEndpoint("https://s3.test", BucketName.Export),
    "https://s3.test",
  );
});

it("[getBucketEndpoint] no var, with prefix", () => {
  setEnv("AWS_BUCKET_PREFIX", "url-prefix");
  setEnv("AWS_BUCKET_EXPORT_ENDPOINT");
  assertEquals(
    getBucketEndpoint("https://s3.test", BucketName.Export),
    "https://s3.test",
  );
});

it("[getBucketEndpoint] var override, no prefix", () => {
  setEnv("AWS_BUCKET_PREFIX");
  setEnv("AWS_BUCKET_EXPORT_ENDPOINT", "https://override-export.s3.test");
  assertEquals(
    getBucketEndpoint("https://s3.test", BucketName.Export),
    "https://override-export.s3.test",
  );
});

it("[getBucketName] no prefix", () => {
  setEnv("AWS_BUCKET_PREFIX");
  assertEquals(getBucketName(BucketName.Export), "export");
});

it("[getBucketName] prefix", () => {
  setEnv("AWS_BUCKET_PREFIX", "name-prefix");
  assertEquals(getBucketName(BucketName.Export), "name-prefix-export");
});
