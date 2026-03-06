import { assert, assertEquals } from "dep:assert";
import { assertSpyCall, assertSpyCalls, stub } from "dep:testing-mock";
import { afterAll, beforeEach, describe, it } from "dep:testing-bdd";
import { createHash } from "@/lib/crypto/index.ts";
import { exists } from "@/lib/file/index.ts";
import { join } from "@/lib/path/index.ts";
import { S3StorageProvider } from "@/pdc/providers/storage/index.ts";
import { mkdir, readFile, rm } from "dep:fs-promises";
import { writeFile } from "../helpers/index.ts";
import { FileManager } from "./FileManager.ts";

describe("File Manager", () => {
  const GEO_PERIMETER_TMP_DIR = "/tmp/perimeter-geo-test";
  const RESSOURCE_URL = "http://www.get.domaine.fr/system/files/documents/2022/09/file";
  const MIRROR_URL = "https://s3.domain.fr/bucket/0524479cc58cffd3d5db6b96ea893853744fe33f15ce76123f8a772a546c4252";
  const FETCH_ARGS: RequestInit = { method: "GET", redirect: "follow" };
  const FILE_CONTENT_STRING = "{}";
  const FILE_SHA256 = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";

  let READABLE_STREAM: ReadableStream;
  let fileManager: FileManager;

  afterAll(async () => {
    await rm(GEO_PERIMETER_TMP_DIR, { recursive: true, force: true });
  });

  beforeEach(async () => {
    await rm(GEO_PERIMETER_TMP_DIR, { recursive: true, force: true });

    fileManager = new FileManager({
      basePath: GEO_PERIMETER_TMP_DIR,
      downloadPath: `${GEO_PERIMETER_TMP_DIR}/download`,
      mirrorUrl: "https://s3.domain.fr/bucket",
    });

    READABLE_STREAM = new ReadableStream({
      start(controller) {
        // Convert the text to a Uint8Array (or other suitable format) and enqueue it
        controller.enqueue(new TextEncoder().encode(FILE_CONTENT_STRING));
        // Close the stream after the text has been enqueued
        controller.close();
      },
    });
  });

  it("should return ressource file if available", async () => {
    const fetchFunctionStub = stub(globalThis, "fetch");
    try {
      // Arrange
      const existingFilepath = join(fileManager.downloadPath, await createHash(RESSOURCE_URL));
      await mkdir(fileManager.downloadPath, { recursive: true });
      await writeFile(existingFilepath, READABLE_STREAM);

      // Act
      const filepath = await fileManager.download({ url: RESSOURCE_URL });
      assertEquals(await readFile(filepath, "utf8"), FILE_CONTENT_STRING);
    } finally {
      fetchFunctionStub.restore();
    }
    // Assert
    assertSpyCalls(fetchFunctionStub, 0);
  });

  it("should discard the file if the hash is invalid", async () => {
    const fetchFunctionStub = stub(globalThis, "fetch", async () => new Response(READABLE_STREAM));

    try {
      // Arrange
      const existingFilepath = join(fileManager.downloadPath, await createHash(RESSOURCE_URL));
      await mkdir(fileManager.downloadPath, { recursive: true });
      await writeFile(
        existingFilepath,
        new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode("invalid content"));
            controller.close();
          },
        }),
      );

      // make sure the file exists with the invalid content
      assert(true === await exists(existingFilepath));
      assert(await readFile(existingFilepath, "utf8"), "invalid content");

      // Act
      await fileManager.download({ url: RESSOURCE_URL, sha256: FILE_SHA256 });

      // make sure the file has been removed and re-downloaded
      // with the right content
      assert(true === await exists(existingFilepath));
      assert(await readFile(existingFilepath, "utf8"), FILE_CONTENT_STRING);
    } finally {
      fetchFunctionStub.restore();
    }
  });

  it("should download from mirror if missing locally", async () => {
    // Arrange
    const fetchFunctionStub = stub(globalThis, "fetch", async () => new Response(READABLE_STREAM));

    try {
      // Act
      const filepath = await fileManager.download({ url: RESSOURCE_URL });
      assertEquals(await readFile(filepath, { encoding: "utf8" }), FILE_CONTENT_STRING);
    } finally {
      fetchFunctionStub.restore();
    }
    // Assert
    assertSpyCall(fetchFunctionStub, 0, { args: [MIRROR_URL, FETCH_ARGS] });
  });

  it("should fallback to source on cache miss", async () => {
    // Arrange
    const s3ProviderStub = stub(S3StorageProvider.prototype, "upload", async () => {
      return MIRROR_URL;
    });

    let fetchFunctionCalls = 0;
    const fetchFunctionStub = stub(globalThis, "fetch", async () => {
      if (fetchFunctionCalls === 0) {
        fetchFunctionCalls = 1;
        // first call to the mirror throws an error
        throw new Error("Invalid URL");
      }

      // subsequent calls to the source return the file
      // call 2: download from source
      // call 3: upload to cache
      return new Response(READABLE_STREAM);
    });

    try {
      // Act
      const filepath = await fileManager.download({ url: RESSOURCE_URL });
      assertEquals(await readFile(filepath, "utf8"), FILE_CONTENT_STRING);
    } finally {
      fetchFunctionStub.restore();
      s3ProviderStub.restore();
    }

    // Assert
    assertSpyCalls(fetchFunctionStub, 2);
    assertSpyCall(fetchFunctionStub, 0, { args: [MIRROR_URL, FETCH_ARGS] });
    assertSpyCall(fetchFunctionStub, 1, { args: [RESSOURCE_URL, FETCH_ARGS] });
  });
});
