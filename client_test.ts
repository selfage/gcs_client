import { CloudStorageClient, ResumableUpload } from "./client";
import { Storage } from "@google-cloud/storage";
import { getEnvVar } from "@selfage/env_var_getter";
import { assertThat, containStr, eq, gt } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { createReadStream } from "fs";
import { stat } from "fs/promises";

let gcsBucket = getEnvVar("gcs_bucket").required().asString();
let videoFile = "test_data/video.mp4";

TEST_RUNNER.run({
  name: "CloudStorageClientTest",
  cases: [
    {
      name: "SingleUpload",
      execute: async () => {
        // Prepare
        let client = CloudStorageClient.create();
        let fileStat = await stat(videoFile);
        let body = createReadStream(videoFile);

        // Execute
        let response = await client.upload(
          gcsBucket,
          "singleUploadVideo",
          body,
          "video/mp4",
          fileStat.size,
        );

        // Verify
        assertThat(response.md5Hash, eq("6CYUDMQPrkUooNzUNzVEug=="), "md5Hash");
      },
      tearDown: async () => {
        try {
          await new Storage()
            .bucket(gcsBucket)
            .file("singleUploadVideo")
            .delete();
        } catch (e) {}
      },
    },
    {
      name: "ResumableUpload",
      execute: async () => {
        // Prepare
        let client = CloudStorageClient.create();
        let chunkSize = 1 * 1024 * 1024;
        let contentLength = (await stat(videoFile)).size;
        let body = createReadStream(videoFile);
        let resumableUpload: ResumableUpload = {};
        let infoCaptured: string;

        // Execute
        let responsePromise = client.resumeUpload(
          gcsBucket,
          "chunkedVideo",
          body,
          "video/mp4",
          contentLength,
          resumableUpload,
          {
            chunkSize,
            logFn: (info) => (infoCaptured = info),
          },
        );
        setTimeout(() => body.destroy(new Error("Interrupted!")), 3000);
        let response = await responsePromise;

        // Verify
        assertThat(Boolean(resumableUpload.url), eq(true), "has url");
        assertThat(resumableUpload.byteOffset, gt(0), "byteOffset");
        assertThat(infoCaptured, containStr("Interrupted!"), "info");
        assertThat(response, eq(undefined), "no response");

        // Execute
        body = createReadStream(videoFile, {
          start: resumableUpload.byteOffset,
        });
        response = await client.resumeUpload(
          gcsBucket,
          "chunkedVideo",
          body,
          "video/mp4",
          contentLength,
          resumableUpload,
          {
            chunkSize,
          },
        );

        // Verify
        assertThat(response.md5Hash, eq("6CYUDMQPrkUooNzUNzVEug=="), "md5Hash");
      },
      tearDown: async () => {
        try {
          await new Storage().bucket(gcsBucket).file("chunkedVideo").delete();
        } catch (e) {}
      },
    },
  ],
});
