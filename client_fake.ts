import path from "path";
import {
  CloudStorageClient,
  ResumableUpload,
  ResumableUploadOptions,
  UploadedResult,
} from "./client";
import { createWriteStream } from "fs";
import { Readable } from "stream";
import { pipeline } from "stream/promises";

export class CloudStorageClientFake extends CloudStorageClient {
  public destroyBodyError: Error;
  public resumeUrl = "resume_url";
  public resumeByteOffset = 156121;

  public constructor(private localDir: string) {
    super(undefined, undefined);
  }

  public async upload(
    bucketName: string,
    filename: string,
    body: Readable,
    contentType: string,
  ): Promise<UploadedResult> {
    await pipeline(
      body,
      createWriteStream(path.join(this.localDir, bucketName, filename)),
    );
    return {
      md5Hash: "md5Hash",
      crc32c: "crc32c",
      createdTimestamp: 0,
      updatedTimestamp: 0,
    };
  }

  public async resumeUpload(
    bucketName: string,
    filename: string,
    body: Readable,
    contentType: string,
    contentLength: number,
    resumableUpload: ResumableUpload,
    options?: ResumableUploadOptions,
  ): Promise<UploadedResult | undefined> {
    let promise = pipeline(
      body,
      createWriteStream(path.join(this.localDir, bucketName, filename)),
    );
    if (this.destroyBodyError) {
      body.destroy(this.destroyBodyError);
    }
    try {
      await promise;
    } catch (e) {
      resumableUpload.url = this.resumeUrl;
      resumableUpload.byteOffset = this.resumeByteOffset;
      return undefined;
    }
    return {
      md5Hash: "md5Hash",
      crc32c: "crc32c",
      createdTimestamp: 0,
      updatedTimestamp: 0,
    };
  }
}
