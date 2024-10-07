import { ChunkedUploadStream } from "./chunked_upload_stream";
import { GoogleAuth } from "google-auth-library";
import { Readable } from "stream";
import { pipeline } from "stream/promises";

export interface ResumableUploadOptions {
  chunkSize?: number;
  logFn?: (info: string) => void;
}

export interface ResumableUpload {
  url?: string;
  byteOffset?: number;
}

export interface UploadedResult {
  md5Hash: string;
  crc32c: string;
  updatedTimestamp: number; // ms
  createdTimestamp: number; // ms
}

export class CloudStorageClient {
  public static create(projectId?: string): CloudStorageClient {
    return new CloudStorageClient(
      new GoogleAuth({
        projectId,
      }),
    );
  }

  private static DEFAULT_CHUNK_SIZE = 32 * 1024 * 1024; // Must be a multiple of 256 x 1024.
  private static EXTRACT_BYTE_OFFSET_REGEX = /^bytes=[0-9]+?-([0-9]+?)$/;
  private static STORAGE_API_DOMAIN = `https://storage.googleapis.com`;

  public constructor(
    private googleAuth: GoogleAuth,
    private storageApiDomain = CloudStorageClient.STORAGE_API_DOMAIN,
  ) {}

  public async resumeUpload(
    bucketName: string,
    filename: string,
    body: Readable,
    contentType: string,
    contentLength: number,
    resumableUpload: ResumableUpload,
    options: ResumableUploadOptions = {},
  ): Promise<UploadedResult | undefined> {
    options.chunkSize ??= CloudStorageClient.DEFAULT_CHUNK_SIZE;
    options.logFn ??= () => {};

    if (!resumableUpload.url) {
      let response = await this.googleAuth.request({
        method: "POST",
        url: `${this.storageApiDomain}/upload/storage/v1/b/${bucketName}/o?uploadType=resumable&name=${filename}`,
        headers: {
          "Content-Length": 0,
          "X-Upload-Content-Type": contentType,
          "X-Upload-Content-Length": contentLength,
        },
      });
      resumableUpload.url = response.headers.location;
      resumableUpload.byteOffset = 0;
    }

    let chunkedUploadStream = new ChunkedUploadStream(
      this.googleAuth,
      options.chunkSize,
      contentLength,
      resumableUpload.url,
      resumableUpload.byteOffset,
    );
    try {
      await pipeline(body, chunkedUploadStream);
    } catch (e) {
      options.logFn(
        `Upload interrupted for file ${filename}. Reason: ${e.message}`,
      );
      if (chunkedUploadStream.range) {
        let matched = CloudStorageClient.EXTRACT_BYTE_OFFSET_REGEX.exec(
          chunkedUploadStream.range,
        );
        resumableUpload.byteOffset = parseInt(matched[1]) + 1;
      }
      return undefined;
    }
    return {
      md5Hash: chunkedUploadStream.response.data.md5Hash,
      crc32c: chunkedUploadStream.response.data.crc32c,
      createdTimestamp: new Date(
        chunkedUploadStream.response.data.timeCreated,
      ).valueOf(),
      updatedTimestamp: new Date(
        chunkedUploadStream.response.data.updated,
      ).valueOf(),
    };
  }

  public async upload(
    bucketName: string,
    filename: string,
    body: Readable,
    contentType: string,
    contentLength: number,
  ): Promise<UploadedResult> {
    let response = await this.googleAuth.request({
      method: "POST",
      url: `${this.storageApiDomain}/upload/storage/v1/b/${bucketName}/o?uploadType=media&name=${filename}`,
      headers: {
        "Content-Type": contentType,
        "Content-Length": contentLength,
      },
      body,
    });
    return {
      md5Hash: response.data.md5Hash,
      crc32c: response.data.crc32c,
      createdTimestamp: new Date(response.data.timeCreated).valueOf(),
      updatedTimestamp: new Date(response.data.updated).valueOf(),
    };
  }
}
