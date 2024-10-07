import { GaxiosResponse } from "gaxios";
import { GoogleAuth } from "google-auth-library";
import { PassThrough, Writable } from "stream";

export class ChunkedUploadStream extends Writable {
  private static INCOMPLETE_ERROR_CODE = 308;

  private chunkCount: number;
  private limit: number;
  private passThough: PassThrough;
  private uploadResponse: Promise<GaxiosResponse<any>>;
  private abortController: AbortController;
  public range: string;
  public response: GaxiosResponse<any>;

  public constructor(
    private googleAuth: GoogleAuth,
    private chunkSize: number,
    private contentLength: number,
    private resumeUrl: string,
    private byteOffset: number,
  ) {
    super();
    if (byteOffset % chunkSize !== 0) {
      throw new Error(
        `byteOffset ${byteOffset} must be a multiple of chunk size ${chunkSize}.`,
      );
    }
    this.chunkCount = byteOffset / chunkSize;
    this.startNewUpload();
  }

  private startNewUpload() {
    this.chunkCount += 1;
    this.limit = Math.min(this.chunkSize * this.chunkCount, this.contentLength);
    this.passThough = new PassThrough();
    this.abortController = new AbortController();
    this.uploadResponse = this.googleAuth.request({
      url: this.resumeUrl,
      method: "PUT",
      headers: {
        "Content-Length": this.limit - this.byteOffset,
        "Content-Range": `bytes ${this.byteOffset}-${this.limit - 1}/${this.contentLength}`,
      },
      signal: this.abortController.signal,
      body: this.passThough,
    });
  }

  async _write(
    chunk: Buffer,
    encoding: BufferEncoding,
    callback: (error?: Error) => void,
  ): Promise<void> {
    let remaining = 0;
    if (this.byteOffset + chunk.byteLength >= this.limit) {
      remaining = this.limit - this.byteOffset;
      this.byteOffset += remaining;
      this.passThough.end(chunk.subarray(0, remaining));
      try {
        this.response = await this.uploadResponse;
        callback();
        return;
      } catch (e) {
        if (e.status !== ChunkedUploadStream.INCOMPLETE_ERROR_CODE) {
          callback(e);
          return;
        }
        this.range = e.response.headers.range;
        this.startNewUpload();
      }
    }
    if (chunk.byteLength === remaining) {
      callback();
      return;
    }
    // chunk.byteLength > remaining
    this.byteOffset += chunk.byteLength - remaining;
    if (this.passThough.write(chunk.subarray(remaining))) {
      callback();
    } else {
      this.passThough.once("drain", () => {
        callback();
      });
    }
  }

  _destroy(
    error: Error | null,
    callback: (error?: Error | null) => void,
  ): void {
    this.abortController.abort();
    callback(error);
  }
}
