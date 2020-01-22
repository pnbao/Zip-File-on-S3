const Archiver = require("archiver");
const AWS = require("aws-sdk");
const path = require("path");
const Stream = require("stream");
const uuid = require("uuid/v4");

const S3 = new AWS.S3();

class ZipHandler {
  constructor(keys, bucketName, zipFileName) {
    this.keys = keys;
    this.bucketName = bucketName;
    this.zipFileName = zipFileName;
  }
  readStream(Bucket, Key) {
    return S3.getObject({ Bucket, Key }).createReadStream();
  }

  writeStream(Bucket, Key) {
    console.log("writeStream");
    const streamPassThrough = new Stream.PassThrough();
    const params = {
      Body: streamPassThrough,
      Bucket,
      ContentType: "application/zip",
      Key,
      // ACL: "public-read"
    };
    return {
      s3StreamUpload: streamPassThrough,
      uploaded: S3.upload(params, error => {
        if (error) {
          console.error(
            `Get error creating stream to s3 ${error.name} ${error.message} ${error.stack}`
          );
          throw error;
        }
      })
    };
  }

  s3DownloadStreams() {
    console.log("s3DownloadStreams");
    return this.keys.map(key => {
      return {
        stream: this.readStream(this.bucketName, key),
        filename: this.zipFileName.slice(0, -4) +'/'+path.basename(key)
      };
    });
  }

  async process() {
    console.log("process");
    const { s3StreamUpload, uploaded } = this.writeStream(
      process.env.ZIP_BUCKET,
      "data/archives/" + this.zipFileName
    );
    const s3DownloadStreams = this.s3DownloadStreams();
    await new Promise((resolve, reject) => {
      const archive = Archiver("zip");
      console.log("create Archiver");
      archive.on("error", error => {
        throw new Error(`${error.name} ${error.code} ${error.message} ${error.path}
      ${error.stack}`);
      });
      console.log("Starting upload");
      s3StreamUpload.on("close", resolve);
      s3StreamUpload.on("end", resolve);
      s3StreamUpload.on("error", reject);
      archive.pipe(s3StreamUpload);
      s3DownloadStreams.forEach(streamDetails =>
        archive.append(streamDetails.stream, { name: streamDetails.filename })
      );
      archive.finalize();
    }).catch(error => {
      throw new Error(`${error.code} ${error.message} ${error.data}`);
    });
    await uploaded.promise();
    console.log("done");
  }
}
exports.zipHandler = async event => {
  console.time("zipProcess");
  console.log(event);
  const zipFileName = uuid() + ".zip";
  const { keys, bucketName } = event;
  const zipHandler = new ZipHandler(keys, bucketName, zipFileName);
  await zipHandler.process();
  const response = {
    bucketName: process.env.ZIP_BUCKET,
    filePath: "data/archives/" + zipFileName
  };
  console.timeEnd("zipProcess");
  return response;
};
