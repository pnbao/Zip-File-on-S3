const Archiver = require("archiver");
const AWS = require("aws-sdk");
const path = require("path");
const Stream = require("stream");
const uuid = require("uuid/v4");
const moment = require("moment");

const S3 = new AWS.S3();

class ZipHandler {
  constructor(keys, bucketName, zipFileName) {
    this.keys = keys;
    this.bucketName = bucketName;
    this.zipFileName = zipFileName;
  }
  readStream(Bucket, Key) {
    // return S3.getObject({ Bucket, Key }).createReadStream();
    let stream;
    try {
      stream = S3.getObject({ Bucket, Key }).createReadStream();
      stream.on("error", err => {
        console.log("errrrr ", Key, err);
        stream.end();
      }); // mannually closes the stream on error
      // req.on("close", () => stream.end()); // ensure stream is closed if request is closed/cancelled
      stream.on("end", () => {
        console.log("already done ", Key);
        stream.end(); // mannually closes the stream
      });
      // stream.pipe(res);
      return stream;
    } catch (err) {
      console.log("errrr in catch in read ", err);
      if (stream) stream.end();
    }
  }

  writeStream(Bucket, Key) {
    console.log("writeStream");
    const streamPassThrough = new Stream.PassThrough();
    const params = {
      Body: streamPassThrough,
      Bucket,
      ContentType: "application/zip",
      Key
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
        filename: this.zipFileName.slice(0, -4) + "/" + path.basename(key)
      };
    });
  }

  async process(userId) {
    console.log("process");
    const { s3StreamUpload, uploaded } = this.writeStream(
      process.env.AC_DATA_BUCKET,
      userId + "/_/data/archives/" + this.zipFileName
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
      // s3StreamUpload.on("close", resolve);
      s3StreamUpload.on("end", () => {
        console.log("s3StreamUpload end");
        s3StreamUpload.end();
        resolve();
      });
      s3StreamUpload.on("error", err => {
        console.log("s3StreamUpload error ", err);
        s3StreamUpload.end();
        reject(err);
      });
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
  //error handle
  let response = {
    statusCode: 200,
    statusDescription: "200 OK",
    isBase64Encoded: false,
    headers: {
      "Content-Type": "text/html;",
      "Access-Control-Allow-Origin": "*"
    }
  };
  const queryString = event.queryStringParameters;
  if (queryString === "") {
    response.body = "The required queryStringParameters parameter is empty.";
    return response;
  }
  const userId = queryString.id;

  const requestParams = JSON.parse(event.body);
  if (requestParams === "") {
    response.body = "The required body parameter is empty.";
    return response;
  }

  if (requestParams.filenames == null) {
    response.body = "The required 'filenames' in body parameter is missing.";
    return response;
  } else if (
    requestParams.filenames === "" ||
    requestParams.filenames.length == 0 ||
    !(typeof requestParams.filenames === "object")
  ) {
    response.body =
      "The required 'filenames' in body parameter is empty or incorrect.";
    return response;
  }
  const filenames = requestParams.filenames;

  if (requestParams.zipName == null) {
    response.body = "The required 'zipName' in body parameter is missing.";
    return response;
  } else if (!(typeof requestParams.zipName === "string")) {
    response.body = "The required 'zipName' in body parameter is incorrect.";
    return response;
  }
  const zipName = requestParams.zipName;

  if (requestParams.zipExt == null) {
    response.body = "The required 'zipExt' in body parameter is missing.";
    return response;
  } else if (!(typeof requestParams.zipExt === "string")) {
    response.body = "The required 'zipExt' in body parameter is incorrect.";
    return response;
  }
  const zipExt = requestParams.zipExt;

  const zipFileName = zipName === "" ? uuid() + zipExt : zipName + zipExt;

  if (requestParams.path == null) {
    response.body = "The required 'path' in body parameter is missing.";
    return response;
  } else if (
    requestParams.path === "" ||
    !(typeof requestParams.path === "string")
  ) {
    response.body =
      "The required 'path' in body parameter is empty or incorrect.";
    return response;
  }
  const folderPath = requestParams.path === "/" ? "" : requestParams.path;

  const keys = filenames.map(file => `${userId}/${folderPath}${file}`);
  const zipHandler = new ZipHandler(
    keys,
    (bucketName = process.env.AC_DATA_BUCKET),
    zipFileName
  );
  await zipHandler.process(userId);

  const secretArn = process.env.AC_DATA_SECRET_ARN;
  const resourceArn = process.env.AC_DATA_RESOURCE_ARN;
  const database = process.env.AC_DATA_DATABASE;

  const data = new AWS.RDSDataService();

  let selectUsers = await data
    .executeStatement({
      secretArn: secretArn,
      resourceArn: resourceArn,
      database: database,
      sql: "SELECT id FROM Users WHERE id = :id",
      parameters: [{ name: "id", value: { stringValue: userId } }]
    })
    .promise();

  if (
    !(
      selectUsers.records.length > 0 &&
      selectUsers.records[0][0].stringValue === userId
    )
  ) {
    response.body = "Requested User ID is not existed.";
    return response;
  }

  let selectFolders = await data
    .executeStatement({
      secretArn: secretArn,
      resourceArn: resourceArn,
      database: database,
      sql:
        "SELECT id, path FROM Folders WHERE path = :path AND user_id = :user_id",
      parameters: [
        {
          name: "path",
          value: {
            stringValue: "/_/data/archives/"
          }
        },
        { name: "user_id", value: { stringValue: userId } }
      ]
    })
    .promise();
  console.log(selectFolders.records);
  let folderId = "";
  if (
    !(
      selectFolders.records.length > 0 &&
      selectFolders.records[0][1].stringValue === "/_/data/archives/"
    )
  ) {
    let insertFolders = await data
      .executeStatement({
        secretArn: secretArn,
        resourceArn: resourceArn,
        database: database,
        sql:
          "INSERT INTO Folders(path,user_id,created_at,updated_at) VALUES(:path,:user_id,:created_at,:updated_at)",
        parameters: [
          { name: "path", value: { stringValue: "/_/data/archives/" } },
          { name: "user_id", value: { stringValue: userId } },
          {
            name: "created_at",
            value: { stringValue: moment().format("YYYY-MM-DD hh:mm:ss") }
          },
          {
            name: "updated_at",
            value: { stringValue: moment().format("YYYY-MM-DD hh:mm:ss") }
          }
        ]
      })
      .promise();
    folderId = insertFolders.generatedFields[0].longValue;
  } else {
    folderId = selectFolders.records[0][0].longValue;
  }
  console.log(folderId);

  let insertFiles = await data
    .executeStatement({
      secretArn: secretArn,
      resourceArn: resourceArn,
      database: database,
      sql:
        "INSERT INTO Files(name,folder_id,created_at,updated_at,saved) VALUES(:name,:folder_id,:created_at,:updated_at,:saved)",
      parameters: [
        { name: "name", value: { stringValue: zipFileName } },
        { name: "folder_id", value: { longValue: folderId } },
        {
          name: "created_at",
          value: { stringValue: moment().format("YYYY-MM-DD hh:mm:ss") }
        },
        {
          name: "updated_at",
          value: { stringValue: moment().format("YYYY-MM-DD hh:mm:ss") }
        },
        { name: "saved", value: { booleanValue: false } }
      ]
    })
    .promise();

  response.body = JSON.stringify({
    zipFileName: zipFileName
  });
  return response;
};
