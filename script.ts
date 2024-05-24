import { PrismaClient } from "@prisma/client";
import { getDMMF } from "@prisma/sdk";
import { pipeline } from "stream";
import { promisify } from "util";
import { join } from "path";
import { readFileSync } from "fs";
import AWS from "aws-sdk";
import csv from "csv-parser";
import dotenv from "dotenv";

dotenv.config();

const prisma = new PrismaClient();
const accessKeyId = process.env.AWS_ACCESS_KEY_ID;
const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
const region = process.env.AWS_REGION;
const s3Bucket = process.env.AWS_BUCKET || "";
const filePattern = process.env.AWS_FILE_PATTERN;

AWS.config.update({
  region,
  accessKeyId,
  secretAccessKey,
});

const s3 = new AWS.S3();

async function getModelFields() {
  const schemaPath = join(__dirname, "prisma/schema.prisma");
  const schema = readFileSync(schemaPath, "utf-8");
  const dmmf = await getDMMF({ datamodel: schema });

  const models = dmmf.datamodel.models;
  let modelFields: { [modelName: string]: string[] } = {};
  let fieldTypes: { [modelName: string]: { [fieldName: string]: string } } = {};

  models.forEach((model) => {
    modelFields[model.name] = model.fields.map((field) => field.name);
    fieldTypes[model.name] = model.fields.reduce((acc: any, field) => {
      acc[field.name] = field.type;
      return acc;
    }, {});
  });

  return { modelFields, fieldTypes };
}

async function processBatch(rows: any[]) {
  console.log(`Writing ${rows.length} rows to the database...`);

  let response;
  try {
    response = await prisma.voter.createMany({
      data: rows,
      skipDuplicates: true,
    });
    console.log("success writing to db!");
  } catch (e) {
    console.log("error writing to db", e);
  }
}

async function getAllFiles(bucket: string, maxKeys: number) {
  let files = [];
  try {
    const params = {
      Bucket: bucket,
      MaxKeys: maxKeys,
    };

    const data: any = await s3.listObjectsV2(params).promise();

    // Sort the files by LastModified date
    const sortedFiles: any[] = data?.Contents.sort((a: any, b: any) => {
      return (
        new Date(b.LastModified).getTime() - new Date(a.LastModified).getTime()
      );
    });

    for (let i = 0; i < sortedFiles.length; i++) {
      const key = sortedFiles[i]?.Key;
      if (key?.startsWith(filePattern)) {
        files.push(key);
      }
    }
  } catch (error) {
    console.error("Error fetching files: ", error);
    throw error;
  }
  return files;
}

async function main() {
  // Seed the database with the voter files
  let startFile = 0;
  let files = [];
  files = await getAllFiles(s3Bucket, 200);
  // only do the first file
  files = files.slice(0, 1);
  console.log("files", files);
  for (let fileNumber = 0; fileNumber < files.length; fileNumber++) {
    if (startFile && startFile > fileNumber) {
      continue;
    }
    try {
      const file = files[fileNumber];
      console.log(
        `processing file number ${fileNumber} filename ${files[fileNumber]}`
      );
      await processVoterFile(file);
    } catch (error) {
      console.log("uncaught error adding voter file", error);
    }
  }
}

async function processVoterFile(s3Key: string) {
  let buffer: any[] = [];
  const batchSize = 1000;
  let batchPromises: any[] = [];

  const s3Stream = s3
    .getObject({ Bucket: s3Bucket, Key: s3Key })
    .createReadStream();

  const { modelFields, fieldTypes } = await getModelFields();

  const processStream = async (row: any) => {
    const keys = Object.keys(row);
    for (const key of keys) {
      if (row[key] === "" || row[key] === null || row[key] === undefined) {
        // any fields with blank or null or undefined values should be removed
        delete row[key];
        continue;
      }

      if (fieldTypes.Voter[key] === "Int") {
        row[key] = Number(row[key]);
      }
      if (fieldTypes.Voter[key] === "DateTime") {
        row[key] = new Date(row[key]);
      }
    }

    buffer.push(row);
    if (buffer.length >= batchSize) {
      batchPromises.push(processBatch(buffer.slice()));
      buffer = [];
    }
  };

  const finishProcessing = async () => {
    if (buffer.length > 0) {
      batchPromises.push(processBatch(buffer));
    }
    await Promise.all(batchPromises);
    console.log("CSV file successfully processed");
  };

  const pipelineAsync = promisify(pipeline);

  try {
    await pipelineAsync(
      s3Stream,
      csv({
        separator: "\t",
        mapHeaders: ({ header, index }) => {
          if (modelFields.Voter.includes(header) === false) {
            // remove any columns that are not in the schema
            return null;
          } else {
            return header.trim();
          }
        },
      }),
      async function* (source: any) {
        // todo: test without for await.
        for await (const row of source) {
          yield processStream(row);
        }
      }
    );
    await finishProcessing();
  } catch (error) {
    console.error("Error processing file", error);
    // console.error("Error processing file", error);
  }
}

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });
