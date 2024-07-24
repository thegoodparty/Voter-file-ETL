import { PrismaClient } from "@prisma/client";
import { getDMMF } from "@prisma/sdk";
import { pipeline } from "stream";
import { promisify } from "util";
import { join } from "path";
import { readFileSync } from "fs";
import csv from "csv-parser";
import dotenv from "dotenv";
import geohash from "ngeohash";
import fs from "fs";

dotenv.config();

const prisma = new PrismaClient();
const localDir = "/home/ec2-user/VM2Uniform";

let total = 0;
let success = 0;
let failed = 0;
const batchSize = 1000;
const resume = 16449000; // for resuming a specific file.

async function getModelFields(modelName: string) {
  const schemaPath = join(__dirname, "prisma/schema.prisma");
  const schema = readFileSync(schemaPath, "utf-8");
  const dmmf = await getDMMF({ datamodel: schema });

  const models = dmmf.datamodel.models;
  let modelFields: { [modelName: string]: string[] } = {};
  let fieldTypes: { [modelName: string]: { [fieldName: string]: string } } = {};

  models.forEach((model) => {
    if (model.name !== modelName) return;
    modelFields[model.name] = model.fields.map((field) => field.name);
    fieldTypes[model.name] = model.fields.reduce((acc: any, field) => {
      acc[field.name] = field.type;
      return acc;
    }, {});
  });

  return { modelFields, fieldTypes };
}

async function processBatch(rows: any[], modelName: string) {
  let response;
  const modelLower = modelName.replace("Voter", "voter");
  console.log(`Writing ${rows.length} rows to ${modelLower}...`);
  try {
    // @ts-ignore
    response = await prisma[modelLower].createMany({
      data: rows,
      skipDuplicates: true,
    });
    success += rows.length;
    // console.log("success writing to db!", response);
    console.log(
      `[${modelName}] Total: ${total}, Success: ${success}, Failed: ${failed}`
    );
  } catch (e) {
    failed += rows.length;
    console.log("error writing to db", e);
  }
}

async function getAllFiles() {
  let files = [];
  try {
    // get files from the local directory
    files = fs.readdirSync(localDir).filter((file: string) => {
      return file.includes(".tab");
    });

    // order files by filename
    files.sort((a, b) => {
      const aNum = parseInt(a.split("--")[0]);
      const bNum = parseInt(b.split("--")[0]);
      return aNum - bNum;
    });
  } catch (error) {
    console.error("Error fetching files: ", error);
    throw error;
  }
  return files;
}

async function main() {
  // Seed the database with the voter files
  let startFile = 4; // 0
  let endFile = 4; // 51
  let files = [];
  files = await getAllFiles();
  console.log("files", files);
  // only do the first file
  // files = files.slice(0, 1);
  // console.log("files", files);
  for (let fileNumber = 0; fileNumber < files.length; fileNumber++) {
    if (startFile && startFile > fileNumber) {
      continue;
    }
    if (endFile && fileNumber > endFile) {
      break;
    }
    try {
      const file = files[fileNumber];
      console.log(
        `processing file number ${fileNumber} filename ${files[fileNumber]}`
      );
      const state = file.split("--")[1];
      await processVoterFile(file, state);
    } catch (error) {
      console.log("uncaught error adding voter file", error);
    }
  }
}

async function truncateTable(state: string) {
  const tableName = `public."Voter${state}"`;
  const query = `TRUNCATE TABLE ${tableName} RESTART IDENTITY;`;
  // we do a transaction because you cannot set timeout on prepared statements.
  try {
    const result = await prisma.$transaction(async (prisma) => {
      await prisma.$executeRaw`SET LOCAL statement_timeout = '3600000';`; // Set timeout to 1 hour
      return await prisma.$executeRawUnsafe(query);
    });
    console.log(`Table ${tableName} truncated successfully`);
  } catch (error) {
    console.error("Error truncating table:", error);
  } finally {
    // await prisma.$disconnect();
  }
}

async function processVoterFile(fileKey: string, state: string) {
  let buffer: any[] = [];
  let batchPromises: any[] = [];
  const modelName = `Voter${state}`;
  // truncate the table before insert.
  // await truncateTable(state);

  // reset the counters.
  total = 0;
  success = 0;
  failed = 0;

  const fileStream = fs.createReadStream(join(localDir, fileKey));

  const { modelFields, fieldTypes } = await getModelFields(modelName);

  const processStream = async (row: any) => {
    if (resume && resume > 0) {
      if (total < resume) {
        total += 1;
        if (total % 10000 === 0) {
          console.log("skipping rows... total", total);
        }
        return;
      }
    }

    const keys = Object.keys(row);
    for (const key of keys) {
      if (row[key] === "" || row[key] === null || row[key] === undefined) {
        // any fields with blank or null or undefined values should be removed
        delete row[key];
        continue;
      }

      if (fieldTypes[modelName][key] === "Int") {
        row[key] = Number(row[key]);
      }
      if (fieldTypes[modelName][key] === "DateTime") {
        row[key] = new Date(row[key]);
      }
    }
    if (row.Residence_Addresses_Latitude && row.Residence_Addresses_Longitude) {
      const geoHash = geohash.encode(
        row.Residence_Addresses_Latitude,
        row.Residence_Addresses_Longitude,
        8
      );
      row["Residence_Addresses_GeoHash"] = geoHash;
    }
    if (row?.City && row.City != "") {
      row.City = row.City.replace(" (EST.)", "");
    }
    buffer.push(row);
    if (buffer.length >= batchSize) {
      total += buffer.length;
      batchPromises.push(processBatch(buffer.slice(), modelName));
      buffer = [];
    }
  };

  const finishProcessing = async () => {
    if (buffer.length > 0) {
      batchPromises.push(processBatch(buffer, modelName));
    }
    await Promise.all(batchPromises);
    console.log("CSV file successfully processed");
  };

  const pipelineAsync = promisify(pipeline);

  try {
    await pipelineAsync(
      fileStream,
      csv({
        separator: "\t",
        mapHeaders: ({ header, index }) => {
          if (modelFields[modelName].includes(header) === false) {
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
