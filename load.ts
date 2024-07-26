import { PrismaClient } from "@prisma/client";
import { pipeline } from "stream";
import { promisify } from "util";
import { join } from "path";
import { getLocalFiles, getModelFields, sendSlackMessage } from "./utils";
import csv from "csv-parser";
import dotenv from "dotenv";
import geohash from "ngeohash";
import fs from "fs";
import minimist from "minimist";

dotenv.config();

let total = 0;
let success = 0;
let failed = 0;
let resume = 0; // for resuming a specific file.
let batchSize = 1000;
const localDir = process.env.LOCAL_DIRECTORY || "/";
const prisma = new PrismaClient();

async function main() {
  const args = minimist(process.argv.slice(2));
  let startFile = 0;
  let endFile = 0;

  if (args?.start) {
    startFile = parseInt(args.start);
  }
  if (args?.end) {
    endFile = parseInt(args.end);
  }
  if (args?.resume) {
    resume = parseInt(args.resume);
  }
  if (args?.batchSize) {
    batchSize = parseInt(args.batch);
  }

  // Seed the database with the voter files
  let files = [];
  files = await getLocalFiles(localDir); // filenames from the local directory
  console.log("files", files);

  for (let fileNumber = 0; fileNumber < files.length; fileNumber++) {
    if (startFile && startFile > fileNumber) {
      continue;
    }
    if (endFile && endFile > 0 && fileNumber > endFile) {
      break;
    }
    try {
      const file = files[fileNumber];
      console.log(
        `processing file number ${fileNumber} filename ${files[fileNumber]}`
      );
      const state = file.split("--")[1];

      const loaded = await prisma.voterFile.findUnique({
        where: {
          Filename: file,
          Loaded: true,
        },
      });

      if (loaded) {
        console.log(`File ${file} already loaded. Skipping...`);
        continue;
      }
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

async function processVoterFile(fileName: string, state: string) {
  let buffer: any[] = [];
  let batchPromises: any[] = [];
  const modelName = `Voter${state}`;
  // truncate disabled for now we will only append.
  // await truncateTable(state);

  // reset the counters.
  total = 0;
  success = 0;
  failed = 0;

  const fileStream = fs.createReadStream(join(localDir, fileName));

  fileStream.on("error", (err) => {
    console.error("Error reading file", err);
  });

  const { modelFields, fieldTypes } = await getModelFields(modelName);

  const processStream = async (row: any) => {
    try {
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
      if (
        row.Residence_Addresses_Latitude &&
        row.Residence_Addresses_Longitude
      ) {
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
      // sleep for 1 ms. (limit writes to 1000 per second)
      await new Promise((resolve) => setTimeout(resolve, 1));
    } catch (error) {
      console.error("Error processing row", error);
      throw error;
    }
  };

  const finishProcessing = async () => {
    if (buffer.length > 0) {
      batchPromises.push(processBatch(buffer, modelName));
    }
    await Promise.all(batchPromises);
    console.log("CSV file successfully processed");

    const voterFile = await prisma.voterFile.findUnique({
      where: {
        Filename: fileName,
      },
    });

    if (!voterFile) {
      console.log("Error: VoterFile not found");
      return;
    }

    // @ts-ignore
    const dbCount = await prisma[modelName].count();
    console.log("dbCount", dbCount);
    console.log("voterFile.Lines", voterFile.Lines);

    if (dbCount < voterFile.Lines) {
      console.error(
        `Error: Database count does not match file count. Database: ${dbCount}, File: ${voterFile.Lines}`
      );
      await sendSlackMessage(
        `Error! VoterFile ETL. Model: ${modelName}. Database count does not match file count. Database: ${dbCount}, File: ${voterFile.Lines}`
      );
      return;
    } else {
      console.log(
        `Database count matches file count. Database: ${dbCount}, File: ${voterFile.Lines}`
      );
      await sendSlackMessage(
        `VoterFile ETL Success. Loaded: ${modelName}. Database Count: ${dbCount}, File Count: ${voterFile.Lines}`
      );
    }

    // update the voter file to indicate that it has been loaded.
    await prisma.voterFile.update({
      where: {
        Filename: fileName,
      },
      data: {
        Loaded: true,
      },
    });
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
        try {
          for await (const row of source) {
            yield processStream(row);
          }
        } catch (error) {
          console.error("Error processing file", error);
          throw error;
        }
      }
    );
    await finishProcessing();
  } catch (error) {
    console.error("Error processing file", error);
    // console.error("Error processing file", error);
  }
}

async function processBatch(rows: any[], modelName: string) {
  const modelLower = modelName.replace("Voter", "voter");
  console.log(`Writing ${rows.length} rows to ${modelLower}...`);
  try {
    // @ts-ignore
    await prisma[modelLower].createMany({
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

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });
