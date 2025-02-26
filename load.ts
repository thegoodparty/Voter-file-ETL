import { PrismaClient } from "@prisma/client";
import { pipeline } from "stream";
import { promisify } from "util";
import { join } from "path";
import { getLocalFiles, getModelFields, sendSlackMessage } from "./utils";
import csv from "csv-parser";
import dotenv from "dotenv";
// import geohash from "ngeohash";
import fs from "fs";
import minimist from "minimist";
import { Transform } from "stream";

dotenv.config();

let total = 0;
let success = 0;
let failed = 0;
let resume = 0; // for resuming a specific file.
let batchSize = 1000;
const localDir = process.env.LOCAL_DIRECTORY || "/";
const prisma = new PrismaClient();
const prismaPool: PrismaClient[] = Array(5)
  .fill(null)
  .map(() => new PrismaClient());
let currentPrismaIndex = 0;
let batchPromises: Promise<void>[] = [];

function processRowData(row: any, fieldTypes: any) {
  const processedRow: any = {};

  for (const [key, value] of Object.entries(row)) {
    if (value === "" || value === null || value === undefined) {
      continue;
    }

    if (fieldTypes[key] === "Int") {
      processedRow[key] = Number(value);
    } else if (fieldTypes[key] === "DateTime") {
      processedRow[key] = new Date(value as string);
    } else {
      processedRow[key] = value;
    }
  }

  return processedRow;
}

async function main() {
  const args = minimist(process.argv.slice(2));
  let startFile = 0;
  let endFile = 0;

  if (args?.start) {
    startFile = parseInt(args.start);
    console.log("starting at file", startFile);
  }
  if (args?.end) {
    endFile = parseInt(args.end);
    console.log("ending at file", endFile);
  }
  if (args?.resume) {
    resume = parseInt(args.resume);
    console.log("resuming from file", resume);
  }
  if (args?.batchSize) {
    batchSize = parseInt(args.batch);
    console.log("batch size", batchSize);
  }

  if (args?.gc) {
    console.log("Garbage collection enabled");
  }

  // Seed the database with the voter files
  let files = [];
  files = await getLocalFiles(localDir); // filenames from the local directory
  console.log("files", files);

  for (let fileNumber = 0; fileNumber < files.length; fileNumber++) {
    if (startFile && startFile > fileNumber) {
      console.log(`skipping file number ${fileNumber}`);
      continue;
    }
    if (endFile && endFile > 0 && fileNumber > endFile) {
      console.log(`ending at file number ${fileNumber}`);
      break;
    }
    try {
      const file = files[fileNumber];
      console.log(
        `processing file number ${fileNumber} filename ${files[fileNumber]}`
      );
      const state = file.split("--")[1];
      if (file.includes("DEMOGRAPHIC")) {
        continue;
      }

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
  total = 0;
  success = 0;
  failed = 0;

  // Define modelName here
  let modelName = `Voter${state}Temp`;

  // Add memory logging
  const logMemory = (label: string) => {
    const used = process.memoryUsage();
    console.log(
      `Memory [${label}]: ${Math.round(used.heapUsed / 1024 / 1024)}MB`
    );
  };

  // logMemory('Start of file');

  const fileStream = fs.createReadStream(join(localDir, fileName), {
    highWaterMark: 1024 * 1024, // 1MB chunks
  });

  const { modelFields, fieldTypes } = await getModelFields(modelName);

  // Create a transform stream to handle the CSV parsing
  const parser = csv({
    separator: "\t",
    mapHeaders: ({ header }) => {
      return modelFields[modelName].includes(header) ? header.trim() : null;
    },
    strict: true,
  });

  // Create a transform stream to handle row processing
  const processor = new Transform({
    objectMode: true,
    transform: async function (row, encoding, callback) {
      try {
        if (resume && total < resume) {
          total += 1;
          if (total % 10000 === 0) {
            console.log("skipping rows... total", total);
            // logMemory('During skip');
          }
          callback();
          return;
        }

        total += 1;
        const processedRow = processRowData(row, fieldTypes[modelName]);
        buffer.push(processedRow);

        if (buffer.length >= batchSize) {
          const currentBatch = buffer.slice();
          buffer = []; // Clear buffer immediately
          // logMemory('Before batch process');

          const promise = processBatch(currentBatch, modelName);
          batchPromises.push(promise);

          // Wait if we have 5 batches in progress
          if (batchPromises.length >= 5) {
            await Promise.all(batchPromises);
            batchPromises = [];
          }

          // logMemory('After batch process');
        }

        if (total % 10000 === 0) {
          console.log(`Processed ${total} rows`);
          // logMemory('Regular interval');
        }

        callback();
      } catch (error) {
        callback(error as Error);
      }
    },
  });

  const finishProcessing = async () => {
    if (buffer.length > 0) {
      await processBatch(buffer, modelName);
    }
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

    let modelLower = modelName.replace("Voter", "voter");
    // modelLower = `${modelLower}temp`;
    // @ts-ignore
    const dbCount = await prisma[modelLower].count();

    console.log("dbCount", dbCount);
    console.log("voterFile.Lines", voterFile.Lines);

    if (dbCount < voterFile.Lines - 1000) {
      console.error(
        `Error: Database count does not match file count. Database: ${dbCount}, File: ${voterFile.Lines}`
      );
      await sendSlackMessage(
        `Error! VoterFile ETL. Model: ${modelName}. Database count does not match file count. Database: ${dbCount}, File: ${voterFile.Lines}`
      );
    } else {
      console.log(
        `Database count matches file count. Database: ${dbCount}, File: ${voterFile.Lines}`
      );
      await sendSlackMessage(
        `VoterFile ETL Success. Loaded: ${modelName}. Database Count: ${dbCount}, File Count: ${voterFile.Lines}`
      );
    }

    modelName = modelName.replace("Temp", "");

    // First Rename the `public."${modelName}"` table to `public."${modelName}Old"`
    const currentTableName = `"${modelName}"`;
    const oldTableName = `"${modelName}Old"`;
    const oldQuery = `ALTER TABLE ${currentTableName} RENAME TO ${oldTableName};`;
    try {
      await prisma.$executeRawUnsafe(oldQuery);
    } catch (error) {
      console.error("Error renaming old table", error);
      // TODO: Put this back after the initial load of all states.
      // await sendSlackMessage(
      //   `Error! VoterFile ETL. Error running query: ${oldQuery}.`
      // );
      // return;
    }

    // Next, Rename the `public."${modelName}Temp"` table to `public."${modelName}"`
    const tempTableName = `"${modelName}Temp"`;
    const newTableName = `"${modelName}"`;
    const newQuery = `ALTER TABLE ${tempTableName} RENAME TO ${newTableName};`;
    try {
      await prisma.$executeRawUnsafe(newQuery);
    } catch (error) {
      console.error("Error renaming new table", error);
      await sendSlackMessage(
        `Error! VoterFile ETL. Error running query: ${newQuery}.`
      );
      return;
    }

    await prisma.voterFile.update({
      where: {
        Filename: fileName,
      },
      data: {
        Loaded: true,
      },
    });

    // Finally, drop the old table
    const dropQuery = `DROP TABLE ${oldTableName};`;
    try {
      await prisma.$executeRaw`SET LOCAL statement_timeout = '3600000';`; // Set timeout to 1 hour
      await prisma.$executeRawUnsafe(dropQuery);
    } catch (error) {
      console.error("Error dropping old table", error);
      // TODO: Put this back after the initial load of all states.
      // await sendSlackMessage(
      //   `Error! VoterFile ETL. Error running query: ${dropQuery}.`
      // );
      // return;
    }
  };

  try {
    await new Promise((resolve, reject) => {
      fileStream
        .pipe(parser)
        .pipe(processor)
        .on("finish", resolve)
        .on("error", (error) => {
          console.error("Stream error:", error);
          reject(error);
        });
    });

    // Process any remaining rows
    if (buffer.length > 0) {
      await processBatch(buffer, modelName);
      buffer = [];
    }

    await finishProcessing();
  } catch (error) {
    console.error("Error processing file", error);
    throw error;
  } finally {
    fileStream.destroy();
    parser.destroy();
    processor.destroy();
  }
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  const timeout = new Promise<never>((_, reject) =>
    setTimeout(() => reject(new Error("Timeout exceeded")), ms)
  );
  return Promise.race([promise, timeout]);
}

async function processBatch(rows: any[], modelName: string, retryCount = 0) {
  let modelLower = modelName.replace("Voter", "voter");
  const maxRetries = 3;

  // Get next client from pool
  const batchPrisma = prismaPool[currentPrismaIndex];
  currentPrismaIndex = (currentPrismaIndex + 1) % prismaPool.length;

  try {
    // @ts-ignore
    await batchPrisma[modelLower].createMany({
      data: rows,
      skipDuplicates: true,
    });
    success += rows.length;
  } catch (e) {
    if (retryCount < maxRetries) {
      console.log(
        `Retrying batch of ${rows.length} rows. Attempt ${
          retryCount + 1
        } of ${maxRetries}`
      );
      // Wait a bit before retrying
      await new Promise((resolve) => setTimeout(resolve, 1000));
      return processBatch(rows, modelName, retryCount + 1);
    }
    failed += rows.length;
    console.error(`Failed to process batch after ${maxRetries} attempts:`, e);
  }
}

main()
  .then(async () => {
    // Disconnect all clients in the pool
    await Promise.all(prismaPool.map((client) => client.$disconnect()));
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.log("Error in main!");
    console.error(e);
    // Disconnect all clients in the pool
    await Promise.all(prismaPool.map((client) => client.$disconnect()));
    await prisma.$disconnect();
    process.exit(1);
  });
