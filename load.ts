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
  let batchPromises: any[] = [];
  let modelName = `Voter${state}Temp`;

  total = 0;
  success = 0;
  failed = 0;

  const fileStream = fs.createReadStream(join(localDir, fileName));

  fileStream.on("error", (err) => {
    console.error("Error reading file", err);
    fileStream.destroy(); // Ensure stream is closed on error
    throw err;
  });

  fileStream.on("end", () => {
    console.log(`Finished reading file ${fileName}`);
    fileStream.destroy(); // Explicitly close the stream
  });

  const { modelFields, fieldTypes } = await getModelFields(modelName);

  async function processStream(row: any) {
    const timeoutDuration = 60000; // 60 seconds

    try {
      if (resume && total < resume) {
        total += 1;
        if (total % 10000 === 0) {
          console.log("skipping rows... total", total);
        }
        return;
      }

      // Process the row data
      const processedRow = processRowData(row, fieldTypes[modelName]);
      buffer.push(processedRow);

      if (buffer.length >= batchSize) {
        total += buffer.length;
        const currentBatch = buffer.slice();
        buffer = []; // Clear buffer immediately

        const promise = withTimeout(
          processBatch(currentBatch, modelName),
          timeoutDuration
        ).catch(async (error) => {
          console.log("Batch processing timed out, retrying...");
          await new Promise((resolve) => setTimeout(resolve, 1000));
          return processBatch(currentBatch, modelName);
        });
        batchPromises.push(promise);

        // Process fewer batches concurrently
        if (batchPromises.length >= 5) {
          await Promise.all(batchPromises);
          batchPromises = [];

          // Force garbage collection if available
          if (global.gc) {
            global.gc();
          }
        }
      }

      // Use setImmediate to prevent event loop blocking
      if (total % 10000 === 0) {
        await new Promise((resolve) => setImmediate(resolve));
      }
    } catch (error) {
      console.error("Error in processStream", error);
      return;
    }
  }

  // Separate row processing logic
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

    // Disable geo-hashing for now as it may be causing memory leaks
    // if (row.Residence_Addresses_Latitude && row.Residence_Addresses_Longitude) {
    //   processedRow["Residence_Addresses_GeoHash"] = geohash.encode(
    //     row.Residence_Addresses_Latitude,
    //     row.Residence_Addresses_Longitude,
    //     8
    //   );
    // }

    if (row?.City && row.City !== "") {
      processedRow.City = row.City.replace(" (EST.)", "");
    }

    return processedRow;
  }

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

      modelName = modelName.replace("Temp", "");

      // First Rename the `public."${modelName}"` table to `public."${modelName}Old"`
      const currentTableName = `public."${modelName}"`;
      const oldTableName = `public."${modelName}Old"`;
      const oldQuery = `ALTER TABLE ${currentTableName} RENAME TO ${oldTableName};`;
      try {
        await prisma.$executeRaw`${oldQuery}`;
      } catch (error) {
        console.error("Error renaming old table", error);
        // TODO: Put this back after the initial load of all states.
        // await sendSlackMessage(
        //   `Error! VoterFile ETL. Error running query: ${oldQuery}.`
        // );
        // return;
      }

      // Next, Rename the `public."${modelName}Temp"` table to `public."${modelName}"`
      const tempTableName = `public."${modelName}Temp"`;
      const newTableName = `public."${modelName}"`;
      const newQuery = `ALTER TABLE ${tempTableName} RENAME TO ${newTableName};`;
      try {
        await prisma.$executeRaw`${newQuery}`;
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
        // await prisma.$executeRaw`${dropQuery}`;
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
    }
  };

  const pipelineAsync = promisify(pipeline);
  try {
    await pipelineAsync(
      fileStream,
      csv({
        separator: "\t",
        mapHeaders: ({ header }) => {
          return modelFields[modelName].includes(header) ? header.trim() : null;
        },
      }),
      async function* (source: any) {
        for await (const row of source) {
          await processStream(row);
        }
      }
    );
    await finishProcessing();
  } catch (error) {
    console.error("Error processing file", error);
  } finally {
    // Ensure the stream is destroyed even if there's an error
    console.log("Destroying file stream");
    fileStream.destroy();
  }
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  const timeout = new Promise<never>((_, reject) =>
    setTimeout(() => reject(new Error("Timeout exceeded")), ms)
  );
  return Promise.race([promise, timeout]);
}

async function processBatch(rows: any[], modelName: string) {
  let modelLower = modelName.replace("Voter", "voter");
  console.log(`Writing ${rows.length} rows to ${modelLower}...`);

  // Create a new PrismaClient instance for this batch
  const batchPrisma = new PrismaClient();

  try {
    // @ts-ignore
    await batchPrisma[modelLower].createMany({
      data: rows,
      skipDuplicates: true,
    });
    success += rows.length;
    console.log(
      `[${modelName}] Total: ${total}, Success: ${success}, Failed: ${failed}`
    );
  } catch (e) {
    failed += rows.length;
    console.log("Error in processBatch", e);
  } finally {
    // Always disconnect this batch's client
    await batchPrisma.$disconnect();
  }
}

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.log("Error in main!");
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });
