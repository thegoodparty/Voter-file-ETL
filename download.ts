import Client from "ssh2-sftp-client";
import path from "path";
import dotenv from "dotenv";
import {
  getLocalFiles,
  unzipFile,
  fileExists,
  countLines,
  sendSlackMessage,
} from "./utils";
import { PrismaClient } from "@prisma/client";
import fs from "fs";

const sftp = new Client();
dotenv.config();

async function main() {
  const localDir = process.env.LOCAL_DIRECTORY || "/";
  const localFiles = await getLocalFiles(localDir);
  console.log("localFiles", localFiles);
  const prisma = new PrismaClient();

  try {
    await sftp.connect({
      host: process.env.SFTP_HOST,
      port: parseInt(process.env?.SFTP_PORT ? process.env.SFTP_PORT : "22"),
      username: process.env.SFTP_USERNAME,
      password: process.env.SFTP_PASSWORD,
    });
    const remoteDir = process.env.REMOTE_DIRECTORY || "/VM2Uniform";

    const remoteFiles: any = await sftp.list(remoteDir);
    const zipFiles: any = remoteFiles.filter((file: any) =>
      file.name.endsWith(".zip")
    );

    for (const file of zipFiles) {
      const remoteFilePath = path.join(remoteDir, file.name);
      const localFilePath = path.join(localDir, file.name);

      if (fileExists(localFiles, file.name)) {
        console.log(
          `File ${file.name} already exists locally. Skipping download.`
        );
        continue;
      } else {
        console.log(`File ${file.name} does not exist locally.`);
        // Check if there are older state files we need to delete.
        const olderFiles = localFiles.filter(
          (f) => f.split("--")[1] === file.name.split("--")[1]
        );
        if (olderFiles.length > 0) {
          console.log(`Deleting older files: ${olderFiles.join(", ")}`);
          olderFiles.forEach((f) => {
            fs.unlinkSync(path.join(localDir, f));
          });
        }
      }

      console.log(`Downloading ${file.name} to ${localFilePath} ...`);
      // Download file
      await sftp.get(remoteFilePath, localFilePath);
      console.log(`${file.name} has been downloaded successfully.`);
      // Unzip the file
      console.log(`Unzipping ${file.name} ...`);
      await unzipFile(localFilePath, localDir);
      console.log(`Unzipped ${file.name}`);

      // Delete the local zip file.
      console.log(`Deleting ${localFilePath} ...`);
      fs.unlinkSync(localFilePath);

      const newVoterFile = localFilePath.replace(".zip", ".tab");
      const newFileName = file.name.replace(".zip", ".tab");

      console.log(`Checking number of lines in ${newVoterFile} ...`);
      // get the number of lines in the file using wc -l and exec
      const count = await countLines(newVoterFile);
      const state = file.name.split("--")[1];

      if (!count || count === 0) {
        console.error(`Error counting lines in ${newVoterFile}`);
        await sendSlackMessage(`Error counting lines in ${newVoterFile}.`);
      }

      await prisma.voterFile.create({
        data: {
          Filename: newFileName,
          State: state,
          Lines: count,
        },
      });

      console.log(
        `Added ${file.name} to the database. State: ${state}. Lines: ${count}`
      );
    }
    console.log("All .zip files have been downloaded successfully.");
  } catch (err) {
    console.error("Error:", err);
  } finally {
    await sftp.end();
  }
}

main();
