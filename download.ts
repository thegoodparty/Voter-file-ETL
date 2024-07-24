import Client from "ssh2-sftp-client";
import path from "path";
import dotenv from "dotenv";
import { getLocalFiles, unzipFile } from "./utils";
import fs from "fs";

const sftp = new Client();
dotenv.config();

async function main() {
  const localDir = process.env.LOCAL_DIRECTORY || "/";
  const localFiles = await getLocalFiles(localDir);
  console.log("localFiles", localFiles);

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

      if (localFiles[0] === file.name) {
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
    }
    console.log("All .zip files have been processed successfully.");
  } catch (err) {
    console.error("Error:", err);
  } finally {
    await sftp.end();
  }
}

main();
